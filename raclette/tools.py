import os
import bz2
import json
import datetime
from collections import defaultdict
import re
import requests
from progress.bar import Bar
import reverse_geocoder as rg
import logging

import maxminddb
import radix

try:
    from neo4j import GraphDatabase
except ImportError:
    GraphDatabase = None

# Default IPinfo geolocation database (mmdb format). Can be overridden with the
# IPINFO_DB environment variable.
IPINFO_DB = os.environ.get("IPINFO_DB", "data/ipinfo_location.mmdb")

# The IPinfo database is automatically downloaded from this URL when it is
# missing or older than IPINFO_DB_MAX_AGE. This requires an IPinfo API token,
# which is read from the IPINFO_TOKEN environment variable.
IPINFO_DB_URL = "https://ipinfo.io/data/ipinfo_location.mmdb"
IPINFO_DB_MAX_AGE = datetime.timedelta(days=7)
IPINFO_DOWNLOAD_CHUNK_SIZE = 1024*1024
IPINFO_DOWNLOAD_TIMEOUT = 60

# Minimum score for ipmap geolocations (legacy)
IPMAP_MIN_SCORE = 50

# IYP (Internet Yellow Pages) database used to fetch anycast prefixes.
# Can be overridden with the IYP_URL environment variable.
IYP_URL = os.environ.get("IYP_URL", "neo4j://iyp-bolt.ihr.live:7687")

# LACeS anycast census datasets in IYP
LACES_REFERENCE_NAMES = ["utwente.laces_v4", "utwente.laces_v6"]

# BGPKIT prefix to ASN dataset in IYP
PFX2ASN_REFERENCE_NAME = "bgpkit.pfx2asn"

# PeeringDB IXP dataset in IYP
IXP_REFERENCE_NAME = "peeringdb.ix"

# Prefixes fetched from IYP are cached on disk and refreshed when older than
# the corresponding max age
ANYCAST_CACHE_FNAME = "cache/anycast_prefixes.json"
ANYCAST_CACHE_MAX_AGE = datetime.timedelta(days=7)

PFX2ASN_CACHE_FNAME = "cache/pfx2asn.json"
PFX2ASN_CACHE_MAX_AGE = datetime.timedelta(days=1)

IXP_CACHE_FNAME = "cache/ixp_prefixes.json"
IXP_CACHE_MAX_AGE = datetime.timedelta(days=7)

# Get the prefixes inferred as anycast by LACeS. These are the GeoPrefix nodes
# created by LACeS, BGP prefixes tagged as 'Anycast' may be only partially
# anycasted.
ANYCAST_QUERY = """MATCH (p:GeoPrefix)-[l:LOCATED_IN]->(:Point)
WHERE l.reference_name IN $reference_names
RETURN DISTINCT p.prefix AS prefix"""

# Get the prefixes originated by each AS. A prefix may be originated by
# several ASes (MOAS), in that case it is reported once per AS.
PFX2ASN_QUERY = """MATCH (a:AS)-[o:ORIGINATE]->(p:BGPPrefix)
WHERE o.reference_name = $reference_name
RETURN p.prefix AS prefix, a.asn AS asn"""

# Get the peering LANs of IXPs along with the PeeringDB ID and name of the IXP
IXP_QUERY = """MATCH (p:PeeringLAN)-[m:MANAGED_BY]->(ixp:IXP)-[:EXTERNAL_ID]->(i:PeeringdbIXID)
WHERE m.reference_name = $reference_name
RETURN p.prefix AS prefix, i.id AS id, ixp.name AS name"""

# https://en.wikipedia.org/wiki/Private_network
priv_lo = re.compile(r"^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
priv_24 = re.compile(r"^10\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
priv_20 = re.compile(r"^192\.168\.\d{1,3}.\d{1,3}$")
priv_16 = re.compile(r"^172.(1[6-9]|2[0-9]|3[0-1]).[0-9]{1,3}.[0-9]{1,3}$")


def isPrivateIP(ip):

    return priv_lo.match(ip) or priv_24.match(ip) or priv_20.match(ip) or priv_16.match(ip)


def defaultdictlist():
    return defaultdict(list)


def valid_date(s):
    try:
        return datetime.datetime.strptime(s+"UTC", "%Y-%m-%dT%H:%M%Z")
    except ValueError:
        return None


def location2str(city, region, country):
    """Textual representation of a location as used in time tracks."""

    return "CT{}, {}, {}".format(city, region, country)


def iyp_query(query, iyp_url=None, **params):
    """Run a read-only query on IYP and yield the resulting records.

    The connection to IYP is closed once all records are consumed, hence the
    returned generator should always be fully consumed.
    """

    if GraphDatabase is None:
        raise ImportError("neo4j python driver is required to query IYP")

    iyp_url = iyp_url if iyp_url else IYP_URL

    # IYP is publicly readable, no credentials needed
    with GraphDatabase.driver(iyp_url, auth=None) as driver:
        with driver.session() as session:
            for record in session.run(query, **params):
                yield record


def fetch_anycast_prefixes(iyp_url=None, reference_names=None):
    """Fetch anycast prefixes from the LACeS datasets in IYP."""

    iyp_url = iyp_url if iyp_url else IYP_URL
    reference_names = reference_names if reference_names \
        else LACES_REFERENCE_NAMES

    prefixes = [record["prefix"] for record in iyp_query(
        ANYCAST_QUERY, iyp_url, reference_names=reference_names)]

    logging.info("Fetched {} anycast prefixes from IYP ({})".format(
        len(prefixes), iyp_url))

    return prefixes


def fetch_prefix_asn(iyp_url=None, reference_name=None):
    """Fetch prefix to ASN mappings from the BGPKIT pfx2as dataset in IYP.

    Return a list of [prefix, asn] pairs. Prefixes originated by several ASes
    are reported once per AS.
    """

    iyp_url = iyp_url if iyp_url else IYP_URL
    reference_name = reference_name if reference_name \
        else PFX2ASN_REFERENCE_NAME

    prefixes = [[record["prefix"], record["asn"]] for record in iyp_query(
        PFX2ASN_QUERY, iyp_url, reference_name=reference_name)]

    logging.info("Fetched {} prefix to ASN mappings from IYP ({})".format(
        len(prefixes), iyp_url))

    return prefixes


def fetch_ixp_prefixes(iyp_url=None, reference_name=None):
    """Fetch IXP peering LANs from the PeeringDB dataset in IYP.

    Return a list of [prefix, peeringdb_ix_id, ixp_name] values.
    """

    iyp_url = iyp_url if iyp_url else IYP_URL
    reference_name = reference_name if reference_name \
        else IXP_REFERENCE_NAME

    prefixes = [[record["prefix"], record["id"], record["name"]]
                for record in iyp_query(IXP_QUERY, iyp_url,
                                        reference_name=reference_name)]

    logging.info("Fetched {} IXP peering LANs from IYP ({})".format(
        len(prefixes), iyp_url))

    return prefixes


def read_prefixes_cache(cache_fname, max_age=None):
    """Read prefixes from the given cache file. Return None if the cache is
    missing, unreadable, or older than max_age."""

    if not os.path.exists(cache_fname):
        return None

    try:
        with open(cache_fname, "r") as fi:
            cache = json.load(fi)

        if max_age is not None:
            timestamp = datetime.datetime.fromisoformat(cache["timestamp"])
            age = datetime.datetime.now(datetime.timezone.utc) - timestamp
            if age > max_age:
                logging.info("Prefixes cache {} is outdated ({})".format(
                    cache_fname, cache["timestamp"]))
                return None

        return cache["prefixes"]

    except (OSError, ValueError, KeyError) as e:
        logging.warning("Could not read prefixes cache {} ({})".format(
            cache_fname, e))
        return None


def write_prefixes_cache(cache_fname, prefixes):
    """Save prefixes to the given cache file."""

    try:
        dname = os.path.dirname(cache_fname)
        if dname and not os.path.exists(dname):
            os.makedirs(dname)

        with open(cache_fname, "w") as fi:
            json.dump({
                "prefixes": prefixes,
                "timestamp": str(datetime.datetime.now(datetime.timezone.utc))
                }, fi)
    except OSError as e:
        logging.warning("Could not write prefixes cache {} ({})".format(
            cache_fname, e))


def get_iyp_prefixes(cache_fname, max_age, fetch_func, **kwargs):
    """Get prefixes from the given cache file or, if the cache is missing or
    outdated, from IYP with the given fetch function.

    Return an empty list if the prefixes can not be retrieved.
    """

    prefixes = read_prefixes_cache(cache_fname, max_age)
    if prefixes is not None:
        return prefixes

    try:
        prefixes = fetch_func(**kwargs)
    except Exception as e:
        logging.error("Could not fetch data from IYP ({})".format(e))
        # Fall back on the cached prefixes even if they are outdated
        prefixes = read_prefixes_cache(cache_fname)
        return prefixes if prefixes is not None else []

    write_prefixes_cache(cache_fname, prefixes)

    return prefixes


def get_anycast_prefixes(cache_fname=ANYCAST_CACHE_FNAME,
                         max_age=ANYCAST_CACHE_MAX_AGE, iyp_url=None,
                         reference_names=None):
    """Get anycast prefixes from the cache file or, if the cache is missing or
    outdated, from IYP.

    Return an empty list if the prefixes can not be retrieved.
    """

    return get_iyp_prefixes(cache_fname, max_age, fetch_anycast_prefixes,
                            iyp_url=iyp_url, reference_names=reference_names)


def get_prefix_asn(cache_fname=PFX2ASN_CACHE_FNAME,
                   max_age=PFX2ASN_CACHE_MAX_AGE, iyp_url=None,
                   reference_name=None):
    """Get prefix to ASN mappings from the cache file or, if the cache is
    missing or outdated, from IYP.

    Return an empty list if the mappings can not be retrieved.
    """

    return get_iyp_prefixes(cache_fname, max_age, fetch_prefix_asn,
                            iyp_url=iyp_url, reference_name=reference_name)


def get_ixp_prefixes(cache_fname=IXP_CACHE_FNAME, max_age=IXP_CACHE_MAX_AGE,
                     iyp_url=None, reference_name=None):
    """Get IXP peering LANs from the cache file or, if the cache is missing or
    outdated, from IYP.

    Return an empty list if the prefixes can not be retrieved.
    """

    return get_iyp_prefixes(cache_fname, max_age, fetch_ixp_prefixes,
                            iyp_url=iyp_url, reference_name=reference_name)


class AnycastPrefixes():
    """Tell if IP addresses are part of an anycast prefix, as reported by the
    LACeS anycast census (fetched from IYP)."""

    def __init__(self, prefixes=None, **kwargs):

        if prefixes is None:
            prefixes = get_anycast_prefixes(**kwargs)

        self.rtree = radix.Radix()
        for prefix in prefixes:
            try:
                self.rtree.add(prefix)
            except ValueError:
                logging.warning("Ignoring invalid anycast prefix: {}".format(
                    prefix))

        logging.info("Loaded {} anycast prefixes".format(len(self)))

    def is_anycast(self, ip):
        """Return True if the given IP address is in an anycast prefix."""

        if not ip:
            return False

        try:
            return self.rtree.search_best(ip) is not None
        except ValueError:
            # not a valid IP address
            return False

    def __len__(self):
        return len(self.rtree.prefixes())


def ipinfo_db_age(db_fname):
    """Return the age of the given IPinfo database as a timedelta, or None if
    the database does not exist."""

    if not os.path.exists(db_fname):
        return None

    mtime = datetime.datetime.fromtimestamp(
            os.path.getmtime(db_fname), datetime.timezone.utc)

    return datetime.datetime.now(datetime.timezone.utc) - mtime


def download_ipinfo_db(db_fname=None, token=None):
    """Download the IPinfo geolocation database (mmdb format).

    The token is read from the IPINFO_TOKEN environment variable if it is not
    given. The database is downloaded to a temporary file and checked before
    replacing the current database, so that a failed download never leaves a
    corrupted database behind.

    Return True if the database was successfully downloaded.
    """

    db_fname = db_fname if db_fname else IPINFO_DB
    token = token if token else os.environ.get("IPINFO_TOKEN", "")

    if not token:
        logging.warning("No IPinfo token found, the geolocation database can "
                        "not be downloaded. Set the IPINFO_TOKEN environment "
                        "variable to enable automatic updates.")
        return False

    tmp_fname = db_fname + ".tmp"

    try:
        dname = os.path.dirname(db_fname)
        if dname and not os.path.exists(dname):
            os.makedirs(dname)

        logging.info("Downloading IPinfo database to {}".format(db_fname))
        # the token is passed as a parameter so that it is not printed in logs
        with requests.get(IPINFO_DB_URL,
                          params={"_src": "frontend", "token": token},
                          stream=True,
                          timeout=IPINFO_DOWNLOAD_TIMEOUT) as resp:
            resp.raise_for_status()

            with open(tmp_fname, "wb") as fi:
                for chunk in resp.iter_content(IPINFO_DOWNLOAD_CHUNK_SIZE):
                    fi.write(chunk)

        # Make sure the downloaded file is a valid database before replacing
        # the current one
        with maxminddb.open_database(tmp_fname):
            pass

        os.replace(tmp_fname, db_fname)
        logging.info("Updated IPinfo database {} ({} bytes)".format(
            db_fname, os.path.getsize(db_fname)))

        return True

    except (requests.RequestException, OSError,
            maxminddb.InvalidDatabaseError) as e:
        logging.error("Could not download IPinfo database ({})".format(e))

        if os.path.exists(tmp_fname):
            try:
                os.remove(tmp_fname)
            except OSError:
                pass

        return False


def update_ipinfo_db(db_fname=None, max_age=IPINFO_DB_MAX_AGE, token=None):
    """Download the IPinfo database if it is missing or older than max_age.

    The previous database is kept if the download fails, so that raclette can
    still geolocate IP addresses with a slightly outdated database.

    Return the database filename.
    """

    db_fname = db_fname if db_fname else IPINFO_DB

    age = ipinfo_db_age(db_fname)
    if age is None:
        logging.info("No IPinfo database found at {}".format(db_fname))
    elif max_age is not None and age > max_age:
        logging.info("IPinfo database {} is outdated ({} days old)".format(
            db_fname, age.days))
    else:
        # the database is recent enough
        return db_fname

    download_ipinfo_db(db_fname, token)

    return db_fname


class Geolocation():
    """Geolocate IP addresses with an IPinfo database (mmdb format).

    Locations are returned as a string formatted as 'CTcity, region, country',
    or None if the IP address is not found in the database.

    Anycast IP addresses are hosted at multiple locations, hence they are not
    geolocated. Set ignore_anycast to False to geolocate them anyway, or give
    an AnycastPrefixes object to reuse already fetched prefixes.

    The database is automatically downloaded if it is missing or older than
    max_age (one week by default). Set auto_update to False to always use the
    local database.
    """

    def __init__(self, db_fname=None, anycast=None, ignore_anycast=True,
                 auto_update=True, max_age=IPINFO_DB_MAX_AGE, token=None):

        self.db_fname = db_fname if db_fname else IPINFO_DB
        self.reader = None

        if auto_update:
            update_ipinfo_db(self.db_fname, max_age, token)

        self.anycast = anycast
        if self.anycast is None and ignore_anycast:
            self.anycast = AnycastPrefixes()

        try:
            self.reader = maxminddb.open_database(self.db_fname)
            logging.info("Loaded IPinfo geolocation database: {}".format(
                self.db_fname))
        except (FileNotFoundError, maxminddb.InvalidDatabaseError) as e:
            logging.error("Could not open IPinfo database {} ({}). "
                          "IP addresses will not be geolocated.".format(
                              self.db_fname, e))

    def ip2city(self, ip):
        """Return the location of the given IP address, or None if unknown."""

        if self.reader is None or not ip:
            return None

        # Anycast addresses are at several locations, geolocating them is
        # meaningless
        if self.anycast is not None and self.anycast.is_anycast(ip):
            return None

        try:
            location = self.reader.get(ip)
        except ValueError:
            # not a valid IP address
            return None

        if not location:
            return None

        city = location.get("city", "")
        region = location.get("region", "")
        country = location.get("country_code", "")

        if not city and not region and not country:
            return None

        return location2str(city, region, country)

    def close(self):
        if self.reader is not None:
            self.reader.close()
            self.reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def read_ipmap_data(score):
    """Read RIPE ipmap geolocations from the cache directory (legacy)."""

    with bz2.open("cache/geolocations_ipmap.csv.bz2", "rt") as bz_file:
        for line in bz_file:
            words = line.rstrip('\n').split(',')
            try:
                if float(words[-1]) >= score:
                    yield (words[0].rstrip("/32"), words[2], words[3], words[5])
            except Exception as e:
                # ignore not well-formated lines in the csv file
                pass


class IpmapGeolocation():
    """Geolocate IP addresses with RIPE ipmap data (legacy).

    This is a drop-in replacement for Geolocation, it is kept for the case we
    want to use ipmap geolocations again.
    """

    def __init__(self, score=IPMAP_MIN_SCORE):

        self.ipmap = {}
        for ip, city, region, country in read_ipmap_data(score):
            self.ipmap[ip] = location2str(city, region, country)

    def ip2city(self, ip):
        """Return the location of the given IP address, or None if unknown."""

        return self.ipmap.get(ip)

    def close(self):
        self.ipmap = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def get_probes_info(geoloc=None):
    """Fetch RIPE Atlas probes information and add the city of the probes that
    can be geolocated.

    geoloc is an object providing an ip2city method (e.g. Geolocation or
    IpmapGeolocation), by default the IPinfo database is used.
    """

    close_geoloc = False
    if geoloc is None:
        geoloc = Geolocation()
        close_geoloc = True

    today = datetime.datetime.today()
    if not os.path.exists("cache/"):
        os.mkdir("cache")

    if os.path.exists("cache/probe_info.json"):
        # Get probe information from cache
        cache = json.load(open("cache/probe_info.json", "r"))
#        print("Loading probe information from cache")

        if close_geoloc:
            geoloc.close()

        return cache["probes"]

    else:
        # Fetch probe information from RIPE API
        url = "https://atlas.ripe.net/api/v2/probes/"
        bar = None
        probes = []
        with requests.Session() as session:

            # Fetch all pages
            while url:
                page = session.get(url).json()
                if bar is None:
                    bar = Bar(
                        "Fetching probe information from RIPE API",
                        max=page["count"],
                        suffix='%(percent)d%%'
                        )

                for probe in page["results"]:
                    bar.next()
                    try:
                        city = geoloc.ip2city(probe["address_v4"])
                        if city is None:
                            city = geoloc.ip2city(probe["address_v6"])

                        if city is not None:
                            probe["city"] = city

                        probes.append(probe)

                    except TypeError:
                        logging.debug("Error with probe: {}".format(probe))

                url = page['next']
            bar.finish()

        # Save probe information to cache
        fi = open("cache/probe_info.json", "w")
        json.dump({
            "probes": probes,
            "timestamp": str(datetime.datetime.now(datetime.timezone.utc))
            }, fi, indent=4)
        fi.close()

        if close_geoloc:
            geoloc.close()

        return probes


if __name__ == "__main__":
    # Populate cache if empty
    get_probes_info()
