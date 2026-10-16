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

# Default IPinfo geolocation database (mmdb format). Can be overridden with the
# IPINFO_DB environment variable.
IPINFO_DB = os.environ.get("IPINFO_DB", "data/ipinfo_location.mmdb")

# Minimum score for ipmap geolocations (legacy)
IPMAP_MIN_SCORE = 50

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


class Geolocation():
    """Geolocate IP addresses with an IPinfo database (mmdb format).

    Locations are returned as a string formatted as 'CTcity, region, country',
    or None if the IP address is not found in the database.
    """

    def __init__(self, db_fname=None):

        self.db_fname = db_fname if db_fname else IPINFO_DB
        self.reader = None

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
