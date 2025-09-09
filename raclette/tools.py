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
import tempfile
import shutil

# Private network regexes
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

def read_ipmap_data(score):
    with bz2.open("cache/geolocations_ipmap.csv.bz2", "rt") as bz_file:
        for line in bz_file:
            words = line.rstrip('\n').split(',')
            try:
                if float(words[-1]) >= score:
                    yield (words[0].rstrip("/32"), words[2], words[3], words[5])
            except Exception:
                pass

def get_probes_info(ipmap=None, refresh_cache=False):
    if ipmap is None:
        ipmap = {}
        for ip, city, state, country in read_ipmap_data(50):
            ipmap[ip] = "CT{}, {}, {}".format(city, state, country)

    if not os.path.exists("cache/"):
        os.mkdir("cache")

    cache_file = "cache/probe_info.json"

    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            cache = json.load(f)
        return cache["probes"]

    # Fetch probe information from RIPE API
    url = "https://atlas.ripe.net/api/v2/probes/"
    bar = None
    probes = []
    with requests.Session() as session:
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
                    if probe['address_v4'] in ipmap:
                        probe["city"] = ipmap[probe["address_v4"]]
                    elif probe['address_v6'] in ipmap:
                        probe["city"] = ipmap[probe["address_v6"]]
                    probes.append(probe)
                except TypeError:
                    logging.debug("Error with probe: {}".format(probe))

            url = page['next']
        bar.finish()

    # Atomic save to cache
    tmp_file = tempfile.NamedTemporaryFile(delete=False, mode="w", dir="cache", suffix=".json")
    json.dump({
        "probes": probes,
        "timestamp": str(datetime.datetime.now(datetime.timezone.utc))
    }, tmp_file, indent=4)
    tmp_file.close()

    # Validate JSON before replacing
    try:
        with open(tmp_file.name, "r") as f:
            json.load(f)
    except Exception:
        os.remove(tmp_file.name)
        raise RuntimeError("Failed to write valid JSON")

    shutil.move(tmp_file.name, cache_file)
    return probes

if __name__ == "__main__":
    # Populate cache
    get_probes_info(ipmap={})

