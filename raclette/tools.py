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

# https://en.wikipedia.org/wiki/Private_network
priv_lo = re.compile("^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
priv_24 = re.compile("^10\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
priv_20 = re.compile("^192\.168\.\d{1,3}.\d{1,3}$")
priv_16 = re.compile("^172.(1[6-9]|2[0-9]|3[0-1]).[0-9]{1,3}.[0-9]{1,3}$")

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
                if int(words[-1]) >= score:
                    yield (words[0].rstrip("/32"), words[2], words[3], words[5])
            except:
                # ignore not well-formated lines in the csv file
                pass

def get_probes_info(ipmap=None):

    if ipmap is None:
        ipmap = {}

        for ip, city, state, country in read_ipmap_data(50):
            ipmap[ip] = "CT{}, {}, {}".format(city, state, country)

    today = datetime.datetime.today()
    if not os.path.exists("cache/"):
        os.mkdir("cache")

    if os.path.exists("cache/probe_info.json"):
        # Get probe information from cache
        cache = json.load(open("cache/probe_info.json","r"))
        print("Loading probe information from cache")

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

                # get city and country names
                # coordinates = [(
                    # probe["geometry"]["coordinates"][1], 
                    # probe["geometry"]["coordinates"][0] )
                    # for probe in page["results"] 
                        # if probe["geometry"] is not None 
                        # if probe["geometry"]["coordinates"] is not None ]

                # cities = rg.search(coordinates)
                # geoloc = dict(zip(coordinates, cities))

                for probe in page["results"]:
                    bar.next()
                    try:
                        # (lon, lat) = probe["geometry"]["coordinates"]
                        # probe["city"] = "CT{}, {}, {}".format(geoloc[(lat,lon)]["name"], geoloc[(lat,lon)]["admin2"], geoloc[(lat,lon)]["cc"])
                        if probe['address_v4'] in ipmap:
                            probe["city"] = ipmap[probe["address_v4"]]
                        elif probe['address_v6'] in ipmap:
                            probe["city"] = ipmap[probe["address_v6"]]

                        probes.append(probe)

                    except TypeError:
                        logging.debug("Error with probe: {}".format(probe))

                url = page['next']
            bar.finish()

        # Save probe information to cache
        fi = open("cache/probe_info.json", "w")
        json.dump({
            "probes":probes, 
            "timestamp":str(datetime.datetime.utcnow())
            }, fi, indent=4)
        fi.close()

        return probes

if __name__ == "__main__":
    # Populate cache if empty
    get_probes_info()
