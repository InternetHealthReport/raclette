import os
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
        msg = "Not a valid date: '{0}'. Accepted format is YYYY-MM-DDThh:mm, for example 2018-06-01T00:00".format(s)
        raise argparse.ArgumentTypeError(msg)

def get_probes_info():
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
                coordinates = [(
                    probe["geometry"]["coordinates"][1], 
                    probe["geometry"]["coordinates"][0] )
                    for probe in page["results"] 
                        if probe["geometry"] is not None 
                        if probe["geometry"]["coordinates"] is not None ]

                cities = rg.search(coordinates)
                geoloc = dict(zip(coordinates, cities))

                for probe in page["results"]:
                    bar.next()
                    try:
                        (lon, lat) = probe["geometry"]["coordinates"]
                        probe["city"] = "CT{}, {}".format(geoloc[(lat,lon)]["name"], geoloc[(lat,lon)]["cc"])
                        probes.append(probe)
                        # yield probe.copy()
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
