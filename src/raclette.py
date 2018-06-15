import sys
import logging
from collections import defaultdict
from itertools import combinations, product
import numpy as np
import cPickle as pickle
import ConfigParser

from dumpReader import DumpReader

from firsthoptimetrack import FirstHopTimeTrack
from astimetrack import ASTimeTrack
# from iptimetrack import IPTimeTrack

def defaultdictlist(): 
    return defaultdict(list)

# Read the config file
config = ConfigParser.ConfigParser()
config.read("conf/raclette.conf")

window_size = int(config.get("main", "window_size"))

dump_name =  config.get("traceroute", "dump_file")
dump_filter =  config.get("traceroute", "filter")

add_probe = config.get("timetrack", "add_probe")

ip2asn_dir = config.get("lib", "ip2asn_directory")
ip2asn_db = config.get("lib", "ip2asn_db")

# Initialisation
sys.path.append(ip2asn_dir)
import ip2asn
i2a = ip2asn.ip2asn(ip2asn_db)
fhtt = FirstHopTimeTrack(i2a)
astt = ASTimeTrack(i2a)
window_start = 0
# history_size = 10
track_bins = defaultdict(defaultdictlist)
diffrtt_bins = defaultdict(defaultdictlist)
next_diffrtt = defaultdict(list)
nb_err_traceroutes = 0
nb_total_traceroutes = 0

results = defaultdict(defaultdictlist)
dates = []

# # Main Loop:
with DumpReader(dump_name, dump_filter) as tr_reader:

    for trace in tr_reader:
        nb_total_traceroutes += 1
        # print("\r {} traceroutes processed".format(nb_total_traceroutes)
        
	if trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
            nb_err_traceroutes += 1
            continue

        # track = fhtt.traceroute2timetrack(trace)
        track = astt.traceroute2timetrack(trace)
        if track is None:
            nb_err_traceroutes += 1
            continue

        if add_probe:
            track["rtts"] = [("pid_{}".format(track["prb_id"]), [0])] + track["rtts"]


        # compute the diff. RTTs between all locations in the tracks
        for locPair in combinations(track["rtts"],2):
            (loc0, rtts0) = locPair[0]
            (loc1, rtts1) = locPair[1]
            track_bins[(loc0,loc1)][int(trace["timestamp"]/window_size)].append([track,locPair])

            # TODO do this computation later, takes too much memory?
            # diffrtt_bins[int(trace["timestamp"]/window_size)][(loc0,loc1)] += [ x1-x0 for x0,x1 in product(rtts0, rtts1)]


results_bins = defaultdict(defaultdictlist)
for locations, date_tracks in track_bins.iteritems():
    
    # TODO: probe diversity

    for date, tracks in date_tracks.iteritems():
        diffrtt = []
        for track, locpair in tracks:
            rtts0 = locPair[0][1]
            rtts1 = locPair[1][1]
            diffrtt += [ x1-x0 for x0,x1 in product(rtts0, rtts1)]

        # Compute median/wilson scores and shift windows
        if len(diffrtt):
            # for locations, dr in diffrtt.iteritems():
            results[str(locations)]["median"].append(np.median(diffrtt))
            results[str(locations)]["nb_samples"].append(len(diffrtt))

        # TODO move this somewhere else
        dates.append(date*window_size)
            

logging.warn("Total number of traceroute: {} ({} ignored)".format(nb_total_traceroutes, nb_err_traceroutes))
pickle.dump(results, open("results.pickle", "wb"))
pickle.dump(dates, open("dates.pickle", "wb"))
