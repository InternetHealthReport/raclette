import sys
import logging
import datetime
from collections import defaultdict
from itertools import combinations, product
import numpy as np
import cPickle as pickle
import ConfigParser
from multiprocessing import Pool
import gc

from dumpReader import DumpReader
from atlasrestreader import AtlasRestReader

from firsthoptimetrack import FirstHopTimeTrack
from astimetrack import ASTimeTrack
# from iptimetrack import IPTimeTrack

def defaultdictlist(): 
    return defaultdict(list)

def valid_date(s):
    try:
        return datetime.datetime.strptime(s+"UTC", "%Y-%m-%dT%H:%M%Z")
    except ValueError:
        msg = "Not a valid date: '{0}'. Accepted format is YYYY-MM-DDThh:mm, for example 2018-06-01T00:00".format(s)
        raise argparse.ArgumentTypeError(msg)


# Read the config file
config = ConfigParser.ConfigParser()
config.read("conf/raclette.conf")

window_size = int(config.get("main", "window_size"))

atlas_start =  valid_date(config.get("traceroute", "start"))
atlas_stop =  valid_date(config.get("traceroute", "stop"))
atlas_msm_ids =  [int(x) for x in config.get("traceroute", "msm_ids").split(",") if x]
atlas_probe_ids =  [int(x) for x in config.get("traceroute", "probe_ids").split(",") if x]

dump_name =  config.get("traceroute", "dump_file")
dump_filter =  config.get("traceroute", "filter")

add_probe = config.get("timetrack", "add_probe")

ip2asn_dir = config.get("lib", "ip2asn_directory")
ip2asn_db = config.get("lib", "ip2asn_db")

gc_expiration = int(config.get("garbagecollector", "expiration"))

# Initialisation
pool = Pool(processes=6)
sys.path.append(ip2asn_dir)
import ip2asn
i2a = ip2asn.ip2asn(ip2asn_db)
fhtt = FirstHopTimeTrack(i2a)
astt = ASTimeTrack(i2a)
window_start = 0
# history_size = 10
track_bins = defaultdict(list)
bins_last_insert= defaultdict(int)
nb_err_traceroutes = 0
nb_expired_traceroutes = 0
nb_total_traceroutes = 0

results = defaultdict(defaultdictlist)
results_bins = defaultdict(defaultdictlist)
dates = []

with AtlasRestReader(atlas_start, atlas_stop, atlas_msm_ids, atlas_probe_ids, pool=pool) as tr_reader:
# with DumpReader(dump_name, dump_filter) as tr_reader:

    # # Main Loop:
    for trace in tr_reader:
        nb_total_traceroutes += 1
        bin_id = int(trace["timestamp"]/window_size)
        # print("\r {} traceroutes processed".format(nb_total_traceroutes)
        
	if trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
            nb_err_traceroutes += 1
            continue

        if bins_last_insert[bin_id] is None:
            nb_expired_traceroutes += 1
            continue

        # track = fhtt.traceroute2timetrack(trace)
        track = astt.traceroute2timetrack(trace)
        if track is None:
            nb_err_traceroutes += 1
            continue

        if add_probe:
            track["rtts"] = [("pid_{}".format(track["prb_id"]), [0])] + track["rtts"]

        # index track based on its timestamp
        track_bins[bin_id].append(track)
        bins_last_insert[bin_id] = nb_total_traceroutes

        if nb_total_traceroutes % gc_expiration == 0:
            logging.warn("Running garbage collection")
            logging.warn("Total number of traceroute: {} ({} ignored, {} expired)".format(nb_total_traceroutes, nb_err_traceroutes, nb_expired_traceroutes))
            expired_bins = []
            for date, tracks in track_bins.iteritems():

                # logging.warn("preProcessing bin {}{}".format(date))
                # TODO: probe diversity

                if bins_last_insert[date]+gc_expiration < nb_total_traceroutes:
                    logging.warn("Processing bin {}".format(date))
                    expired_bins.append(date)
                    
                    diffrtt = defaultdict(list)
                    for track in tracks:
                        for locPair in combinations(track["rtts"],2):
                            (loc0, rtts0) = locPair[0]
                            (loc1, rtts1) = locPair[1]
                            diffrtt[(loc0,loc1)] += [ x1-x0 for x0,x1 in product(rtts0, rtts1)]

                    # Compute median/wilson scores and shift windows
                    if len(diffrtt):
                        for locations, dr in diffrtt.iteritems():
                            results[str(locations)]["median"].append(np.median(dr))
                            results[str(locations)]["nb_samples"].append(len(dr))

            # Garbage collection
            # remember expired dates and delete corresponding bins
            for date in expired_bins:
                bins_last_insert[date] = None 
                del track_bins[date]
                # print("Removing bin {}, {}".format(locations, date))
            logging.warn("Running python garbage collection")
            gc.collect()

for date, values in bins_last_insert.iteritems():
    dates.append(date*window_size)
                            

logging.warn("Total number of traceroute: {} ({} ignored, {} expired)".format(nb_total_traceroutes, nb_err_traceroutes, nb_expired_traceroutes))
pickle.dump(results, open("results.pickle", "wb"))
pickle.dump(dates, open("dates.pickle", "wb"))
