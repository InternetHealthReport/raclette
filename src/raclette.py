import sys
import logging
import datetime
from collections import defaultdict
import cPickle as pickle
import ConfigParser
from multiprocessing import Pool, Queue

from dumpReader import DumpReader
from atlasrestreader import AtlasRestReader

from firsthoptimetrack import FirstHopTimeTrack
from astimetrack import ASTimeTrack
# from iptimetrack import IPTimeTrack
from tracksmanager import TracksManager


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


atlas_start =  valid_date(config.get("traceroute", "start"))
atlas_stop =  valid_date(config.get("traceroute", "stop"))
atlas_msm_ids =  [int(x) for x in config.get("traceroute", "msm_ids").split(",") if x]
atlas_probe_ids =  [int(x) for x in config.get("traceroute", "probe_ids").split(",") if x]
atlas_chunk_size = int(config.get("traceroute","chunk_size"))

dump_name =  config.get("traceroute", "dump_file")
dump_filter =  config.get("traceroute", "filter")

add_probe = config.get("timetrack", "add_probe")

ip2asn_dir = config.get("lib", "ip2asn_directory")
ip2asn_db = config.get("lib", "ip2asn_db")

tm_expiration = int(config.get("tracksmanager", "expiration"))
tm_window_size = int(config.get("tracksmanager", "window_size"))

# Initialisation
pool = Pool(processes=6)
sys.path.append(ip2asn_dir)
import ip2asn
i2a = ip2asn.ip2asn(ip2asn_db)
fhtt = FirstHopTimeTrack(i2a)
astt = ASTimeTrack(i2a)
tm = TracksManager(tm_window_size, tm_expiration)
nb_err_traceroutes = 0
nb_total_traceroutes = 0

results = defaultdict(defaultdictlist)
dates = []

with AtlasRestReader(atlas_start, atlas_stop, atlas_msm_ids, atlas_probe_ids, chunk_size=atlas_chunk_size, pool=pool) as tr_reader:
# with DumpReader(dump_name, dump_filter) as tr_reader:

    # # Main Loop:
    for trace in tr_reader:
        nb_total_traceroutes += 1
        
        # Check if the traceroute is valid
        if trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
            nb_err_traceroutes += 1
            continue

        # Make a track from the traceroute
        # track = fhtt.traceroute2timetrack(trace)
        track = astt.traceroute2timetrack(trace)

        if add_probe:
            track["rtts"] = [("pid_{}".format(track["prb_id"]), [0])] + track["rtts"]
        
        tm.add_track(track) 
        tm.collect_results(results, dates)

        if nb_total_traceroutes % tm_expiration == 0:
            logging.warn("Total number of traceroute: {} ({} ignored)\nTotal number of tracks: {} ({} expired, {} ignored, {} empty)".format(nb_total_traceroutes, nb_err_traceroutes, tm.nb_tracks, tm.nb_expired_tracks, tm.nb_ignored_tracks, tm.nb_empty_tracks))

tm.collect_results(results, dates, force_expiration=0.5)

logging.warn("Saving results on disk")
pickle.dump(results, open("results.pickle", "wb"))
pickle.dump(dates, open("dates.pickle", "wb"))
