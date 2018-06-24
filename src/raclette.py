import sys
import os
import logging
import datetime
from collections import defaultdict
import cPickle as pickle
import ConfigParser
import argparse
from multiprocessing import Process, Pool, JoinableQueue, Pipe

import tools
from dumpReader import DumpReader
from atlasrestreader import AtlasRestReader

from firsthoptimetrack import FirstHopTimeTrack
from astimetrack import ASTimeTrack
# from iptimetrack import IPTimeTrack
from tracksaggregator import TracksAggregator
from delaychangedetector import DelayChangeDetector
from sqlitesaver import SQLiteSaver


class Raclette():


    def read_config(self):

        parser = argparse.ArgumentParser()
        parser.add_argument("-C","--config_file", help="Get all parameters from the specified config file", type=str, default="conf/raclette.conf")
        args = parser.parse_args()

        # Read the config file
        config = ConfigParser.ConfigParser()
        config.read(args.config_file)

        self.atlas_start =  tools.valid_date(config.get("io", "start"))
        self.atlas_stop =  tools.valid_date(config.get("io", "stop"))
        self.atlas_msm_ids =  [int(x) for x in config.get("io", "msm_ids").split(",") if x]
        self.atlas_probe_ids =  [int(x) for x in config.get("io", "probe_ids").split(",") if x]
        self.atlas_chunk_size = int(config.get("io","chunk_size"))

        self.dump_name =  config.get("io", "dump_file")
        self.dump_filter =  config.get("io", "filter")

        self.add_probe = config.get("timetrack", "add_probe")

        self.ip2asn_dir = config.get("lib", "ip2asn_directory")
        self.ip2asn_db = config.get("lib", "ip2asn_db")

        self.tm_expiration = int(config.get("tracksaggregator", "expiration"))
        self.tm_window_size = int(config.get("tracksaggregator", "window_size"))
        self.tm_significance_level = float(config.get("tracksaggregator", "significance_level"))

        self.saver_filename = config.get("io", "results")
        self.log_filename = config.get("io", "log")

        # Create output directories
        for fname in [self.saver_filename, self.log_filename]:
            dname = fname.rpartition("/")[0]
            if not os.path.exists(dname):
                os.makedirs(dname)

        FORMAT = '%(asctime)s %(processName)s %(message)s'
        logging.basicConfig(format=FORMAT, filename=self.log_filename, level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
        logging.info("Started: %s" % sys.argv)
        logging.info("Arguments: %s" % args)
        for sec in config.sections():
            logging.info("Config: [%s] %s" % (sec,config.items(sec,False)))

        self.config = config

    def save_aggregates(self, saver_queue, aggregates):

        for date, results in aggregates.iteritems():
            saver_queue.put("BEGIN TRANSACTION;")
            for locations, agg in results.iteritems():
                entry = ("diffrtt", (date, locations[0], locations[1], agg["median"], agg["conf_high"], agg["conf_low"]))
                saver_queue.put(entry)
            saver_queue.put("COMMIT;")

    def main(self):

        # Initialisation
        saver_queue = JoinableQueue(1000000)
        detector_pipe = Pipe(False)

        # These are run in a separate process
        detector_delay = DelayChangeDetector(detector_pipe[0], saver_queue)
        saver_sqlite = SQLiteSaver(self.saver_filename, saver_queue)

        saver_sqlite.start()
        detector_delay.start()

        sys.path.append(self.ip2asn_dir)
        import ip2asn

        i2a = ip2asn.ip2asn(self.ip2asn_db)
        fhtt = FirstHopTimeTrack(i2a)
        astt = ASTimeTrack(i2a)
        tm = TracksAggregator(self.tm_window_size, self.tm_expiration, self.tm_significance_level)
        nb_total_traceroutes = 0

        saver_queue.put(("experiment", [datetime.datetime.now(), str(sys.argv), str(self.config.sections())]))

        with AtlasRestReader(self.atlas_start, self.atlas_stop, astt, self.atlas_msm_ids, self.atlas_probe_ids, 
                chunk_size=self.atlas_chunk_size) as tr_reader:
        # with DumpReader(dump_name, dump_filter) as tr_reader:

            # # Main Loop:
            for track in tr_reader:
                nb_total_traceroutes += 1
                if not track:
                    continue
                
                if self.add_probe:
                    track["rtts"] = [("pid_{}".format(track["prb_id"]), [0])] + track["rtts"]
                
                tm.add_track(track) 
                aggregates = tm.aggregate()
                self.save_aggregates(saver_queue, aggregates)


        logging.info("Finished to read data {}".format(datetime.datetime.today()))

        # Try to aggregate remaining track bins
        aggregates = tm.aggregate(force_expiration=0.5)
        self.save_aggregates(saver_queue, aggregates)

        # closing
        saver_queue.join()
        saver_sqlite.terminate()
        detector_delay.terminate()

        logging.info("Ended on {}".format(datetime.datetime.today()))


if __name__ == "__main__":
    ra = Raclette()
    ra.read_config()
    ra.main() 
