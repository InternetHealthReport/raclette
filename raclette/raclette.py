import sys
import os
import logging
import datetime
from collections import defaultdict
import pickle
import configparser
import argparse
from multiprocessing import Process, Pool, JoinableQueue, Pipe
import importlib
import traceback

import tools
from dumpReader import DumpReader
from atlasrestreader import AtlasRestReader

from tracksaggregator_cy import TracksAggregator
from delaychangedetector import DelayChangeDetector
from sqlitesaver import SQLiteSaver


class Raclette():
    """
    Main program that connects all modules. Fetch traceroute data, compute time
    tracks, differential RTTs and anomaly detection.
    Parameters are provided in the default config file (conf/raclette.conf) or 
    the one given as argument.
    """

    def read_config(self):
        """
        Reads the configuration file and initialize accordingly.
        """

        parser = argparse.ArgumentParser()
        parser.add_argument("-C","--config_file", help="Get all parameters from the specified config file", type=str, default="conf/raclette.conf")
        args = parser.parse_args()

        # Read the config file
        config = configparser.ConfigParser()
        config.read(args.config_file)

        self.atlas_start =  tools.valid_date(config.get("io", "start"))
        self.atlas_stop =  tools.valid_date(config.get("io", "stop"))
        self.atlas_msm_ids =  [int(x) for x in config.get("io", "msm_ids").split(",") if x]
        self.atlas_probe_ids =  [int(x) for x in config.get("io", "probe_ids").split(",") if x]
        self.atlas_chunk_size = int(config.get("io","chunk_size"))

        try:
            self.dump_name =  config.get("io", "dump_file")
            self.dump_filter =  config.get("io", "filter")
        except configparser.NoOptionError:
            pass

        self.timetrack_converter = config.get("timetrack", "converter")

        self.ip2asn_dir = config.get("lib", "ip2asn_directory")
        self.ip2asn_db = config.get("lib", "ip2asn_db")
        self.ip2asn_ixp = config.get("lib", "ip2asn_ixp")

        self.tm_expiration = int(config.get("tracksaggregator", "expiration"))
        self.tm_window_size = int(config.get("tracksaggregator", "window_size"))
        self.tm_significance_level = float(config.get("tracksaggregator", "significance_level"))
        self.tm_min_tracks = int(config.get("tracksaggregator", "min_tracks"))

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
        """
        Save differential RTTs values on disk.
        """

        for date, results in aggregates.items():
            saver_queue.put("BEGIN TRANSACTION;")
            [saver_queue.put(
                    ("diffrtt", 
                    (date, locations[0], locations[1], agg["median"], 
                        agg["conf_high"], agg["conf_low"], agg["min"],
                        agg["nb_tracks"], agg["nb_probes"], agg["entropy"], 
                        agg["hop"], agg["nb_real_rtts"]))
                )
                for locations, agg in results.items()]
            saver_queue.put("COMMIT;")


    def main(self):
        """
        Main program connecting all modules.
        """

        try:
            # Initialisation
            saver_queue = JoinableQueue()
            detector_pipe = Pipe(False)

            # These are run in a separate process
            detector_delay = DelayChangeDetector(detector_pipe[0], saver_queue)
            saver_sqlite = SQLiteSaver(self.saver_filename, saver_queue)

            saver_sqlite.start()
            detector_delay.start()

            sys.path.append(self.ip2asn_dir)
            import ip2asn
            i2a = ip2asn.ip2asn(self.ip2asn_db, self.ip2asn_ixp)

            try:
                timetrac_module = importlib.import_module("timetrack."+self.timetrack_converter)
                timetrackconverter = timetrac_module.TimeTrackConverter(i2a)
            except ImportError:
                logging.error("Timetrack converter unknown! ({})".format(self.timetrack_converter))
                traceback.print_exc(file=sys.stdout)
                return

            tm = TracksAggregator(self.tm_window_size, self.tm_expiration, self.tm_significance_level, self.tm_min_tracks)

            saver_queue.put(("experiment", [datetime.datetime.now(), str(sys.argv), str(self.config.sections())]))

            tr_reader = AtlasRestReader(self.atlas_start, self.atlas_stop, timetrackconverter, 
                    self.atlas_msm_ids, self.atlas_probe_ids, chunk_size=self.atlas_chunk_size) 
            # tr_reader  = DumpReader(dump_name, dump_filter)

            # # Main Loop:
            with tr_reader:
                for track in tr_reader.read():
                    if not track:
                        continue

                    tm.add_track(track) 
                    aggregates = tm.aggregate()
                    self.save_aggregates(saver_queue, aggregates)

            logging.info("Finished to read data {}".format(datetime.datetime.today()))

            # Try to aggregate remaining track bins
            aggregates = tm.aggregate(force_expiration=0.5)
            self.save_aggregates(saver_queue, aggregates)

            logging.info("Number of ignored tracks {}".format(tm.nb_ignored_tracks))

            # closing
            saver_queue.join()
            saver_sqlite.terminate()
            detector_delay.terminate()

            logging.info("Ended on {}".format(datetime.datetime.today()))
        
        except Exception as e:
            print("type error: " + str(e))
            print(traceback.format_exc())

if __name__ == "__main__":
    ra = Raclette()
    ra.read_config()
    ra.main() 

