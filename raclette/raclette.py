import sys
import os
import logging
import datetime
from collections import defaultdict
import pickle
import configparser
import argparse
from multiprocessing import Process, Pool, Queue, Pipe
import importlib
import traceback

import tools

from tracksaggregator_cy import TracksAggregator
from anomalydetector import AnomalyDetector


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
        parser.add_argument("-C","--config_file", 
                help="Get all parameters from the specified config file", 
                type=str, default="conf/raclette.conf")
        args = parser.parse_args()

        # Read the config file
        config = configparser.ConfigParser()
        config.read(args.config_file)

        try:
            self.reader = config.get("io", "reader")
        except configparser.NoOptionError:
            # Default to Atlas REST 
            self.reader = "atlasrestreader"

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
        try:
            self.detection_enabled = int(config.get("anomalydetector", "enable"))
        except configparser.NoOptionError:
            self.detection_enabled = 0

        self.ip2asn_dir = config.get("lib", "ip2asn_directory")
        self.ip2asn_db = config.get("lib", "ip2asn_db")
        self.ip2asn_ixp = config.get("lib", "ip2asn_ixp")

        self.tm_window_size = int(config.get("tracksaggregator", "window_size"))
        self.tm_significance_level = float(config.get("tracksaggregator", "significance_level"))
        self.tm_min_tracks = int(config.get("tracksaggregator", "min_tracks"))

        try:
            self.saver = config.get("io", "saver")
        except configparser.NoOptionError:
            # Default to SQLite saver 
            self.saver = "sqlitesaver"

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


    def correct_times(self):
        """
        If start and stop times are not valid then analyze the last time window
        """

        # Timestamps are not valid, analyze the last time window
        if self.atlas_start is None and self.atlas_stop is None:
            currentTime = datetime.datetime.utcnow()
            window_size = int(self.tm_window_size/60)
            minutebin = int(currentTime.minute / window_size)*window_size
            self.atlas_start = currentTime.replace(microsecond=0, second=0, minute=minutebin)-datetime.timedelta(minutes=window_size)
            self.atlas_stop = currentTime.replace(microsecond=0, second=0, minute=minutebin)
            logging.warning('Set start and stop times: {}, {}'.format(self.atlas_start, self.atlas_stop))
            

    def save_aggregates(self, saver_queue, aggregates):
        """
        Save differential RTTs values on disk.
        """

        for date, results in aggregates.items():
            saver_queue.put("BEGIN TRANSACTION;")
            [saver_queue.put(
                    ("diffrtt", 
                    (date, locations[0], locations[1], agg["median"], 
                        agg["min"], agg["nb_samples"],
                        agg["nb_tracks"], agg["nb_probes"], agg["entropy"], 
                        agg["hop"], agg["nb_real_rtts"]))
                )
                for locations, agg in results.items()]
            saver_queue.put("COMMIT;")
            if self.detection_enabled:
                [self.detector_pipe[1].send((date, locations, agg)) 
                        for locations, agg in results.items()]


    def main(self):
        """
        Main program connecting all modules.
        """

        try:
            # Saver initialisation
            saver_queue = Queue()
            try:
                saver_module = importlib.import_module(self.saver)
                # These are run in a separate process
                saver = saver_module.Saver(self.saver_filename, saver_queue)
                saver.start()
            except ImportError:
                logging.error("Saver unknown! ({})".format(self.saver))
                traceback.print_exc(file=sys.stdout)
                return

            # Detector initialisation
            if self.detection_enabled:
                self.detector_pipe = Pipe(False)
                detector = AnomalyDetector(self.detector_pipe[0], saver_queue)
                detector.start()

            # Time Track initialisation
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

            # Aggregator initialisation
            tm = TracksAggregator(self.tm_window_size, self.tm_significance_level, self.tm_min_tracks)
            saver_queue.put(("experiment", [datetime.datetime.now(), str(sys.argv), str(self.config.sections())]))

            # Reader initialisation
            try:
                reader_module = importlib.import_module(self.reader)
                tr_reader = reader_module.Reader(self.atlas_start, self.atlas_stop, timetrackconverter, 
                    self.atlas_msm_ids, self.atlas_probe_ids, chunk_size=self.atlas_chunk_size, config=self.config) 
            # tr_reader  = DumpReader(dump_name, dump_filter)
            except ImportError:
                logging.error("Reader unknown! ({})".format(self.reader))
                traceback.print_exc(file=sys.stdout)
                return

            # # Main Loop:
            with tr_reader:
                for track in tr_reader.read():
                    if not track:
                        continue

                    aggregates = tm.add_track(track) 
                    if aggregates:
                        self.save_aggregates(saver_queue, aggregates)

            logging.info("Finished to read data {}".format(datetime.datetime.today()))

            # Try to get results from remaining track bins
            aggregates = tm.aggregate(force_expiration=0.5)
            self.save_aggregates(saver_queue, aggregates)

            logging.info("Number of ignored tracks {}".format(tm.nb_ignored_tracks))

            # closing
            saver_queue.put("MAIN_FINISHED")
            saver.join()
            # saver.terminate()
            if self.detection_enabled:
                detector.terminate()

            logging.info("Ended on {}".format(datetime.datetime.today()))
        
        except Exception as e:
            print("type error: " + str(e))
            print(traceback.format_exc())

if __name__ == "__main__":
    ra = Raclette()
    ra.read_config()
    ra.correct_times()
    ra.main() 

