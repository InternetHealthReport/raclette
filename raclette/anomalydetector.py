import multiprocessing
import logging
from collections import deque, defaultdict
from bisect import insort, bisect_left
from itertools import islice
import numpy as np


class History():
    # TODO implement lagged history? compute the median for what happened 1 day
    # ago?
    def __init__(self, window_size):
        self.fifo = deque()
        self.sorted_values = []
        self.median_index = window_size // 2
        self.window_size = window_size

    def median(self):
        """Compute the median value of the history"""
        if len(self.sorted_values) < self.window_size:
            return self.sorted_values[len(self.sorted_values)//2]
        else:
            return self.sorted_values[self.median_index]

    def mad(self, median=None):
        """Compute the median absolute deviation of the history"""
        if median is None:
            median = self.median()
        return np.median(np.abs(np.array(self.sorted_values,copy=False)-median))

    def update(self, item):
        """Update history with latest value"""

        if len(self.sorted_values) < self.window_size:
            # Bootstrap: the history is not yet complete
            self.fifo.append(item)
            insort(self.sorted_values, item)
        else:
            # Usual case:
            old = self.fifo.popleft()
            self.fifo.append(item)
            del self.sorted_values[bisect_left(self.sorted_values, old) ]
            insort(self.sorted_values, item)


class AnomalyDetector(multiprocessing.Process):

    def __init__(self, input_pipe, saver_pipe, threshold=3, window_size=12*4):
        multiprocessing.Process.__init__(self)

        self.input_pipe = input_pipe

        self.history_tracks = defaultdict(lambda:History(window_size))
        self.history_conf_high= defaultdict(lambda:History(window_size))
        self.history_conf_low = defaultdict(lambda:History(window_size))
        self.history_median = defaultdict(lambda:History(window_size))
        self.threshold = threshold


    def run(self):
        logging.warn("Started detector")
        while True:
            locations, agg_tracks = self.input_pipe.recv()

            # Update histories with new values
            self.history_tracks[locations].update(agg_tracks["nb_tracks"])
            self.history_median[locations].update(agg_tracks["median"])
            # self.history_conf_high[locations].update(agg_tracks["conf_high"])
            # self.history_conf_low[locations].update(agg_tracks["conf_low"])

            # Check for diff. rtt anomalies
            # # old method
            # conf_high_ref = self.history_conf_high[locations].median()
            # conf_low_ref = self.history_conf_low[locations].median()
            # if len(self.history_conf_low[locations].sorted_values)>4 and (
                # agg_tracks["conf_high"] < conf_low_ref or agg_tracks["conf_low"] > conf_high_ref
                    # ):
                # #TODO report anomaly
                # print("Anomalous diff. RTT: ", locations, agg_tracks)
                # print(conf_low_ref, conf_high_ref)
            if len(self.history_tracks[locations].sorted_values)>12:
                dev = self.mad_detection(self.history_median[locations], agg_tracks["median"])
                if dev is not None and self.threshold < abs(dev):
                    # TODO proper anomaly report
                    print("Anomalous diff.rtt: ", locations, agg_tracks)
                    print(dev, len(self.history_median[locations].sorted_values), self.history_median[locations].median(), self.history_median[locations].mad())
                #TODO handle case where dev is None

                # Check for abnormal number of tracks
                dev = self.mad_detection(self.history_tracks[locations], agg_tracks["nb_tracks"])
                if dev is not None and self.threshold < abs(dev):
                    # TODO proper anomaly report
                    print("Anomalous #tracks: ", locations, agg_tracks)
                    print(agg_tracks["nb_tracks"])
                #TODO handle case where dev is None
            
    
    def mad_detection(self, hist, value):
        """Compute the classical x-sigma like value, but using median and MAD."""

        median = hist.median()
        mad = hist.mad(median=median)
        dev = mad*1.4826

        if dev < 1:
            dev = 1.0

        return (value-median)/(dev)



