import multiprocessing
import logging
from collections import deque, defaultdict
from bisect import insort, bisect_left
from itertools import islice
import numpy as np
import json


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

    def __init__(self, input_pipe, saver_queue, threshold=5, window_size=12*4,
            min_dev_perc=0.01):
        multiprocessing.Process.__init__(self)

        self.input_pipe = input_pipe
        self.saver_queue = saver_queue

        self.window_size = window_size
        self.metrics = [ "nb_tracks", "median", "hop", "nb_real_rtts", 
                "nb_probes", "entropy"]
        self.reported_metrics = ["median", "nb_tracks"]
        self.history= defaultdict(lambda:defaultdict(lambda:History(window_size)))
        self.threshold = threshold
        self.min_dev_perc = min_dev_perc


    def run(self):
        logging.warn("Started detector")
        while True:
            date, locations, agg_tracks = self.input_pipe.recv()

            # Update histories with new values
            for metric in self.metrics:
                self.history[metric][locations].update(agg_tracks[metric])

            # Look for anomalies only if we have enough samples
            if len(self.history["nb_tracks"][locations].sorted_values)>self.window_size/2:
                anomaly = False

                dev = defaultdict(float)
                for metric in self.reported_metrics:
                    dev[metric] = self.mad_detection(
                            self.history[metric][locations], agg_tracks[metric])
                    if self.threshold < abs(dev[metric]):
                        anomaly = True

                if anomaly:
                    # Compute reliability
                    for metric in self.metrics:
                        if metric not in self.reported_metrics:
                            dev[metric] = self.mad_detection(
                                    self.history[metric][locations], 
                                    agg_tracks[metric])

                    asym = int(0.5+
                            1-(agg_tracks["nb_real_rtts"]/agg_tracks["nb_tracks"]))
                    # TODO check entropy for in-network anomalies
                    # asym*(1-agg_tracks["entropy"])+

                    reliability = np.mean([d for m, d in dev.items()
                                    if m not in ["median", "nb_tracks"]])
                    
                    self.saver_queue.put(
                        ("anomaly", [
                            date, locations[0], locations[1], 
                            json.dumps(dev), reliability
                            ])
                        )
            
    
    def mad_detection(self, hist, value):
        """Compute the classical x-sigma like value, but using median and MAD."""

        median = hist.median()
        mad = hist.mad(median=median)
        dev = mad*1.4826
        min_dev = median*self.min_dev_perc


        if dev < min_dev:
            dev = min_dev

        if dev == 0:
            return 0

        return (value-median)/(dev)



