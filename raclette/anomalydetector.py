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

    def __init__(self, input_pipe, saver_queue, threshold=5, window_size=12*4,
            min_dev_perc=0.01):
        multiprocessing.Process.__init__(self)

        self.input_pipe = input_pipe
        self.saver_queue = saver_queue

        self.window_size = window_size
        self.history_tracks = defaultdict(lambda:History(window_size))
        self.history_conf_high= defaultdict(lambda:History(window_size))
        self.history_conf_low = defaultdict(lambda:History(window_size))
        self.history_median = defaultdict(lambda:History(window_size))
        self.threshold = threshold
        self.min_dev_perc = min_dev_perc


    def run(self):
        logging.warn("Started detector")
        while True:
            date, locations, agg_tracks = self.input_pipe.recv()

            # Update histories with new values
            self.history_tracks[locations].update(agg_tracks["nb_tracks"])
            self.history_median[locations].update(agg_tracks["median"])

            if len(self.history_tracks[locations].sorted_values)>self.window_size/2:
                anomaly_delay = False
                anomaly_tracks = False
                dev_delay = self.mad_detection(self.history_median[locations], agg_tracks["median"])
                if self.threshold < abs(dev_delay):
                    anomaly_delay = True

                # Check for abnormal number of tracks
                dev_tracks = self.mad_detection(self.history_tracks[locations], agg_tracks["nb_tracks"])
                if self.threshold < abs(dev_tracks):
                    anomaly_tracks = True

                if anomaly_delay or anomaly_tracks:
                    self.saver_queue.put(
                        ("anomaly", [
                            date, locations[0], locations[1], 
                            anomaly_delay, dev_delay, 
                            anomaly_tracks, dev_tracks, 
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

        return (value-median)/(dev)



