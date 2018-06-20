import logging
import numpy as np
from collections import defaultdict 
from itertools import combinations, product


class TracksAggregator():

    def __init__(self, window_size, expiration):
        self.window_size = window_size
        self.nb_ignored_tracks = 0
        self.nb_empty_tracks = 0
        self.nb_tracks = 0
        self.track_bins = {}
        self.bins_last_insert= defaultdict(int)
        self.expiration = expiration
        self.nb_expired_bins = 0
        self.nb_expired_tracks = 0

    def add_track(self, track):
        """Add a new track to the history."""

        self.nb_tracks += 1
        if track is None:
            self.nb_empty_tracks += 1
            return

        bin_id = int(track["timestamp"]/self.window_size)
        if self.bins_last_insert[bin_id] is None:
            self.nb_ignored_tracks += 1
            return

        # index track based on its timestamp
        if bin_id not in self.track_bins:
            self.track_bins[bin_id] = []

        self.track_bins[bin_id].append(track)
        self.bins_last_insert[bin_id] = self.nb_tracks


    def compute_median_diff_rtt(self, tracks, results):
        diffrtt = defaultdict(list)
        for track in tracks:
            for locPair in combinations(track["rtts"],2):
                (loc0, rtts0) = locPair[0]
                (loc1, rtts1) = locPair[1]
                diffrtt[(loc0,loc1)] += [ x1-x0 for x0,x1 in product(rtts0, rtts1)]

        # Compute median/wilson scores and shift windows
        # TODO wilson
        if len(diffrtt):
            for locations, dr in diffrtt.iteritems():
                dra = np.array(dr, copy=False)
                results[str(locations)]["median"].append(np.median(dra))
                results[str(locations)]["nb_samples"].append(len(dra))

    def collect_results(self, results, dates, force_expiration=0):
        """Find expired track bins, that is bins that are not affected by the 
        last n track insertions (n=self.expiration). And compute median differential
        RTTs, number of sample, etc.. for expired bins. """

        if self.nb_tracks % self.expiration == 0 or force_expiration:

            logging.warn("Running results collection")
            expired_bins = []
            for date, tracks in self.track_bins.iteritems():

                # TODO: add probe diversity

                if self.bins_last_insert[date]+self.expiration < self.nb_tracks or force_expiration:
                    if force_expiration and not len(tracks)>force_expiration*(self.nb_expired_tracks/self.nb_expired_bins):
                        continue
                    else:
                        logging.debug("Force expiration for bin {}".format(date))

                    logging.warn("Processing bin {}".format(date))
                    expired_bins.append(date)

                    self.compute_median_diff_rtt(tracks, results)
                    dates.append(date*self.window_size+self.window_size/2)
                    self.nb_expired_tracks += len(tracks)
                    self.nb_expired_bins += 1

            # remember expired dates and delete corresponding bins
            for date in expired_bins:
                self.bins_last_insert[date] = None 
                del self.track_bins[date]


