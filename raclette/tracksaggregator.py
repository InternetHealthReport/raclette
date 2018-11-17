import logging
import numpy as np
import scipy
import statsmodels.api as sm
from collections import defaultdict 
from itertools import combinations, product
import tools

def normalized_entropy(pk):
    """ Computes normalized entropy of the given distribution. Does not copy 
    the input data.  Slightly faster than scipy implementation for small lists.
    """

    pk = np.array(pk, copy=False)
    pk = pk / float(np.sum(pk, axis=0))
    vec = scipy.special.entr(pk)

    return np.sum(vec, axis=0)/np.log(len(pk))


class TracksAggregator():
    """
    Sort tracks based on their timestamp and compute min and median differential 
    RTT for each time bin.
    """

    def __init__(self, window_size, expiration, significance_level, min_tracks):
        self.window_size = window_size
        self.significance_level = significance_level
        self.min_tracks = min_tracks
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
        if not track :
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


    def compute_median_diff_rtt(self, tracks):
        """Compute several statistics from the set of given tracks.
        The returned dictionnary provides for each pair of locations found in the
        tracks, the differential median RTT, minimum, entropy of probes ASN, 
        number of diff. RTT samples, number of unique probes."""

        results = {}
        counters = defaultdict(lambda: {
            "diffrtt": [], 
            "unique_probes": set(),
            "nb_tracks_per_asn": defaultdict(int),
            "nb_tracks": 0,
            "hop": []
            })

        for track in tracks:
            nb_hops = [hopnb for x in range(len(track["rtts"])-1,0,-1) for hopnb in range(1,x+1)]
            for hop, locPair in zip(nb_hops, combinations(track["rtts"],2)):
                (loc0, rtts0) = locPair[0]
                (loc1, rtts1) = locPair[1]
                count = counters[(loc0,loc1)]
                count["diffrtt"] += [ x1-x0 for x0,x1 in product(rtts0, rtts1)]
                count["nb_tracks_per_asn"][track["from_asn"]] += 1
                count["unique_probes"].add(track["prb_id"])
                count["nb_tracks"] += 1
                count["hop"].append(hop)

        # Compute median
        for locations, count in counters.items():
            if count["nb_tracks"]<self.min_tracks:
                continue

            count["diffrtt"].sort()
            entropy =  normalized_entropy(list(count["nb_tracks_per_asn"].values())) if len(count["nb_tracks_per_asn"])>1 else 0.0

            results[locations] = {
                    "median": count["diffrtt"][int(len(count["diffrtt"])/2)],
                    "min": count["diffrtt"][0],
                    "nb_tracks": count["nb_tracks"],
                    "nb_probes": len(count["unique_probes"]),
                    "entropy": entropy,
                    "hop": np.median(count["hop"])
                    }

        return results


    def aggregate(self, force_expiration=0):
        """Find expired track bins, that is bins that are not affected by the 
        last n track insertions (n=self.expiration). And compute median differential
        RTTs, number of sample, etc.. for expired bins. """

        results = {} 

        if self.nb_tracks % self.expiration == 0 or force_expiration:

            logging.debug("Running results collection")
            expired_bins = []

            for date, tracks in self.track_bins.items():

                if self.bins_last_insert[date]+self.expiration < self.nb_tracks or force_expiration:
                    if force_expiration and not len(tracks)>force_expiration*(self.nb_expired_tracks/self.nb_expired_bins):
                        continue
                    else:
                        logging.debug("Force expiration for bin {}".format(date))

                    logging.info("Processing bin {}".format(date))
                    expired_bins.append(date)

                    timebin = date*self.window_size+self.window_size/2
                    results[timebin] = self.compute_median_diff_rtt(tracks)
                    self.nb_expired_tracks += len(tracks)
                    self.nb_expired_bins += 1
                    logging.info("Finished processing bin {}".format(date))

            # remember expired dates and delete corresponding bins
            for date in expired_bins:
                self.bins_last_insert[date] = None 
                del self.track_bins[date]

        return results


