# distutils: language = c++
# cython: language_level=3

import traceback
# except Exception as e:
    # print("type error: " + str(e))
    # print(traceback.format_exc())
import logging
import numpy as np
import statsmodels.api as sm
from collections import defaultdict
from itertools import combinations, product
import tools

from libc.math cimport log
from libcpp.algorithm cimport nth_element
from libcpp.vector cimport vector
from libcpp.pair cimport pair as cpp_pair
from cython.operator cimport dereference as deref, preincrement as inc
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cython.parallel import prange
cimport cython

@cython.boundscheck(False)
@cython.nonecheck(False)
def enumerate_loc_diffrtt(nb_hops, track_rtts):
    """Enumerate over all locations and compute diffrtt"""

    cdef double x1,x0
    cdef list rtts0, rtts1, locs0, locs1
    cdef str loc0, loc1

    # split each location set only once instead of once per location pair
    split_rtts = [(loc_set.split("|"), rtts) for loc_set, rtts in track_rtts]

    for hop, ((locs0, rtts0),(locs1, rtts1)) in \
            zip(nb_hops, combinations(split_rtts,2)):
        # double for loops are faster than itertools.product
        # diffrtt =  [ x1-x0 for x0, x1 in product(rtts0,rtts1)]
        diffrtt =  [ x1-x0 for x0 in rtts0 for x1 in rtts1]

        for loc0 in locs0:
            for loc1 in locs1:
                if loc0 != loc1:
                    yield (hop, diffrtt, (loc0, loc1))


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cdef double normalized_entropy(long *count, int nbelem) nogil:
    """ Computes normalized entropy of the given distribution. Does not copy
    the input data.  Slightly faster than scipy implementation for small lists.
    """

    cdef Py_ssize_t i
    cdef double p
    cdef double entropysum = 0
    cdef double countsum = 0

    if nbelem < 2:
        return 0.0

    for i in range(nbelem):
        countsum += count[i]

    for i in range(nbelem):
        # entropy element-wise, skipping empty bins (log(0) undefined)
        if count[i] > 0:
            p = count[i]/countsum
            entropysum -= p*log(p)

    return entropysum/log(nbelem)


class TracksAggregator():
    """
    Sort tracks based on their timestamp and compute minimum and median
    differential RTT for each time bin.
    """

    def __init__(self, window_size, significance_level, min_tracks):
        self.window_size = window_size
        self.significance_level = significance_level
        self.min_tracks = min_tracks
        self.nb_ignored_tracks = 0
        self.nb_empty_tracks = 0
        self.track_bins = {}
        self.msm_bin = defaultdict(int)
        self.nb_expired_bins = 0
        self.nb_expired_tracks = 0
        self.nb_hops_cache = {}


    def add_track(self, track):
        """Add a new track to the history."""

        cdef double win_size = self.window_size
        cdef long ts

        if not track :
            self.nb_empty_tracks += 1
            return

        ts = track["timestamp"]
        bin_id = int(ts/win_size)

        if self.msm_bin[track['msm_id']] > bin_id \
                and bin_id not in self.track_bins:
            # Belong to expired bins
            self.nb_ignored_tracks += 1
            return

        # index track based on its timestamp
        try:
            self.track_bins[bin_id].append(track)
        except KeyError:
            self.track_bins[bin_id] = [track]

        # new bin: can we process the previous bins?
        if self.msm_bin[track['msm_id']] < bin_id:
            self.msm_bin[track['msm_id']] = bin_id
            return self.aggregate()

        return


    @cython.boundscheck(False)
    def compute_median_diff_rtt(self, tracks):
        """Compute several statistics from the set of given tracks.
        The returned dictionnary provides for each pair of locations found in
        the tracks, the differential median RTT, minimum, entropy of
        probes ASN, number of diff. RTT samples, number of unique probes."""

        cdef int nbsamples
        cdef int nblocations
        cdef int i, hopnb
        cdef double x0, x1
        cdef str loc0, loc1

        results = {}
        counters = defaultdict(lambda: {
            "diffrtt": [],
            "unique_probes": set(),
            "nb_tracks_per_asn": defaultdict(int),
            "nb_tracks": 0,
            "nb_samples": 0,
            "hop": [],
            "nb_real_rtts": 0,
            })
        nb_hops_cache = self.nb_hops_cache
        logging.info("Computing differential RTTs")

        for track in tracks:
            nblocations = len(track["rtts"])
            from_asn = track["from_asn"]
            prb_id = track["prb_id"]

            try:
                nb_hops = nb_hops_cache[nblocations]
            except KeyError:
                nb_hops = [hopnb for i in range(nblocations-1,0,-1)
                        for hopnb in range(1,i+1)]
                self.nb_hops_cache[nblocations] = nb_hops

            for hop, diffrtt, locations in \
                enumerate_loc_diffrtt(nb_hops, track["rtts"]):
                    count = counters[locations]
                    count["diffrtt"] += diffrtt
                    count["nb_tracks_per_asn"][from_asn] += 1
                    count["unique_probes"].add(prb_id)
                    count["nb_tracks"] += 1
                    count["nb_samples"] += len(diffrtt)
                    count["hop"].append(hop)
                    # If the locations correspond to the source locations
                    # or the probe ASN then we count this as a real RTT
                    # sample
                    if locations[0] in track["rtts"][0][0] \
                        or locations[0] == track["from_asn"]:
                            count["nb_real_rtts"] += 1

        logging.info("Computing statistics")
        # Compute median
        for count in counters.values():
            count["diffrtt"] = np.asarray(count["diffrtt"])

        nblocations = len(counters.values())
        cdef double * *array_diffrtt =  <double**> PyMem_Malloc(
                nblocations * 2 * sizeof(double*))
        cdef long * *array_entropy =  <long **> PyMem_Malloc(
                nblocations * sizeof(long*))
        cdef int * array_entropy_size =  <int *> PyMem_Malloc(
                nblocations * sizeof(int))
        cdef double *entropy_values =  <double *> PyMem_Malloc(
                nblocations * sizeof(double))
        cdef double *min_values =  <double *> PyMem_Malloc(
                nblocations * sizeof(double))
        cdef double [:] tmp_array
        cdef long [:] tmp_ent
        cdef int loc_idx
        cdef double *begin
        cdef double *end
        cdef double minval
        cdef Py_ssize_t nbelem, j

        # keep a reference to the entropy arrays so they are not garbage
        # collected before the prange loop below uses their data pointers
        entropy_refs = []

        for loc_idx, x in enumerate(counters.values()):
            tmp_array = x["diffrtt"]
            array_diffrtt[loc_idx*2] = &tmp_array[0]
            array_diffrtt[loc_idx*2+1] = &tmp_array[tmp_array.shape[0]]

            ent_arr = np.asarray(list(x["nb_tracks_per_asn"].values()))
            entropy_refs.append(ent_arr)
            tmp_ent = ent_arr
            array_entropy[loc_idx] = &tmp_ent[0]
            array_entropy_size[loc_idx] = tmp_ent.shape[0]

        for loc_idx in prange(nblocations, nogil=True):
            begin = array_diffrtt[loc_idx*2]
            end = array_diffrtt[loc_idx*2+1]
            nbelem = end - begin
            if nbelem > 0:
                # partial sort: only place the median at its final position
                # (O(n) instead of O(n log n) for a full sort)
                nth_element(begin, begin + nbelem//2, end)
                # elements before the median position are all <= median,
                # so the minimum is among the first nbelem//2+1 elements
                minval = begin[0]
                for j in range(1, nbelem//2 + 1):
                    if begin[j] < minval:
                        minval = begin[j]
                min_values[loc_idx] = minval
            else:
                min_values[loc_idx] = 0
            entropy_values[loc_idx] = normalized_entropy(
                    array_entropy[loc_idx], array_entropy_size[loc_idx])

        PyMem_Free(array_diffrtt)
        PyMem_Free(array_entropy)
        PyMem_Free(array_entropy_size)

        min_tracks = self.min_tracks
        for loc_idx, (locations, count) in enumerate(counters.items()):
            if count["nb_tracks"]<min_tracks:
                continue

            diff_rtt_values = count["diffrtt"]
            nbsamples = diff_rtt_values.shape[0]
            # median, min and entropy are precomputed in parallel before
            # this loop
            entropy = entropy_values[loc_idx]

            results[locations] = {
                    "median": count["diffrtt"][int(nbsamples/2)],
                    "min": min_values[loc_idx],
                    "nb_tracks": count["nb_tracks"],
                    "nb_samples": count["nb_samples"],
                    "nb_probes": len(count["unique_probes"]),
                    "entropy": entropy,
                    "hop": np.median(count["hop"]),
                    "nb_real_rtts": count["nb_real_rtts"]
                    }

        PyMem_Free(entropy_values)
        PyMem_Free(min_values)

        return results


    def aggregate(self, force_expiration=0):
        """Find expired track bins, that is bins that are not affected by the
        last n track insertions (n=self.expiration). And compute median
        differential RTTs, number of sample, etc.. for expired bins. """

        results = {}

        logging.debug("Running results collection")
        expired_bins = []

        if not self.track_bins:
            logging.warning("No track to aggregate")
            return results

        # msm_bin may be empty if no track was ever added
        min_processed_bin = min(self.msm_bin.values(), default=0)

        for date, tracks in self.track_bins.items():

            if min_processed_bin > date or force_expiration:
                if self.nb_expired_bins > 0 and force_expiration and  \
                        len(tracks)<force_expiration*(self.nb_expired_tracks/self.nb_expired_bins):
                    continue
                else:
                    logging.debug("Force expiration for bin {}".format(date))

                logging.info("Processing bin {} ({} tracks)".format(date, len(tracks)))
                expired_bins.append(date)

                timebin = date*self.window_size+self.window_size/2
                results[timebin] = self.compute_median_diff_rtt(tracks)
                self.nb_expired_tracks += len(tracks)
                self.nb_expired_bins += 1
                logging.info("Finished processing bin {}".format(date))

        # delete corresponding bins
        for date in expired_bins:
            del self.track_bins[date]

        return results


