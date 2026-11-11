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
@cython.nonecheck(False)
def enumerate_loc_pairs(track_rtts):
    """Enumerate the location pairs of a track.

    Yields exactly the same pairs as enumerate_loc_diffrtt but skips the
    differential RTT computation. Used by the counting pass of
    compute_median_diff_rtt.
    """

    cdef list locs0, locs1
    cdef str loc0, loc1

    split_locs = [loc_set.split("|") for loc_set, _ in track_rtts]

    for locs0, locs1 in combinations(split_locs, 2):
        for loc0 in locs0:
            for loc1 in locs1:
                if loc0 != loc1:
                    yield (loc0, loc1)


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


@cython.final
cdef class PairCounter:
    """Per location pair accumulator.

    Replaces the plain 7-key dict used before. A dict costs about 272 bytes
    per location pair whereas this extension type only needs the object
    header plus seven slots. With hundreds of thousands of pairs per time
    bin the counters are one of the largest structures of the aggregation
    phase, so the saving is substantial.
    """

    cdef list diffrtt
    cdef set unique_probes
    cdef dict nb_tracks_per_asn
    cdef list hop
    cdef long nb_tracks
    cdef long nb_samples
    cdef long nb_real_rtts

    def __cinit__(self):
        self.diffrtt = []
        self.unique_probes = set()
        self.nb_tracks_per_asn = {}
        self.hop = []
        self.nb_tracks = 0
        self.nb_samples = 0
        self.nb_real_rtts = 0


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

        cdef int nblocations
        cdef int i, hopnb
        cdef double x0, x1
        cdef str loc0, loc1
        cdef PairCounter count
        cdef dict counters = {}
        cdef dict asn_counts
        cdef dict pair_tracks
        cdef long nb_pair_tracks
        cdef int min_tracks = self.min_tracks
        cdef bint prefiltered = min_tracks > 1

        results = {}
        nb_hops_cache = self.nb_hops_cache

        if prefiltered and not isinstance(tracks, (list, tuple)):
            # the tracks are read twice below, make sure they can be
            tracks = list(tracks)

        if prefiltered:
            # First pass: only count how many tracks cross each pair of
            # locations. Accumulating RTT samples for every pair is what
            # makes this function run out of memory on large time bins,
            # while the min_tracks filter discards the vast majority of the
            # pairs at the very end. Counting first is much cheaper than
            # computing the differential RTTs, and lets the second pass
            # allocate data for the surviving pairs only.
            logging.info("Counting location pairs")
            pair_tracks = {}
            for track in tracks:
                for locations in enumerate_loc_pairs(track["rtts"]):
                    if locations in pair_tracks:
                        pair_tracks[locations] += 1
                    else:
                        pair_tracks[locations] = 1

            for locations, nb_pair_tracks in pair_tracks.items():
                if nb_pair_tracks >= min_tracks:
                    counters[locations] = PairCounter()
            logging.info("Keeping {} location pairs out of {}".format(
                len(counters), len(pair_tracks)))
            pair_tracks = None

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
                    count = counters.get(locations)
                    if count is None:
                        if prefiltered:
                            # dropped by the counting pass
                            continue
                        count = PairCounter()
                        counters[locations] = count
                    count.diffrtt += diffrtt
                    asn_counts = count.nb_tracks_per_asn
                    if from_asn in asn_counts:
                        asn_counts[from_asn] += 1
                    else:
                        asn_counts[from_asn] = 1
                    count.unique_probes.add(prb_id)
                    count.nb_tracks += 1
                    count.nb_samples += len(diffrtt)
                    count.hop.append(hop)
                    # If the locations correspond to the source locations
                    # or the probe ASN then we count this as a real RTT
                    # sample
                    if locations[0] in track["rtts"][0][0] \
                        or locations[0] == track["from_asn"]:
                            count.nb_real_rtts += 1

        logging.info("Computing statistics")

        nblocations = len(counters)

        # The differential RTTs and the per-ASN track counts of all location
        # pairs are packed in a single array each, the values of a pair are
        # the slice [offset[i], offset[i+1]).
        # Using one numpy array per location pair instead wastes a lot of
        # memory: an array header is about 190 bytes while a typical pair only
        # holds a handful of samples.
        cdef Py_ssize_t *diffrtt_offset = <Py_ssize_t *> PyMem_Malloc(
                (nblocations+1) * sizeof(Py_ssize_t))
        cdef Py_ssize_t *entropy_offset = <Py_ssize_t *> PyMem_Malloc(
                (nblocations+1) * sizeof(Py_ssize_t))
        cdef double *entropy_values =  <double *> PyMem_Malloc(
                nblocations * sizeof(double))
        cdef double *min_values =  <double *> PyMem_Malloc(
                nblocations * sizeof(double))
        cdef double *median_values =  <double *> PyMem_Malloc(
                nblocations * sizeof(double))
        cdef double [:] diffrtt_mv
        cdef long [:] entropy_mv
        cdef double *diffrtt_base = NULL
        cdef long *entropy_base = NULL
        cdef int loc_idx
        cdef double *begin
        cdef double *end
        cdef double minval
        cdef Py_ssize_t nbelem, j
        cdef Py_ssize_t dpos = 0
        cdef Py_ssize_t epos = 0

        for loc_idx, count in enumerate(counters.values()):
            diffrtt_offset[loc_idx] = dpos
            entropy_offset[loc_idx] = epos
            dpos += len(count.diffrtt)
            epos += len(count.nb_tracks_per_asn)
        diffrtt_offset[nblocations] = dpos
        entropy_offset[nblocations] = epos

        diffrtt_all = np.empty(dpos, dtype=np.float64)
        entropy_all = np.empty(epos, dtype=np.int64)

        for loc_idx, count in enumerate(counters.values()):
            diffrtt = count.diffrtt
            if diffrtt:
                diffrtt_all[diffrtt_offset[loc_idx]:diffrtt_offset[loc_idx+1]] \
                        = diffrtt
            # release the python list as soon as it is copied, it is the
            # largest structure of the aggregation phase
            count.diffrtt = None

            asn_counts = count.nb_tracks_per_asn
            if asn_counts:
                entropy_all[entropy_offset[loc_idx]:entropy_offset[loc_idx+1]] \
                        = list(asn_counts.values())
            count.nb_tracks_per_asn = None

        if dpos > 0:
            diffrtt_mv = diffrtt_all
            diffrtt_base = &diffrtt_mv[0]
        if epos > 0:
            entropy_mv = entropy_all
            entropy_base = &entropy_mv[0]

        for loc_idx in prange(nblocations, nogil=True):
            begin = diffrtt_base + diffrtt_offset[loc_idx]
            end = diffrtt_base + diffrtt_offset[loc_idx+1]
            nbelem = end - begin
            if nbelem > 0:
                # partial sort: only place the median at its final position
                # (O(n) instead of O(n log n) for a full sort)
                nth_element(begin, begin + nbelem//2, end)
                median_values[loc_idx] = begin[nbelem//2]
                # elements before the median position are all <= median,
                # so the minimum is among the first nbelem//2+1 elements
                minval = begin[0]
                for j in range(1, nbelem//2 + 1):
                    if begin[j] < minval:
                        minval = begin[j]
                min_values[loc_idx] = minval
            else:
                median_values[loc_idx] = 0
                min_values[loc_idx] = 0
            entropy_values[loc_idx] = normalized_entropy(
                    entropy_base + entropy_offset[loc_idx],
                    <int> (entropy_offset[loc_idx+1] - entropy_offset[loc_idx]))

        PyMem_Free(diffrtt_offset)
        PyMem_Free(entropy_offset)

        for loc_idx, (locations, count) in enumerate(counters.items()):
            if count.nb_tracks<min_tracks:
                continue

            # median, min and entropy are precomputed in parallel before
            # this loop
            results[locations] = {
                    "median": median_values[loc_idx],
                    "min": min_values[loc_idx],
                    "nb_tracks": count.nb_tracks,
                    "nb_samples": count.nb_samples,
                    "nb_probes": len(count.unique_probes),
                    "entropy": entropy_values[loc_idx],
                    "hop": np.median(count.hop),
                    "nb_real_rtts": count.nb_real_rtts
                    }

        PyMem_Free(entropy_values)
        PyMem_Free(min_values)
        PyMem_Free(median_values)

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


