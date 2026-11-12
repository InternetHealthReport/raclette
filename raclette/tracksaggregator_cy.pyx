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
import tools

from libc.math cimport log
from libc.stdint cimport uint64_t, uint32_t
from libcpp.algorithm cimport nth_element
from libcpp.vector cimport vector
from libcpp.pair cimport pair as cpp_pair
from libcpp.unordered_map cimport unordered_map
from cython.operator cimport dereference as deref, preincrement as inc
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cython.parallel import prange
cimport cython


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


# Bounds for the initial size of the location pair counting table.
DEF MIN_PAIR_BUCKETS = 1 << 16
DEF MAX_PAIR_BUCKETS = 1 << 26


cdef inline uint64_t pair_key(int loc0, int loc1) nogil:
    """Pack a pair of location ids into a single integer key."""
    return (<uint64_t> (<uint32_t> loc0) << 32) | <uint64_t> (<uint32_t> loc1)


@cython.boundscheck(False)
@cython.wraparound(False)
cdef void intern_track(list track_rtts, dict loc_ids, list loc_names,
        vector[int] &ids, vector[Py_ssize_t] &starts,
        vector[Py_ssize_t] &nb_rtts) except *:
    """Translate the locations of a track to integer ids.

    The ids of the k-th hop are ids[starts[k]:starts[k+1]], and nb_rtts[k] is
    the number of RTT samples of that hop. Working with ids instead of strings
    means the location pairs can be handled as plain integers, hence without
    building a python tuple for every one of them.
    """

    cdef str loc
    cdef object loc_id

    ids.clear()
    starts.clear()
    nb_rtts.clear()

    for loc_set, rtts in track_rtts:
        starts.push_back(ids.size())
        nb_rtts.push_back(len(rtts))
        for loc in loc_set.split("|"):
            loc_id = loc_ids.get(loc)
            if loc_id is None:
                loc_id = len(loc_names)
                loc_ids[loc] = loc_id
                loc_names.append(loc)
            ids.push_back(loc_id)

    starts.push_back(ids.size())


@cython.final
cdef class PairCounter:
    """Per location pair accumulator.

    The differential RTTs and the hop numbers are kept in C++ vectors instead
    of python lists. A python list of floats costs about 32 bytes per sample
    (8 for the list slot and 24 for the float object) whereas a vector of
    float32 needs 4, and a vector is already contiguous so the statistics
    below read it without copying it to a numpy array first. A large time bin
    holds billions of samples, so this is what decides whether the process
    fits in memory or not.

    float32 keeps about 7 significant digits, so a differential RTT of a few
    seconds is still exact to far below the microsecond precision of the RTTs
    reported by RIPE Atlas. The subtraction itself is done in double
    precision and narrowed only once, when the result is stored.
    """

    cdef vector[float] diffrtt
    cdef vector[int] hop
    cdef set unique_probes
    cdef dict nb_tracks_per_asn
    cdef long nb_tracks
    cdef long nb_samples
    cdef long nb_real_rtts

    def __cinit__(self):
        self.unique_probes = set()
        self.nb_tracks_per_asn = {}
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
        # number of location pairs of the last processed bin, used to size
        # the counting table of the next one
        self.last_nb_pairs = 0


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
    @cython.wraparound(False)
    def compute_median_diff_rtt(self, tracks):
        """Compute several statistics from the set of given tracks.
        The returned dictionnary provides for each pair of locations found in
        the tracks, the differential median RTT, minimum, entropy of
        probes ASN, number of diff. RTT samples, number of unique probes."""

        cdef int nblocations
        cdef str loc0
        cdef list rtts0, rtts1, track_rtts
        cdef PairCounter count
        cdef list counters = []
        cdef dict asn_counts
        cdef int min_tracks = self.min_tracks
        cdef bint prefiltered = min_tracks > 1
        cdef bint diff_done
        cdef Py_ssize_t a, b, k0, k1, nbhops, p, q, n0, n1
        cdef int id0, id1, hop, pair_id
        cdef double v0
        cdef uint64_t key
        cdef long nb_track_samples
        cdef Py_ssize_t size_hint

        # location pair bookkeeping. The pairs are identified by a packed
        # integer key, the maps below are C++ containers so that neither the
        # key nor the counts are allocated as python objects. A time bin holds
        # tens of millions of pairs, a python dict of tuples costs about 125
        # bytes for each of them.
        cdef unordered_map[uint64_t, cpp_pair[long, long]] pair_tracks
        cdef unordered_map[uint64_t, cpp_pair[long, long]].iterator tracks_it
        cdef cpp_pair[long, long] *pair_count
        cdef unordered_map[uint64_t, int] pair_ids
        cdef unordered_map[uint64_t, int].iterator ids_it
        cdef vector[uint64_t] pair_of_counter

        # location interning
        cdef dict loc_ids = {}
        cdef list loc_names = []
        cdef vector[int] ids
        cdef vector[Py_ssize_t] starts
        cdef vector[Py_ssize_t] nb_rtts
        # flags the locations of a track that count as a real RTT sample
        cdef vector[char] real_rtt

        # scratch buffers: the diff. RTTs of a hop pair are computed once and
        # then bulk copied to each location pair of that hop pair. buf1 holds
        # the second hop RTTs in double precision so that the subtraction is
        # not done on already narrowed values.
        cdef vector[double] buf1
        cdef vector[float] diffbuf

        results = {}

        if prefiltered and not isinstance(tracks, (list, tuple)):
            # the tracks are read twice below, make sure they can be
            tracks = list(tracks)

        if prefiltered:
            # First pass: only count how many tracks and how many samples
            # cross each pair of locations. Accumulating RTT samples for every
            # pair is what makes this function run out of memory on large time
            # bins, while the min_tracks filter discards the vast majority of
            # the pairs at the very end. Counting first is much cheaper than
            # computing the differential RTTs, and lets the second pass
            # allocate data for the surviving pairs only, at their exact size.
            logging.info("Counting location pairs")

            # Size the table up front. unordered_map grows by relinking every
            # node into a new bucket array, and that is dominated by random
            # memory accesses: reaching tens of millions of pairs one rehash
            # at a time costs more than the counting itself. Consecutive bins
            # cover the same topology, so the previous bin is a good
            # predictor, and the number of pairs is a few times the number of
            # tracks otherwise.
            if tracks:
                size_hint = 4 * len(tracks)
                if size_hint < self.last_nb_pairs:
                    size_hint = self.last_nb_pairs
                if size_hint < MIN_PAIR_BUCKETS:
                    size_hint = MIN_PAIR_BUCKETS
                elif size_hint > MAX_PAIR_BUCKETS:
                    size_hint = MAX_PAIR_BUCKETS
                pair_tracks.reserve(size_hint)

            for track in tracks:
                intern_track(track["rtts"], loc_ids, loc_names,
                        ids, starts, nb_rtts)
                nbhops = starts.size() - 1

                for k0 in range(nbhops):
                    for k1 in range(k0+1, nbhops):
                        nb_track_samples = nb_rtts[k0] * nb_rtts[k1]
                        for p in range(starts[k0], starts[k0+1]):
                            id0 = ids[p]
                            for q in range(starts[k1], starts[k1+1]):
                                id1 = ids[q]
                                if id0 == id1:
                                    continue
                                pair_count = &pair_tracks[pair_key(id0, id1)]
                                pair_count.first += 1
                                pair_count.second += nb_track_samples

            # Pre-allocate the accumulators of the surviving pairs only. Their
            # exact size is known, so the vectors below never have to grow:
            # growing them doubles their capacity, which wastes up to half of
            # the memory of the largest allocation of this function.
            tracks_it = pair_tracks.begin()
            while tracks_it != pair_tracks.end():
                if deref(tracks_it).second.first >= min_tracks:
                    count = PairCounter()
                    count.hop.reserve(deref(tracks_it).second.first)
                    count.diffrtt.reserve(deref(tracks_it).second.second)
                    pair_ids[deref(tracks_it).first] = len(counters)
                    pair_of_counter.push_back(deref(tracks_it).first)
                    counters.append(count)
                inc(tracks_it)

            self.last_nb_pairs = pair_tracks.size()
            logging.info("Keeping {} location pairs out of {}".format(
                len(counters), pair_tracks.size()))
            pair_tracks.clear()

        logging.info("Computing differential RTTs")

        for track in tracks:
            track_rtts = track["rtts"]
            intern_track(track_rtts, loc_ids, loc_names, ids, starts, nb_rtts)
            nbhops = starts.size() - 1
            from_asn = track["from_asn"]
            prb_id = track["prb_id"]
            first_loc_set = track_rtts[0][0]

            # If a location corresponds to the source locations or to the probe
            # ASN then its RTTs are counted as real RTT samples. This only
            # depends on the track, so it is resolved once per location here
            # instead of once per location pair below.
            real_rtt.resize(ids.size())
            for p in range(<Py_ssize_t> ids.size()):
                loc0 = <str> loc_names[ids[p]]
                real_rtt[p] = loc0 in first_loc_set or loc0 == from_asn

            for k0 in range(nbhops):
                rtts0 = track_rtts[k0][1]
                for k1 in range(k0+1, nbhops):
                    # locations are ordered along the path, so the number of
                    # hops between the two is simply their distance
                    hop = k1 - k0
                    # the differences are the same for every location pair of
                    # this hop pair, compute them at most once and only if at
                    # least one of these pairs survived the filter
                    diff_done = False

                    for p in range(starts[k0], starts[k0+1]):
                        id0 = ids[p]
                        for q in range(starts[k1], starts[k1+1]):
                            id1 = ids[q]
                            if id0 == id1:
                                continue

                            key = pair_key(id0, id1)
                            ids_it = pair_ids.find(key)
                            if ids_it == pair_ids.end():
                                if prefiltered:
                                    # dropped by the counting pass
                                    continue
                                pair_id = len(counters)
                                count = PairCounter()
                                pair_ids[key] = pair_id
                                pair_of_counter.push_back(key)
                                counters.append(count)
                            else:
                                count = <PairCounter> counters[
                                        deref(ids_it).second]

                            if not diff_done:
                                rtts1 = track_rtts[k1][1]
                                n0 = len(rtts0)
                                n1 = len(rtts1)
                                buf1.resize(n1)
                                for b in range(n1):
                                    buf1[b] = rtts1[b]
                                diffbuf.resize(n0*n1)
                                for a in range(n0):
                                    v0 = rtts0[a]
                                    for b in range(n1):
                                        diffbuf[a*n1+b] = <float> (buf1[b] - v0)
                                diff_done = True

                            # single memcpy instead of n0*n1 push_back
                            count.diffrtt.insert(count.diffrtt.end(),
                                    diffbuf.begin(), diffbuf.end())

                            asn_counts = count.nb_tracks_per_asn
                            if from_asn in asn_counts:
                                asn_counts[from_asn] += 1
                            else:
                                asn_counts[from_asn] = 1
                            count.unique_probes.add(prb_id)
                            count.nb_tracks += 1
                            count.nb_samples += diffbuf.size()
                            count.hop.push_back(hop)
                            if real_rtt[p]:
                                count.nb_real_rtts += 1

        pair_ids.clear()

        logging.info("Computing statistics")

        nblocations = len(counters)

        # The diff. RTTs and the hop numbers are already contiguous in the
        # vectors, only their addresses are collected here. The per-ASN track
        # counts are small, they are packed in a single array where the
        # values of a pair are the slice [offset[i], offset[i+1]).
        cdef float **diffrtt_ptr = <float **> PyMem_Malloc(
                (nblocations+1) * sizeof(float *))
        cdef Py_ssize_t *diffrtt_len = <Py_ssize_t *> PyMem_Malloc(
                (nblocations+1) * sizeof(Py_ssize_t))
        cdef int **hop_ptr = <int **> PyMem_Malloc(
                (nblocations+1) * sizeof(int *))
        cdef Py_ssize_t *hop_len = <Py_ssize_t *> PyMem_Malloc(
                (nblocations+1) * sizeof(Py_ssize_t))
        cdef Py_ssize_t *entropy_offset = <Py_ssize_t *> PyMem_Malloc(
                (nblocations+1) * sizeof(Py_ssize_t))
        cdef double *entropy_values =  <double *> PyMem_Malloc(
                (nblocations+1) * sizeof(double))
        cdef double *min_values =  <double *> PyMem_Malloc(
                (nblocations+1) * sizeof(double))
        cdef double *median_values =  <double *> PyMem_Malloc(
                (nblocations+1) * sizeof(double))
        cdef double *hop_values =  <double *> PyMem_Malloc(
                (nblocations+1) * sizeof(double))
        cdef long [:] entropy_mv
        cdef long *entropy_base = NULL
        cdef int loc_idx
        cdef float *begin
        cdef float minval
        cdef int *hbegin
        cdef int upper, lower
        cdef Py_ssize_t nbelem, nbhop, j
        cdef Py_ssize_t epos = 0

        for loc_idx in range(nblocations):
            count = <PairCounter> counters[loc_idx]
            entropy_offset[loc_idx] = epos
            epos += len(count.nb_tracks_per_asn)
            diffrtt_ptr[loc_idx] = count.diffrtt.data()
            diffrtt_len[loc_idx] = count.diffrtt.size()
            hop_ptr[loc_idx] = count.hop.data()
            hop_len[loc_idx] = count.hop.size()
        entropy_offset[nblocations] = epos

        entropy_all = np.empty(epos, dtype=np.int64)

        for loc_idx in range(nblocations):
            count = <PairCounter> counters[loc_idx]
            asn_counts = count.nb_tracks_per_asn
            if asn_counts:
                entropy_all[entropy_offset[loc_idx]:entropy_offset[loc_idx+1]] \
                        = list(asn_counts.values())
            count.nb_tracks_per_asn = None

        if epos > 0:
            entropy_mv = entropy_all
            entropy_base = &entropy_mv[0]

        for loc_idx in prange(nblocations, nogil=True):
            nbelem = diffrtt_len[loc_idx]
            if nbelem > 0:
                begin = diffrtt_ptr[loc_idx]
                # partial sort: only place the median at its final position
                # (O(n) instead of O(n log n) for a full sort)
                nth_element(begin, begin + nbelem//2, begin + nbelem)
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

            # median number of hops, same definition as numpy.median: the
            # average of the two central values when the count is even
            nbhop = hop_len[loc_idx]
            if nbhop > 0:
                hbegin = hop_ptr[loc_idx]
                nth_element(hbegin, hbegin + nbhop//2, hbegin + nbhop)
                upper = hbegin[nbhop//2]
                if nbhop % 2 == 1:
                    hop_values[loc_idx] = upper
                else:
                    lower = hbegin[0]
                    for j in range(1, nbhop//2):
                        if hbegin[j] > lower:
                            lower = hbegin[j]
                    hop_values[loc_idx] = (lower + upper)/2.0
            else:
                hop_values[loc_idx] = 0

            entropy_values[loc_idx] = normalized_entropy(
                    entropy_base + entropy_offset[loc_idx],
                    <int> (entropy_offset[loc_idx+1] - entropy_offset[loc_idx]))

        PyMem_Free(diffrtt_ptr)
        PyMem_Free(diffrtt_len)
        PyMem_Free(hop_ptr)
        PyMem_Free(hop_len)
        PyMem_Free(entropy_offset)

        # the samples are not needed anymore, release them before building
        # the results
        for loc_idx in range(nblocations):
            count = <PairCounter> counters[loc_idx]
            count.diffrtt.clear()
            count.diffrtt.shrink_to_fit()
            count.hop.clear()
            count.hop.shrink_to_fit()

        for loc_idx in range(nblocations):
            count = <PairCounter> counters[loc_idx]
            if count.nb_tracks<min_tracks:
                continue

            key = pair_of_counter[loc_idx]
            locations = (loc_names[<Py_ssize_t> (key >> 32)],
                    loc_names[<Py_ssize_t> (key & 0xFFFFFFFF)])

            # median, min, hop and entropy are precomputed in parallel before
            # this loop
            results[locations] = {
                    "median": median_values[loc_idx],
                    "min": min_values[loc_idx],
                    "nb_tracks": count.nb_tracks,
                    "nb_samples": count.nb_samples,
                    "nb_probes": len(count.unique_probes),
                    "entropy": entropy_values[loc_idx],
                    "hop": hop_values[loc_idx],
                    "nb_real_rtts": count.nb_real_rtts
                    }

        PyMem_Free(entropy_values)
        PyMem_Free(min_values)
        PyMem_Free(median_values)
        PyMem_Free(hop_values)

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
