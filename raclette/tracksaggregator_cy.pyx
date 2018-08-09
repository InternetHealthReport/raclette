# distutils: language = c++


# import traceback
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
from libcpp.algorithm cimport sort as cpp_sort
from libcpp.vector cimport vector
from libcpp.pair cimport pair as cpp_pair
from cython.operator cimport dereference as deref, preincrement as inc
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from libc.stdlib cimport malloc, free
from cython.parallel import prange
cimport cython

@cython.boundscheck(False) 
@cython.nonecheck(False) 
def enumerate_loc_diffrtt(nb_hops, track_rtts):
    cdef double x1,x0
    cdef str loc0, loc1

    for hop, ((loc_set0, rtts0),(loc_set1, rtts1)) in zip(nb_hops, combinations(track_rtts,2)):
        diffrtt =  [ x1-x0 for x0,x1 in product(rtts0, rtts1)] 

        for locations in product(loc_set0, loc_set1):
                if loc0 == loc1:
                    continue

                yield hop, diffrtt, locations


@cython.boundscheck(False) 
cdef double normalized_entropy(long *count, int nbelem) nogil:
    """ Computes normalized entropy of the given distribution. Does not copy 
    the input data.  Slightly faster than scipy implementation for small lists.
    """

    cdef Py_ssize_t i
    cdef double entropyelem
    cdef double entropysum = 0
    cdef double countsum = 0
    cdef double *norm_count = <double*> malloc( nbelem * sizeof(double))

    if nbelem < 2:
        return 0.0

    for i in prange(nbelem):
        countsum+=count[i]

    # normalize
    for i in prange(nbelem):
        norm_count[i] = count[i]/countsum

        # entropy element-wise
        if norm_count[i]<=0:
            entropyelem=0
        else:
            entropyelem = -norm_count[i]*log(norm_count[i])

        entropysum += entropyelem

    free(norm_count)

    return entropysum/log(nbelem)


class TracksAggregator():
    """
    Sort tracks based on their timestamp and compute median differential RTT and
    wilson score for each time bin.
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
        self.wilson_cache = {}
        self.nb_hops_cache = {}


    def add_track(self, track):
        """Add a new track to the history."""

        cdef double win_size = self.window_size
        cdef long ts = track["timestamp"]

        self.nb_tracks += 1
        if not track :
            self.nb_empty_tracks += 1
            return

        bin_id = int(ts/win_size)
        if self.bins_last_insert[bin_id] is None:
            self.nb_ignored_tracks += 1
            return

        # index track based on its timestamp
        try:
            self.track_bins[bin_id].append(track)
        except KeyError:
            self.track_bins[bin_id] = [track]

        self.bins_last_insert[bin_id] = self.nb_tracks


    @cython.boundscheck(False) 
    def compute_median_diff_rtt(self, tracks):
        """Compute several statistics from the set of given tracks.
        The returned dictionnary provides for each pair of locations found in the
        tracks, the differential median RTT, wilson scores, entropy of probes ASN, 
        number of diff. RTT samples, number of unique probes."""

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
            "hop": []
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
                nb_hops = [hopnb for i in range(nblocations-1,0,-1) for hopnb in range(1,i+1)]
                self.nb_hops_cache[nblocations] = nb_hops

            # for hop, ((loc_set0,rtts0),(loc_set1,rtts1)) in zip(nb_hops, combinations(track["rtts"],2)):
                # diffrtt =  [ x1-x0 for x0,x1 in product(rtts0, rtts1)] 

                # for locations in product(loc_set0, loc_set1):

            for hop, diffrtt, locations in enumerate_loc_diffrtt(nb_hops, track["rtts"]): 
                    count = counters[locations]
                    count["diffrtt"] += diffrtt 
                    count["nb_tracks_per_asn"][from_asn] += 1
                    count["unique_probes"].add(prb_id)
                    count["nb_tracks"] += 1
                    count["hop"].append(hop)

        logging.info("Computing statistics")
        # Compute median/wilson scores 
        for count in counters.values():
            count["diffrtt"] = np.asarray(count["diffrtt"])

        nblocations = len(counters.values())
        cdef double * *array_diffrtt =  <double**> PyMem_Malloc( nblocations * 2 * sizeof(double*))
        cdef long * *array_entropy =  <long **> PyMem_Malloc( nblocations * sizeof(long*))
        cdef int * array_entropy_size =  <int *> PyMem_Malloc( nblocations * sizeof(int))
        cdef double *entropy_values =  <double *> PyMem_Malloc( nblocations * sizeof(double))
        cdef double [:] tmp_array
        cdef long [:] tmp_ent
        cdef int loc_idx
        
        for loc_idx, x in enumerate(counters.values()):
            tmp_array = x["diffrtt"]
            array_diffrtt[loc_idx*2] = &tmp_array[0]
            array_diffrtt[loc_idx*2+1] = &tmp_array[tmp_array.shape[0]]

            tmp_ent = np.asarray(list(x["nb_tracks_per_asn"].values()))
            array_entropy[loc_idx] = &tmp_ent[0]
            array_entropy_size[loc_idx] = tmp_ent.shape[0]

        for loc_idx in prange(nblocations, nogil=True):
            cpp_sort(array_diffrtt[loc_idx*2], array_diffrtt[loc_idx*2+1])
            entropy_values[loc_idx] = normalized_entropy(array_entropy[loc_idx], array_entropy_size[loc_idx]) 

        PyMem_Free(array_diffrtt)
        PyMem_Free(array_entropy)
        PyMem_Free(array_entropy_size)

        wilson_conf = None
        wilson_cache = self.wilson_cache
        min_tracks = self.min_tracks
        for loc_idx, (locations, count) in enumerate(counters.items()):
            if count["nb_tracks"]<min_tracks:
                continue
            
            diff_rtt_values = count["diffrtt"]
            nbsamples = diff_rtt_values.shape[0]
            # these 2 steps are precomuped in parallel before this loop
            # inplace_sort(diff_rtt_values, nbsamples)
            # tmp_ent = np.array(list(count["nb_tracks_per_asn"].values()),dtype=np.int64,copy=False)
            # entropy =  normalized_entropy(tmp_ent)

            entropy = entropy_values[loc_idx]

            # Compute the wilson score
            if nbsamples in wilson_cache:
                wilson_conf = wilson_cache[nbsamples]
            else:
                wilson_conf = sm.stats.proportion_confint(
                        nbsamples/2, 
                        nbsamples, 
                        self.significance_level, "wilson")
                wilson_conf = np.array(wilson_conf)*nbsamples
                wilson_cache[nbsamples] = wilson_conf 

            results[locations] = {
                    "conf_low": count["diffrtt"][int(wilson_conf[0])],
                    "conf_high": count["diffrtt"][int(wilson_conf[1])],
                    "median": count["diffrtt"][int(nbsamples/2)],
                    "nb_tracks": count["nb_tracks"],
                    "nb_probes": len(count["unique_probes"]),
                    "entropy": entropy,
                    "hop": np.median(count["hop"])
                    }

        PyMem_Free(entropy_values)

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


