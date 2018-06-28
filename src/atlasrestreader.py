import json
import datetime
import logging
import itertools 
from multiprocessing import Pool
from ripe.atlas.cousteau import AtlasResultsRequest

TT_CONVERTER = None

def get_results(kwargs, retry=3):

    global TT_CONVERTER

    logging.debug("Requesting results for {}".format(kwargs))
    is_success, results = AtlasResultsRequest(**kwargs).create()

    if is_success:
        return map(TT_CONVERTER.traceroute2timetrack,results)
    else:
        logging.warning("Atlas request failed for {}".format(kwargs))
        if retry > 0:
            return get_results(kwargs, retry-1)
        else:
            logging.error("All retries failed for {}".format(kwargs))
            return None


class AtlasRestReader():

    def __init__(self, start, end, timetrack_converter, msm_ids=[5001,5004,5005], 
            probe_ids=[1,2,3,4,5,6,7,8], chunk_size=900):

        global TT_CONVERTER
        TT_CONVERTER = timetrack_converter 

        self.pool = Pool(processes=6) 
        self.msm_ids = msm_ids
        self.probe_ids = probe_ids
        self.start = start
        self.end = end
        self.chunk_size = chunk_size

    def __enter__(self):
        
        params = []
        window_start = self.start
        while window_start+datetime.timedelta(seconds=self.chunk_size) <= self.end:
            for msm_id in self.msm_ids:
                kwargs = {
                    "msm_id": msm_id,
                    "start": window_start,
                    "stop": window_start+datetime.timedelta(seconds=self.chunk_size),
                    "probe_ids": self.probe_ids,
                        }
                params.append(kwargs)
            window_start += datetime.timedelta(seconds=self.chunk_size)
            
        return itertools.chain.from_iterable(self.pool.imap(get_results, params))

    def __exit__(self, type, value, traceback): 
        if self.pool is not None: 
            self.pool.terminate()
        return False


if __name__ == "__main__":
    with AtlasRestReader(datetime.datetime(2018,6,1,0,0), datetime.datetime(2018,6,2,0,0)) as arr:
            for tr in arr:
                print tr
