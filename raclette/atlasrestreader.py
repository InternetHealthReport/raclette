import json
import datetime
import logging
import itertools 
import threading 
from concurrent.futures import ThreadPoolExecutor
from ripe.atlas.cousteau import AtlasResultsRequest

# Semaphore used to control the number of buffered results from the pool
semaphore = threading.Semaphore(32) 

def get_results(param, retry=3):
    traceroute2timetrack, kwargs = param

    if retry==3 :
        semaphore.acquire()

    # logging.info("Requesting results for {}".format(kwargs))
    is_success, results = AtlasResultsRequest(**kwargs).create()
    # logging.info("Server replied {}".format(kwargs))

    if is_success:
        return map(traceroute2timetrack,results)
    else:
        logging.warning("Atlas request failed for {}".format(kwargs))
        if retry > 0:
            return get_results(param, retry-1)
        else:
            logging.error("All retries failed for {}".format(kwargs))
            return


class AtlasRestReader():

    def __init__(self, start, end, timetrack_converter, msm_ids=[5001,5004,5005], 
            probe_ids=[1,2,3,4,5,6,7,8], chunk_size=900):


        self.pool = None
        # self.semaphore = None
        self.msm_ids = msm_ids
        self.probe_ids = probe_ids
        self.start = start
        self.end = end
        self.chunk_size = chunk_size
        self.params = []
        self.timetrack_converter = timetrack_converter 

    def __enter__(self):    
        self.params = []
        logging.warn("creating the pool")
        self.pool = ThreadPoolExecutor(max_workers=8) 

        window_start = self.start
        while window_start+datetime.timedelta(seconds=self.chunk_size) <= self.end:
            for msm_id in self.msm_ids:
                kwargs = {
                    "msm_id": msm_id,
                    "start": window_start,
                    "stop": window_start+datetime.timedelta(seconds=self.chunk_size),
                    "probe_ids": self.probe_ids,
                        }
                self.params.append(
                        [ self.timetrack_converter.traceroute2timetrack,
                        kwargs])
            window_start += datetime.timedelta(seconds=self.chunk_size)
        logging.warn("pool ready")

        return self
    
    def __exit__(self, type, value, traceback):
        self.close()


    def read(self):
        # m = map(get_results, self.params)
        m = self.pool.map(get_results, self.params)

        for tracks in m: 
            yield from tracks
            
            semaphore.release()


    def close(self): 
        if self.pool is not None: 
            self.pool.shutdown()
        return False


if __name__ == "__main__":
    with AtlasRestReader(datetime.datetime(2018,6,1,0,0), datetime.datetime(2018,6,2,0,0)) as arr:
            for tr in arr:
                print(tr)

