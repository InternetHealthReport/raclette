import json
import datetime
import calendar
import logging
import itertools 
import threading 
from requests_futures.sessions import FuturesSession
from concurrent.futures import ThreadPoolExecutor
# from ripe.atlas.cousteau import AtlasResultsRequest
from progress.bar import Bar
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Semaphore used to control the number of buffered results from the pool
semaphore = threading.Semaphore(4) 

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
    max_workers=4,
):
    session = session or FuturesSession(max_workers=max_workers)
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def worker_task(sess, resp):
    """Process json in background"""
    resp.data = resp.json()

def cousteau_on_steroid(params, retry=3):
    url = "https://atlas.ripe.net/api/v2/measurements/{0}/results"
    req_param = {
            "start": int(calendar.timegm(params["start"].timetuple())),
            "stop": int(calendar.timegm(params["stop"].timetuple())),
            }
    
    if params["probe_ids"]:
        req_param["probe_ids"] = params["probe_ids"]

    queries = []

    session = requests_retry_session()
    for msm in params["msm_id"]:
        queries.append( session.get(url=url.format(msm), params=req_param,
                        background_callback=worker_task
            ) )

    for query in queries:
        resp = query.result()
        yield (resp.ok, resp.data)


def get_results(param, retry=3):
    traceroute2timetrack, kwargs = param

    if retry==3 :
        semaphore.acquire()

    # logging.info("Requesting results for {}".format(kwargs))
    # success_list, results_list = AtlasResultsRequest(**kwargs).create()
    results_list = cousteau_on_steroid(kwargs)
    # logging.info("Server replied {}".format(kwargs))

    for is_success, results in results_list:
        if is_success:
            yield map(traceroute2timetrack,results)
        else:
            logging.error("All retries failed for {}".format(kwargs))
            # logging.warning("Atlas request failed for {}".format(kwargs))
            # if retry > 0:
                # return get_results(param, retry-1)
            # else:
                # logging.error("All retries failed for {}".format(kwargs))
                # return


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
        self.bar = None

    def __enter__(self):    
        self.params = []
        logging.warn("creating the pool")
        self.pool = ThreadPoolExecutor(max_workers=4) 

        window_start = self.start
        while window_start+datetime.timedelta(seconds=self.chunk_size) <= self.end:
            # for msm_id in self.msm_ids:
            kwargs = {
                "msm_id": self.msm_ids,
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
        self.bar = Bar("Processing", max=len(self.params), suffix='%(percent)d%%')
        # m = map(get_results, self.params)
        msm_results = self.pool.map(get_results, self.params)

        for res in msm_results:
            for tracks in res: 
                yield from tracks
                
            semaphore.release()
            self.bar.next()


    def close(self): 
        if self.pool is not None: 
            self.pool.shutdown()
        self.bar.finish()
        return False


if __name__ == "__main__":
    with AtlasRestReader(datetime.datetime(2018,6,1,0,0), datetime.datetime(2018,6,2,0,0)) as arr:
            for tr in arr:
                print(tr)

