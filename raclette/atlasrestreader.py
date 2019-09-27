import json
import datetime
import calendar
import logging
import itertools 
import threading 
import json
from requests_futures.sessions import FuturesSession
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
# from ripe.atlas.cousteau import AtlasResultsRequest
from progress.bar import Bar

# Semaphore used to control the number of buffered results from the pool
# semaphore = threading.Semaphore(4) 

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

def worker_task(resp, *args, **kwargs):
    """Process json in background"""
    try:
        resp.data = resp.json()
    except json.decoder.JSONDecodeError:
        logging.error("Error while reading Atlas json data.\n")
        resp.data = {}


def cousteau_on_steroid(params, retry=3):
    url = "https://atlas.ripe.net/api/v2/measurements/{0}/results"
    queries = []
    probes = [None]

    if params["probe_ids"]:
        probes = [params["probe_ids"][x:x+20] for x in range(0, len(params["probe_ids"]), 20)]

    session = requests_retry_session()
    for msm in params["msm_id"]:
        req_param = {
                "start": int(calendar.timegm(params["start"].timetuple())),
                "stop": int(calendar.timegm(params["stop"].timetuple())),
                }
        
        for pb in probes:
            if pb is not None:
                req_param["probe_ids"] = ",".join([str(i) for i in pb])

            queries.append( session.get(url=url.format(msm), params=req_param,
                            hooks={ 'response': worker_task, }) )

    for query in queries:
        try:
            resp = query.result()
            yield resp
        except requests.exceptions.ChunkedEncodingError:
            logging.error("Could not retrieve traceroutes for {}".format(query))


def get_results(param, retry=3):
    traceroute2timetrack, kwargs = param

    # if retry==3 :
        # semaphore.acquire()

    # logging.info("Requesting results for {}".format(kwargs))
    # success_list, results_list = AtlasResultsRequest(**kwargs).create()
    results_list = cousteau_on_steroid(kwargs)
    # logging.info("Server replied {}".format(kwargs))

    for resp in results_list:
        if resp.ok:
            # logging.info("Received {} traceroutes".format(len(resp.data)))
            yield map(traceroute2timetrack, resp.data)
        else:
            logging.error("All retries failed for {}".format(resp.url))
            # logging.warning("Atlas request failed for {}".format(kwargs))
            # if retry > 0:
                # return get_results(param, retry-1)
            # else:
                # logging.error("All retries failed for {}".format(kwargs))
                # return


class Reader():

    def __init__(self, start, end, timetrack_converter, msm_ids=[5001,5004,5005], 
            probe_ids=[1,2,3,4,5,6,7,8], chunk_size=900, config=None):


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
                
            # semaphore.release()
            self.bar.next()


    def close(self): 
        if self.pool is not None: 
            self.pool.shutdown()
        self.bar.finish()
        return False

