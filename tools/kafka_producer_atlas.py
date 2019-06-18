import json
import datetime
import calendar
import json
import logging
import time
import requests
import sys
import configparser
import argparse
import tools
from datetime import timedelta
from requests_futures.sessions import FuturesSession
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor


#IMPORT KAFKA PRODUCER
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['Kafka1:9092', 'Kafka2:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

#end import
logging.basicConfig()#should be removable soon

parser = argparse.ArgumentParser()
parser.add_argument("-C","--config_file", help="Get all parameters from the specified config file", type=str, default="conf/raclette.conf")
args = parser.parse_args()

# Read the config file
config = configparser.ConfigParser()
config.read(args.config_file)

atlas_msm_ids =  [int(x) for x in config.get("io", "msm_ids").split(",") if x]
atlas_probe_ids =  [int(x) for x in config.get("io", "probe_ids").split(",") if x]

atlas_start =  tools.valid_date(config.get("io", "start"))
atlas_stop =  tools.valid_date(config.get("io", "stop"))



topic = config.get("io", "kafka_topic")

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
    max_workers=4,
):
    """ Retry if there is a problem"""
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
    try:
        resp.data = resp.json()
    except json.decoder.JSONDecodeError:
        logging.error("Error while reading Atlas json data.\n")
        resp.data = {}


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
        try:
            resp = query.result()
            yield (resp.ok, resp.data)
        except requests.exceptions.ChunkedEncodingError:
            logging.error("Could not retrieve traceroutes for {}".format(query))


#if (len(sys.argv) == 1):
#    print("One argument. Every Ten Minutes")
#    #CollectionTime = datetime.datetime.utcnow()
#    CollectionTime = datetime.datetime.utcnow()
##    while True:
#        params = { "msm_id": [1748022, 1748024, 11645084, 11645087, 2244316, 2244318, 2244318, 2435592, 2435594, 1796567, 1796569], "start": (CollectionTime - timedelta(minutes=20)), "stop": (CollectionTime - timedelta(minutes=10)), "probe_ids": [] }
#        for is_success, data in cousteau_on_steroid(params):
#            print("downloading")
#
#            if is_success:
#                for traceroute in data:
#                    producer.send('TURBO_TIME_TEST4', value=traceroute, timestamp_ms = traceroute.get('timestamp'))
##            else:
#                print("Error could not load the data")
#        CollectionTime = CollectionTime + timedelta(minutes = 10)
#        time.sleep(600)
if (len(sys.argv) >= 3):
    print("3 Arguments.  Using Start and End Time")
    CollectionTime = atlas_start
    StopTime = atlas_stop
    #CollectionTime = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d-%H:%M") Input times in command line version
    #StopTime = datetime.datetime.strptime(sys.argv[2],"%Y-%m-%d-%H:%M")
    while CollectionTime < StopTime:
        #params = { "msm_id": [1748022, 1748024, 11645084, 11645087, 2244316, 2244318, 2244318, 2435592, 2435594, 1796567, 1796569], "start": (CollectionTime - timedelta(minutes=20)), "stop": (CollectionTime - timedelta(minutes=10)), "probe_ids": [] } old msm id version
        params = { "msm_id": atlas_msm_ids, "start": (CollectionTime - timedelta(minutes=20)), "stop": (CollectionTime - timedelta(minutes=10)), "probe_ids": atlas_probe_ids }
        for is_success, data in cousteau_on_steroid(params):

            print("downloading")
            if is_success:
                for traceroute in data:
                    producer.send(topic, value=traceroute, timestamp_ms = traceroute.get('timestamp'))
                    producer.flush()
            else:
                print("Error could not load the data")

        CollectionTime = CollectionTime + timedelta(minutes = 10)
else:
    print("Improper argument use.  Need either none or exactly 2, first start time, then end time")
