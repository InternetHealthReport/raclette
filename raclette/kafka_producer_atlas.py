import json
import calendar
import json
import msgpack
import logging
import numpy as np
import requests
import sys
import configparser
import argparse
import tools
from datetime import timedelta
from requests_futures.sessions import FuturesSession
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

#IMPORT KAFKA PRODUCER
from kafka import KafkaProducer #, KafkaAdminClient, NewTopic
from kafka.admin import KafkaAdminClient, NewTopic


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
    max_workers=8,
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


def worker_task(resp, *args, **kwargs):
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
        queries.append( [session.get(url=url.format(msm), params=req_param,
                hooks={ 'response': worker_task, }
            ), [url.format(msm), req_param]] )

    for tmp in queries:
        query = tmp[0]
        try:
            resp = query.result()
            yield (resp.ok, resp.data)
        except requests.exceptions.ChunkedEncodingError:
            logging.error("Could not retrieve traceroutes for {}".format(query))
            logging.error(tmp[1])

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
            value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
            key_serializer=lambda k: k.to_bytes(8, byteorder='big'),
            compression_type='snappy', linger_ms=1000) 

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
    chunk_size = int(config.get('io', 'chunk_size'))

    topic = config.get("io", "kafka_topic")
    admin_client = KafkaAdminClient(bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'], client_id='atlas_producer_admin')

    try:
        topic_list = [NewTopic(name=topic, num_partitions=3, replication_factor=0)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except:
        pass

    current_time = atlas_start
    end_time = atlas_stop
    while current_time < end_time:
        logging.warning("downloading: "+str(current_time))
        params = { "msm_id": atlas_msm_ids, "start": current_time, "stop": current_time  + timedelta(seconds=chunk_size), "probe_ids": atlas_probe_ids }
        
        for is_success, data in cousteau_on_steroid(params):
            if is_success:
                for traceroute in data:
                    try:
                        producer.send(topic, key=traceroute['msm_id'], 
                                value=traceroute, timestamp_ms = traceroute.get('timestamp')*1000)
                    except KeyError:
                        logging.warning('Ignoring one traceroute: {}'.format(traceroute))
            else:
                print("Error could not load the data")

            producer.flush()

        current_time = current_time + timedelta(seconds = chunk_size)
