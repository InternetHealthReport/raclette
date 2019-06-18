import json
import logging
from kafka import KafkaConsumer


class Reader():
    """Consumes traceroute data from Kafka"""

    def __init__(self, start, end, timetrack_converter, 
                msm_ids=[5001, 5004, 5005], probe_ids=[1, 2, 3, 4, 5, 6, 7, 8], 
                chunk_size=900, config=None):

        self.msm_ids = msm_ids
        self.probe_ids = probe_ids
        self.start = start
        self.end = end
        self.chunk_size = chunk_size
        self.params = []
        self.timetrack_converter = timetrack_converter
        self.consumer = None
        self.config = config

    def __enter__(self):
        self.consumer = KafkaConsumer(
                bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m),
                group_id='raclette_traceroute_reader',
                consumer_timeout_ms=10000)

        self.consumer.subscribe(self.config.get('io', 'topic'))
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def read(self):
        logging.info("Entering Infinite For")
        for message in self.consumer:
            traceroute = message.value
            yield self.timetrack_converter.traceroute2timetrack(traceroute)
        self.consumer.close()
        logging.info("should be closed")

    def close(self):
        return False
