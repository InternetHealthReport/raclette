import msgpack
import logging
from confluent_kafka import Consumer, KafkaError




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
        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_raclette_traceroute_reader0',
            'auto.offset.reset': 'earliest',
            'session.timeout.ms'=1800*1000,
        })

        self.consumer.subscribe([self.config.get('io', 'kafka_topic')])

        return self

    def __exit__(self, type, value, traceback):
        pass

    def read(self):
        logging.info("Start consuming data")
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            print('Received message: {}'.format(msg.value().decode('utf-8')))
            traceroute = msgpack.unpackb(msg.value(), raw=False)

            #needed? the consumer is not filtering by msm or probe id
            # if (self.probe_ids is not None and traceroute['prb_id'] not in self.probe_ids) or \
                    # (self.msm_ids is not None and traceroute['msm_id'] not in self.msm_ids):
                        # pass

            yield self.timetrack_converter.traceroute2timetrack(traceroute)

        self.consumer.close()
        logging.info("closed the consumer")
