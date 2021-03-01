import msgpack
import logging
import calendar
from confluent_kafka import Consumer, TopicPartition, KafkaError
import confluent_kafka


class Reader():
    """Consumes traceroute data from Kafka"""

    def __init__(self, start, end, timetrack_converter, 
                msm_ids=[5001, 5004, 5005], probe_ids=[1, 2, 3, 4, 5, 6, 7, 8], 
                chunk_size=900, config=None):

        self.msm_ids = msm_ids
        self.probe_ids = probe_ids
        self.start = int(calendar.timegm(start.timetuple()))*1000
        self.end = int(calendar.timegm(end.timetuple()))*1000
        self.chunk_size = chunk_size
        self.params = []
        self.timetrack_converter = timetrack_converter
        self.consumer = None
        self.config = config
        self.topic = self.config.get('io', 'kafka_topic')
        self.partition_total = 0
        self.partition_paused = 0

    def __enter__(self):
        """Setup kafka consumer"""

        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_raclette_'+str(self.start),
            'auto.offset.reset': 'earliest',
            'max.poll.interval.ms': 1800*1000,
        })

        # Set offsets according to start time
        topic_info = self.consumer.list_topics(self.topic)
        partitions = [TopicPartition(self.topic, partition_id, self.start) 
                for partition_id in  topic_info.topics[self.topic].partitions.keys()]

        offsets = self.consumer.offsets_for_times(partitions)

        # remove empty partitions
        offsets = [part for part in offsets if part.offset > 0]
        self.partition_total = len(offsets)
        self.partition_paused = 0
        self.consumer.assign(offsets)

        return self

    def __exit__(self, type, value, traceback):
        self.consumer.close()
        logging.info("closed the consumer")

    def read(self):

        logging.info("Start consuming data")
        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            # Filter with start and end times
            ts = msg.timestamp()
            if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME and ts[1] < self.start:
                continue

            if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME and ts[1] >= self.end:
                self.consumer.pause([TopicPartition(self.topic, msg.partition())])
                self.partition_paused += 1
                if self.partition_paused < self.partition_total:
                    continue
                else:
                    break

            traceroute = msgpack.unpackb(msg.value(), raw=False)

            #needed? the consumer is not filtering the msm or probe ids
            # if (self.probe_ids is not None and traceroute['prb_id'] not in self.probe_ids) or \
                    # (self.msm_ids is not None and traceroute['msm_id'] not in self.msm_ids):
                        # pass

            yield self.timetrack_converter.traceroute2timetrack(traceroute)

