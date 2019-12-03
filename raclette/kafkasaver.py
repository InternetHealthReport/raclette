import multiprocessing
import logging
import msgpack
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


class Saver(multiprocessing.Process):
    """Dumps data to a Kafka topic. """

    def __init__(self, filename, saver_queue):
        multiprocessing.Process.__init__(self)
        logging.warn("Init saver")
        self.saver_queue = saver_queue
        self.expid = None
        self.prevts = -1
        self.topic = 'ihr_raclette_diffrtt'
        logging.warn("End init saver")

    def run(self):

        logging.info("Started saver")

        admin_client = AdminClient({'bootstrap.servers':'kafka1:9092, kafka2:9092, kafka3:9092'})
        topic_list = [NewTopic(self.topic, num_partitions=1, replication_factor=2)]
        admin_client.create_topics(topic_list)
        created_topic = admin_client.create_topics(topic_list)
        for topic, f in created_topic.items():
            try:
                f.result()  # The result itself is None
                logging.warning("Topic {} created".format(topic))
            except Exception as e:
                logging.warning("Failed to create topic {}: {}".format(topic, e))

        # Create producer
        self.producer = Producer({'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
            'default.topic.config': {'compression.codec': 'snappy'}}) 

        main_running = True
        while main_running or not self.saver_queue.empty():
            elem = self.saver_queue.get()
            if isinstance(elem, str):
                if elem == "MAIN_FINISHED":
                    main_running = False
            else:
                self.save(elem)

        self.producer.flush()


    def save(self, elem):

        t, data = elem

        if t == "diffrtt":
            (ts, startpoint, endpoint, median, minimum, nb_samples, nb_tracks,
                    nb_probes, entropy, hop, nb_real_rtts) = data

            msg = {
                'ts' : ts,
                'startpoint' : startpoint,
                'endpoint' : endpoint,
                'median' : median,
                'minimum' : minimum,
                'nb_samples' : nb_samples,
                'nb_tracks' : nb_tracks,
                'nb_probes' : nb_probes,
                'entropy' : entropy,
                'hop' : hop,
                'nb_real_rtts' : nb_real_rtts
                }


            self.producer.produce(
                    self.topic, 
                    msgpack.packb(msg, use_bin_type=True), 
                    timestamp = int(ts)*1000
                    )

            # Trigger any available delivery report callbacks from previous produce() calls
            self.producer.poll(0)

            if self.prevts != ts:
                self.prevts = ts
                logging.info("start recording diff. RTTs (ts={})".format(ts))

        #elif t == "anomaly":
            #self.cursor.execute("INSERT INTO anomaly \
                    #(ts, startpoint, endpoint, anomaly, reliability, expid) \
                    #VALUES (?, ?, ?, ?, ?, ?)", data+[self.expid])
