import multiprocessing
import logging
import msgpack
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


class Saver(multiprocessing.Process):
    """Dumps data to a Kafka topic. """

    def __init__(self, filename, saver_queue):
        multiprocessing.Process.__init__(self)
        logging.warn("Init saver")
        self.saver_queue = saver_queue
        self.expid = None
        self.prevts = -1
        self.topic = 'ihr_raclette_results'
        logging.warn("End init saver")

    def run(self):

        logging.info("Started saver")
        self.producer = KafkaProducer(
                bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
                value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
                )

        try:
            admin_client = KafkaAdminClient(bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'], client_id='atlas_producer_admin')
            topic_list = [NewTopic(name=self.topic, num_partitions=1, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except TopicAlreadyExistsError:
            pass


        main_running = True

        while main_running or not self.saver_queue.empty():
            elem = self.saver_queue.get()
            if isinstance(elem, str):
                pass
            else:
                self.save(elem)

        self.producer.close()


    def save(self, elem):

        t, data = elem

        if t == "diffrtt":
            (ts, startpoint, endpoint, median, minimum, nb_samples, nb_tracks,
                    nb_probes, entropy, hop, nbrealrtts) = data

            serialized_data = {
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
                'nbrealrtts' : nbrealrtts
                }

            self.producer.send(self.topic, value = serialized_data,
                    timestamp_ms=int(ts))

            if self.prevts != ts:
                self.prevts = ts
                logging.info("start recording diff. RTTs (ts={})".format(ts))

        #elif t == "anomaly":
            #self.cursor.execute("INSERT INTO anomaly \
                    #(ts, startpoint, endpoint, anomaly, reliability, expid) \
                    #VALUES (?, ?, ?, ?, ?, ?)", data+[self.expid])
