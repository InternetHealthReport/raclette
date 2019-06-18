import apsw
import multiprocessing
import logging
import json
from kafka import KafkaProducer
import traceback


class Saver(multiprocessing.Process):
    """Dumps data to a Kafka topic. """

    def __init__(self, filename, saver_queue):
        multiprocessing.Process.__init__(self)
        logging.warn("Init saver")
        self.filename = filename
        self.conn = apsw.Connection(filename)
        self.cursor = self.conn.cursor()
        self.saver_queue = saver_queue
        self.expid = None
        self.prevts = -1
        logging.warn("End init saver")

    def run(self):

        logging.info("Started saver")
        self.createdb()
        self.producer = KafkaProducer(bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
                                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        main_running = True

        while main_running or not self.saver_queue.empty():
            elem = self.saver_queue.get()
            if isinstance(elem, str):
                if elem.endswith(";"):
                    self.cursor.execute(elem)
                elif elem == "MAIN_FINISHED":
                    main_running = False
            else:
                self.save(elem)
            # self.saver_queue.task_done()

    def createdb(self):
        logging.info("Creating databases")
        # Table storing experiements parameters
        self.cursor.execute("CREATE TABLE IF NOT EXISTS experiment \
                (id integer primary key, date text, cmd text, args text)")

        # Table storing aggregated differential RTTs
        self.cursor.execute("CREATE TABLE IF NOT EXISTS diffrtt \
                (ts integer, startpoint text, endpoint text, median real, \
                minimum real, nbsamples integer, nbtracks integer, \
                nbprobes integer, entropy real, hop integer, nbrealrtts integer,\
                expid integer, \
                foreign key(expid) references experiment(id))")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts ON diffrtt (ts)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_startpoint ON diffrtt (startpoint)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_endpoint ON diffrtt (endpoint)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_expid ON diffrtt (expid)")

        # Table storing anomalous delay changes
        self.cursor.execute("CREATE TABLE IF NOT EXISTS anomaly \
                (ts integer, startpoint text, endpoint text, anomaly json, \
                reliability real, expid integer, \
                foreign key(expid) references experiment(id))")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts ON anomaly (ts)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_startpoint ON anomaly (startpoint)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_endpoint ON anomaly (endpoint)")


    def save(self, elem):

        t, data = elem
        if t == "experiment":
            self.cursor.execute("INSERT INTO experiment(date, cmd, args) \
                    VALUES (?, ?, ?)", (str(data[0]), data[1], data[2]))
            self.expid = self.conn.last_insert_rowid()
            if self.expid != 1:
                logging.warning("Database exists: \
                results will be stored with experiment ID (expid) = %s" % self.expid)

        if not self.expid:
            logging.error("No experiment inserted for this data")
            return

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

            self.cursor.execute("INSERT INTO diffrtt \
                    (ts, startpoint, endpoint, median, minimum, nbsamples, \
                    nbtracks, nbprobes, entropy, hop, nbrealrtts, expid) \
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )",
                    (ts, startpoint, endpoint, median, minimum, nb_samples,
                        nb_tracks, nb_probes, entropy, hop, nbrealrtts,
                        self.expid) )
                        
            self.producer.send('SQLTEST4', value = serialized_data)
            if self.prevts != ts:
                self.prevts = ts
                logging.info("start recording diff. RTTs (ts={})".format(ts))

        #elif t == "anomaly":
            #self.cursor.execute("INSERT INTO anomaly \
                    #(ts, startpoint, endpoint, anomaly, reliability, expid) \
                    #VALUES (?, ?, ?, ?, ?, ?)", data+[self.expid])
