import apsw
import multiprocessing
import logging

class SQLiteSaver(multiprocessing.Process):
    """Dumps data to a SQLite database. """

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
        
        while True:
            elem = self.saver_queue.get()
            if isinstance(elem, str) and elem.endswith(";"):
                self.cursor.execute(elem)
            else:
                self.save(elem)
            self.saver_queue.task_done()

    def createdb(self):
        logging.info("Creating databases")
        # Table storing experiements parameters
        self.cursor.execute("CREATE TABLE IF NOT EXISTS experiment (id integer primary key, date text, cmd text, args text)")

        # Table storing aggregated differential RTTs 
        self.cursor.execute("CREATE TABLE IF NOT EXISTS diffrtt (ts integer, startpoint text, endpoint text, median real, confhigh real, conflow real, nbtracks integer, nbprobes integer, entropy real, hop integer, expid integer, foreign key(expid) references experiment(id))")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts ON diffrtt (ts)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_startpoint ON diffrtt (startpoint)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_endpoint ON diffrtt (endpoint)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_expid ON diffrtt (expid)")

        # Table storing anomalous delay changes
        self.cursor.execute("CREATE TABLE IF NOT EXISTS delayanomaly (ts integer, startpoint text, endpoint text, median real, expid integer, foreign key(expid) references experiment(id))")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts ON delayanomaly (ts)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_startpoint ON delayanomaly (startpoint)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_endpoint ON delayanomaly (endpoint)")

        # Table storing normal references for delay changes
        self.cursor.execute("CREATE TABLE IF NOT EXISTS delayreference (ts integer, startpoint text, endpoint text, median real, confhigh real, conflow real, expid integer, foreign key(expid) references experiment(id))")
        self.cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ts ON delayreference (ts)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_startpoint ON delayreference (startpoint)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_endpoint ON delayreference (endpoint)")

    def save(self, elem):
        t, data = elem

        if t == "experiment":
            self.cursor.execute("INSERT INTO experiment(date, cmd, args) VALUES (?, ?, ?)", (str(data[0]), data[1], data[2]))
            self.expid = self.conn.last_insert_rowid()
            if self.expid != 1:
                logging.warning("Database exists: results will be stored with experiment ID (expid) = %s" % self.expid)

        if not self.expid:
            logging.error("No experiment inserted for this data")
            return

        elif t == "diffrtt":
            ts, startpoint, endpoint, median, high, low, nb_tracks, nb_probes, entropy, hop = data

            if self.prevts != ts:
                self.prevts = ts
                logging.info("start recording diff. RTTs (ts={})".format(ts))
            
            self.cursor.execute("INSERT INTO diffrtt(ts, startpoint, endpoint, median, confhigh, conflow, nbtracks, nbprobes, entropy, hop, expid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (ts, startpoint, endpoint, median, high, low, nb_tracks, nb_probes, entropy, hop, self.expid) )
                    # zip([ts]*len(hege), [scope]*len(hege), hege.keys(), hege.values(), [self.expid]*len(hege)) )

        elif t == "delayanomaly":
            self.cursor.execute("INSERT INTO delayanomaly(ts, startpoint, endpoint, median, expid) VALUES (?, ?, ?, ?, ?)", data+[self.expid])

        elif t == "delayreference":
            self.cursor.execute("INSERT INTO delayreference(ts, startpoint, endpoint, median, confhigh, conflow, expid) VALUES (?, ?, ?, ?, ?, ?, ?)", data+[self.expid] )
