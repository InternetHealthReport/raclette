import apsw
import threading
import logging

class SQLiteSaver(threading.Thread):

    """Dumps variables to a SQLite database. """

    def __init__(self, filename, saverQueue):
        threading.Thread.__init__(self)
       
        self.filename = filename
        self.conn = apsw.Connection(filename)
        self.cursor = self.conn.cursor()
        self.saverQueue = saverQueue
        self.expid = None
        self.prevts = -1


    def run(self):

        self.createdb()
        
        while True:
            elem = self.saverQueue.get()
            if isinstance(elem, str) and elem.endswith(";"):
                self.cursor.execute(elem)
            else:
                self.save(elem)
            self.saverQueue.task_done()


    def createdb(self):
        logging.warn("Creating databases")
        # Table storing experiements parameters
        self.cursor.execute("CREATE TABLE IF NOT EXISTS experiment (id integer primary key, date text, cmd text, args text)")

        # Table storing aggregated differential RTTs 
        self.cursor.execute("CREATE TABLE IF NOT EXISTS diffrtt (ts integer, startpoint text, endpoint text, median real, confhigh real, conflow real, expid integer, foreign key(expid) references experiment(id))")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts ON diffrtt (ts)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_startpoint ON diffrtt (startpoint)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_endpoint ON diffrtt (endpoint)")

        # Table storing anomalous delay changes
        self.cursor.execute("CREATE TABLE IF NOT EXISTS delayanomaly (ts integer, startpoint text, endpoint text, median real, foreign key(expid) references experiment(id))")
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

        if self.expid is None:
            logging.error("No experiment inserted for this data")
            return

        elif t == "diffrtt":
            ts, startpoint, endpoint, median, high, low = data

            if self.prevts != ts:
                self.prevts = ts
                logging.debug("start recording differential RTTs")
            
            self.cursor.execute("INSERT INTO diffrtt(ts, startpoint, endpoint, median, confhigh, conflow, expid) VALUES (?, ?, ?, ?, ?, ?, ?)", (ts, startpoint, endpoint, median, high, low, self.expid) )
                    # zip([ts]*len(hege), [scope]*len(hege), hege.keys(), hege.values(), [self.expid]*len(hege)) )

        elif t == "delayanomaly":
            self.cursor.execute("INSERT INTO delayanomaly(ts, startpoint, endpoint, median, expid) VALUES (?, ?, ?, ?, ?)", data+[self.expid])

        elif t == "delayreference":
            self.cursor.execute("INSERT INTO delayreference(ts, startpoint, endpoint, median, confhigh, conflow, expid) VALUES (?, ?, ?, ?, ?, ?, ?)", data+[self.expid] )
