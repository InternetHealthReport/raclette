import multiprocessing
import logging

class DelayChangeDetector(multiprocessing.Process):

    def __init__(self, aggregator_pipe, saver_queue):
        multiprocessing.Process.__init__(self)

        self.aggregator_pipe = aggregator_pipe
        self.saver_queue = saver_queue

    def run(self):
        logging.warn("Started detector")
        while True:
            aggregated_diff_rtt = self.aggregator_pipe.recv()

            # Do something
