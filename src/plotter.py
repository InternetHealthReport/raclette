import os
from matplotlib import pylab as plt
import itertools
import sqlite3
import numpy as np
from collections import defaultdict
from datetime import datetime
from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes, mark_inset
import matplotlib.dates as mdates

def ecdf(a, ax=None, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    if ax is None:
        plt.plot( sorted, yvals, **kwargs )
    else:
        ax.plot( sorted, yvals, **kwargs )


def eccdf(a, ax=None, **kwargs):
    sorted=np.sort( a )
    yvals=np.arange(len(sorted))/float(len(sorted))
    if ax is None:
        plt.plot( sorted, 1-yvals, **kwargs )
    else:
        ax.plot( sorted, 1-yvals, **kwargs )

    return {k:v for k,v in zip(sorted, 1-yvals)}



class Plotter(object):

    """Read results from the sqlite database and plot interesting stuff. """

    def __init__(self, db="results/ashash_results.sql"):
        if not isinstance(db,list):
            db = [db]

        self.dbfiles = db
        self.cursor = []
        for d in db:
            if os.path.getsize(d) > 100000:
                conn = sqlite3.Connection(d) 
                self.cursor.append(conn.cursor())


    def diffrtt_distribution(self, startpoint, endpoint, filename="{}_diffrtt_distribution.pdf"):
        """Plot the distribution of differential RTT values for the given start
        and end points. """
        
        fig = plt.figure()

        diffrtt = defaultdict(list)
        for cursor_id, cursor in enumerate(self.cursor):
            data=cursor.execute("SELECT median, confhigh, conflow  FROM diffrtt where expid=%s and startpoint=%s and endpoint=%s order by ts" % (expid, startpoint, endpoint))
            
            for ts, dr_med, dr_high, dr_low in data:
                diffrtt["median"].append(dr_med)
                diffrtt["high"].append(dr_high)
                diffrtt["low"].append(dr_low)

        yval = ecdf(diffrtt["median"], label="median")
        yval = ecdf(diffrtt["high"], label="high")
        yval = ecdf(diffrtt["low"], label="low")

        plt.xlabel("Differential RTT (ms)")
        plt.ylabel("CDF")
        # plt.xscale("log")
        plt.ylim([0, 1.1])

        fig.tight_layout()
        fig.savefig(filename)



    def diffrtt_time(self, location, filename="{}_diffrtt_time.pdf", expid=1):

        filename = filename.format(location)
        diffrtt = defaultdict(lambda: defaultdict(list))
        marker = itertools.cycle(('^', '.', 'x', '+', 'v','*'))
        color = itertools.cycle(('C1', 'C0', 'C2', 'C4', 'C3'))
        fig = plt.figure()
        ax = plt.subplot()
        for cursor_id, cursor in enumerate(self.cursor):
            data=cursor.execute("SELECT ts, startpoint, endpoint, median, confhigh, conflow  FROM diffrtt where expid=%s and (startpoint=%s or endpoint=%s) order by ts" % (expid, location, location))
            
            for ts, loc0, loc1, dr_med, dr_high, dr_low in data:
                xval = datetime.utcfromtimestamp(ts)
                diffrtt[(loc0,loc1)]["ts"].append(xval)
                diffrtt[(loc0,loc1)]["median"].append(dr_med)
                diffrtt[(loc0,loc1)]["high"].append(dr_high)
                diffrtt[(loc0,loc1)]["low"].append(dr_low)

        for locations, data in hege.iteritems():
            plt.plot(data["ts"], data["median"], label=str(asn))
            # plt.plot(data["ts"], data["median"], marker=marker.next(), label=str(asn))

        plt.ylabel("Differential RTT (ms)")
        plt.title(location)
        plt.legend(loc='upper center', ncol=4, bbox_to_anchor=(0.5, 1.2), fontsize=8 )
        fig.autofmt_xdate() 
        plt.tight_layout()
        plt.savefig(filename)



