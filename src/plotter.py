import os
import logging
from matplotlib import pylab as plt
import itertools
import sqlite3
import numpy as np
from collections import defaultdict
from datetime import datetime
import matplotlib.dates as mdates
import pandas as pd

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
        self.conn = []
        for d in db:
            if os.path.getsize(d) > 100000:
                conn = sqlite3.Connection(d) 
                self.conn.append(conn)


    def diffrtt_distribution(self, startpoint, endpoint, filename="{}_diffrtt_distribution.pdf", expid=1):
        """Plot the distribution of differential RTT values for the given start
        and end points. """
        
        all_df = []
        for conn in self.conn:
            all_df.append(pd.read_sql_query("SELECT ts, median, confhigh, conflow, nbsamples, nbprobes  FROM diffrtt where expid=? and startpoint=? and endpoint=?", conn, "ts", params=(expid, startpoint, endpoint), parse_dates=["ts"]) )
            
        diffrtt = pd.concat(all_df)

        # Plot the distribution of the median differential RTT
        fig = plt.figure()

        yval = ecdf(diffrtt["median"], label="median")
        yval = ecdf(diffrtt["confhigh"], label="high")
        yval = ecdf(diffrtt["conflow"], label="low")

        plt.xlabel("Differential RTT (ms)")
        plt.ylabel("CDF")
        plt.legend(loc='best', fontsize=8 )
        # plt.xscale("log")
        plt.ylim([0, 1.1])

        fig.tight_layout()
        fig.savefig(filename)


    def diffrtt_time(self, location, filename="{}_diffrtt_time_{}.pdf", expid=1, tz="UTC"):

        filename = filename.format(location, "raw")
        all_df = []

        for conn in self.conn:
            all_df.append(pd.read_sql_query("SELECT ts, startpoint, endpoint, median, confhigh, conflow, nbsamples FROM diffrtt where expid=? and (startpoint=? or endpoint=?) order by ts" , conn, "ts", params=(expid, location, location), parse_dates=["ts"]) )
            
        diffrtt = pd.concat(all_df)
        diffrtt.index = diffrtt.index.tz_localize("UTC")
        diffrtt_grp = diffrtt.groupby(["startpoint","endpoint"])

        nbsamples_avg = diffrtt["nbsamples"].mean()

        fig = plt.figure()
        for locations, data in diffrtt_grp:
            # Ignore locations with a small number of samples
            if data["nbsamples"].mean()>nbsamples_avg:
                plt.plot(data["median"], label=str(locations))

        plt.gca().xaxis_date(tz)
        plt.ylabel("Differential RTT (ms)")
        plt.xlabel("Time ({})".format(tz))
        plt.title(location)
        plt.legend(loc='best', ncol=4, fontsize=8 )
        fig.autofmt_xdate() 
        # plt.tight_layout()
        plt.savefig(filename)


    def profile_endpoint(self, endpoint, filename="{}_profile_{}.pdf", expid=1, tz="UTC", ylim=[2,12]):


        all_df = []
        for conn in self.conn:
            all_df.append(pd.read_sql_query("SELECT ts, startpoint, endpoint, median, confhigh, conflow, nbsamples FROM diffrtt where expid=? and endpoint=? order by ts" , conn, "ts", params=(expid, endpoint), parse_dates=["ts"]) )
            
        diffrtt = pd.concat(all_df)
        diffrtt.index = diffrtt.index.tz_localize("UTC")
        if tz != "UTC":
            diffrtt.index = diffrtt.index.tz_convert(tz)

        nbsamples_avg = diffrtt["nbsamples"].mean()
        logging.warn("{} average samples".format(nbsamples_avg))

        # Split weekday and weekend
        weekday = diffrtt[diffrtt.index.weekday<5]
        weekend = diffrtt[diffrtt.index.weekday>4]
        weekday_avg = weekday.groupby(weekday.index.hour).median()
        weekend_avg = weekend.groupby(weekend.index.hour).median()

        fig = plt.figure(figsize=(6,4))
        plt.plot(weekday_avg["median"])
        plt.plot(weekday_avg["confhigh"])
        plt.plot(weekday_avg["conflow"])
        plt.ylabel("Differential RTT (ms)")
        plt.xlabel("Time ({})".format(tz))
        plt.title("Weekday: {}, {} probes".format(endpoint, len(diffrtt["startpoint"].unique())))
        plt.ylim(ylim)
        # plt.tight_layout()
        filename = filename.format(endpoint, "weekday")
        plt.savefig(filename)

        fig = plt.figure(figsize=(6,4))
        plt.plot(weekend_avg["median"])
        plt.plot(weekend_avg["confhigh"])
        plt.plot(weekend_avg["conflow"])
        plt.ylabel("Average Differential RTT (ms)")
        plt.xlabel("Time ({})".format(tz))
        plt.title("Weekend: {}, {} probes".format(endpoint, len(diffrtt["startpoint"].unique())))
        plt.ylim(ylim)
        # plt.tight_layout()
        filename = filename.format(endpoint, "weekend")
        plt.savefig(filename)


