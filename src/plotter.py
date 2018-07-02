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
from ripe.atlas.cousteau import Probe
import reverse_geocoder as rg


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

        self.probe_info = {}

        if not os.path.exists("fig/"):
            os.mkdir("fig")

    def get_probe_info(self, probe_id):
        """Query RIPE Atlas API to get probe information. The result is stored
        in self.probe_info."""

        if probe_id not in self.probe_info:
            # filters = {"id": probe_id}
            # probes = ProbeRequest(**filters)
            probe = Probe(id=probe_id)
            self.probe_info[probe_id] = probe

        return self.probe_info[probe_id]
        


    def probe_geo_loc(self, probe_id, resolution):
        """Retrieve probe informations and find the geo-location of the probe
        corresponding to probe_id. The resolution parameter can take the following
        values: cc, name, admin1, admin2"""

        probe = self.get_probe_info(probe_id)
        lon, lat = probe.geometry["coordinates"]
        geoloc = rg.search((lat, lon))

        return geoloc[0][resolution]


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


    def metric_over_time(self, startpoint, endpoint, metric="median", filename="fig/{}_{}_{}_expid{}_diffrtt_time.pdf", expid=1, tz="UTC", ylim=None, probe_geoloc=None, group = True, label=None, startpoint_label=None, endpoint_label=None):

        all_df = []
        endpoint_label = endpoint if endpoint_label is None else endpoint_label
        startpoint_label = startpoint if startpoint_label is None else startpoint_label

        for conn in self.conn:
            all_df.append(pd.read_sql_query("SELECT ts, startpoint, endpoint, median, confhigh, conflow, nbsamples FROM diffrtt where expid=? and startpoint like ? and endpoint like ? order by ts" , conn, "ts", params=(expid, startpoint, endpoint), parse_dates=["ts"]) )
            
        diffrtt = pd.concat(all_df)
        diffrtt.index = diffrtt.index.tz_localize("UTC")

        # Geolocate probes if needed
        if probe_geoloc is not None:
            geo_loc = {}
            nb_probes_per_geo = defaultdict(int)
            for loc in diffrtt["startpoint"].unique():
                if loc.startswith("pid"):
                    geo = self.probe_geo_loc(int(loc.rpartition("_")[2]), probe_geoloc)
                    geo_loc[loc] = geo
                    nb_probes_per_geo[geo] += 1

            diffrtt["startpoint"] = diffrtt["startpoint"].replace(geo_loc.keys(), geo_loc.values())

        diffrtt_grp = diffrtt.groupby(["startpoint","endpoint"])

        nbsamples_avg = diffrtt["nbsamples"].mean()
        logging.warn("{} average samples".format(nbsamples_avg))

        if group:
            fig = plt.figure(figsize=(8,4))

        for locations, data in diffrtt_grp:
            if data["nbsamples"].mean()<nbsamples_avg:
                continue
            if not group :
                fig = plt.figure(figsize=(8,4))
            # Ignore locations with a small number of samples
            if group:
                label = str(locations) if label is None else label
                plt.plot(data[metric], label=label)
            else:
                plt.plot(data[metric], label=label)
                plt.title("{} to {} ({} probes)".format(locations[0], locations[1], nb_probes_per_geo[locations[0]]))
        
            plt.gca().xaxis_date(tz)
            plt.ylabel("Differential RTT (ms)")
            plt.xlabel("Time ({})".format(tz))
            plt.ylim(ylim)
            fig.autofmt_xdate() 
            # plt.tight_layout()
            if not group:
                fname = filename.format(locations[0], locations[1], metric, expid)
                plt.savefig(fname)

        if group:
            plt.title("{} to {}".format(startpoint_label, endpoint_label))
            plt.legend(loc='best', ncol=4, fontsize=8 )
            fname = filename.format(startpoint_label, endpoint_label, metric, expid)
            plt.savefig(fname)


    def profile_endpoint(self, endpoint, filename="fig/{}_profile_{}_expid{}.pdf", expid=1, tz="UTC", ylim=[2,12]):


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
        fname = filename.format(endpoint, "weekday", expid)
        plt.savefig(fname)

        fig = plt.figure(figsize=(6,4))
        plt.plot(weekend_avg["median"])
        plt.plot(weekend_avg["confhigh"])
        plt.plot(weekend_avg["conflow"])
        plt.ylabel("Average Differential RTT (ms)")
        plt.xlabel("Time ({})".format(tz))
        plt.title("Weekend: {}, {} probes".format(endpoint, len(diffrtt["startpoint"].unique())))
        plt.ylim(ylim)
        # plt.tight_layout()
        fname = filename.format(endpoint, "weekend", expid)
        plt.savefig(fname)


