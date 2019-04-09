import os
import sys
import logging
from matplotlib import pylab as plt
import itertools
import sqlite3
import numpy as np
from collections import defaultdict
from datetime import datetime
import matplotlib.dates as mdates
import pandas as pd

try:
    import reverse_geocoder as rg
except ImportError:
    logging.warn("Could not import reverse_geocoder")


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

def location2str(location):
    """Prettier textual representation of the given location"""
    
    location = location.replace("%","")
    if location.startswith("CT"):
        return location[2:]
    elif location.startswith("PB"):
        return "Probe"+location[2:]
    else:
        return location

class Plotter(object):

    """Read results from the sqlite database and plot interesting stuff. """

    def __init__(self, db="results/ashash_results.sql", fig_directory="fig/"):
        if not isinstance(db,list):
            db = [db]

        self.dbfiles = db
        self.conn = []
        for d in db:
            if os.path.getsize(d) > 100000:
                conn = sqlite3.Connection(d) 
                self.conn.append(conn)

        self.probe_info = {}

        if not os.path.exists(fig_directory):
            os.mkdir(fig_directory)

        self.fig_directory = fig_directory

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


    def replace_by_probe_geoloc(self, diffrtt, resolution):
        """Replace probes in the given DataFrame by their geolocation. The resolution 
        parameter can take the following values: cc, name, admin1, admin2"""

        geo_loc = {}
        nb_probes_per_geo = defaultdict(int)
        for loc in diffrtt["startpoint"].unique():
            if loc.startswith("PB"):
                geo = self.probe_geo_loc(int(loc[2:]), resolution)
                geo_loc[loc] = geo
                nb_probes_per_geo[geo] += 1

        diffrtt["startpoint"] = diffrtt["startpoint"].replace(geo_loc.keys(), geo_loc.values())

        return diffrtt, nb_probes_per_geo


    def diffrtt_distribution(self, startpoint, endpoint, filename="{}_{}_diffrtt_distribution.pdf", expid=1):
        """Plot the distribution of differential RTT values for the given start
        and end points. """
        
        all_df = []
        for conn in self.conn:
            all_df.append(pd.read_sql_query("SELECT ts, median, confhigh, conflow, nbtracks, nbprobes  FROM diffrtt where expid=? and startpoint=? and endpoint=?", conn, "ts", params=(expid, startpoint, endpoint), parse_dates=["ts"]) )
            
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
        fname = self.fig_directory+filename.format(startpoint, endpoint)
        fname = fname.replace(" ","_").replace(",","")
        fig.savefig(filename)


    def metric_over_time(self, startpoint, endpoint, metric="median", 
            filename="{}_{}_{}_expid{}_diffrtt_time.pdf", expid=1, tz="UTC", 
            ylim=None, geo_resolution=None, group = True, label=None, 
            startpoint_label=None, endpoint_label=None, displayed_anomaly=-1,
            title=None):
        """Plot a metric (e.g. median, nbtracks, or nbpobes) for the given locations.

        Args:
            startpoint (str): The start location. It can be a SQL regular expression 
            to match several locations (e.g. 'PB%' select all probes).
            endpoint (str): The end location. It can be a SQL regular expression 
            to match several locations (e.g. 'PB%' select all probes).
            metric (str): A column name from the database (median, confhigh, 
            conflow, nbtracks, nbprobes).
            ylim (list): The upper and lower bound for the y axis.
            geo_resolution (str): Replace probes by their geolocation if set to
            'name', 'admin1', 'admin2', or 'cc'.
            group (bool): Plot all graph in one figure if True.
            label (str): Label for non-grouped plot.
            startpoint_label (str): To change the label for the startpoint.
            endpoint_label (str): To change the label for the endpoint.
            displayed_anomaly (float): Display anomalies with a reliability score
            lower than the given value. Set to -1 to not show anomalies.

        """

        all_df = []
        endpoint_label = location2str(endpoint) if endpoint_label is None else endpoint_label
        startpoint_label = location2str(startpoint) if startpoint_label is None else startpoint_label

        if displayed_anomaly>=0:
            for conn in self.conn:
                # all_df.append(pd.read_sql_query(
                    # "select diffrtt.*, anomaly.anomaly as anomaly, anomaly.reliability as reliability from diffrtt left join anomaly ON diffrtt.expid=anomaly.expid and diffrtt.startpoint=anomaly.startpoint and diffrtt.endpoint = anomaly.endpoint and diffrtt.ts = anomaly.ts where reliability > 0 limit 10;",
                    # conn,"ts", parse_dates=["ts"] 
                    # ))
                all_df.append(pd.read_sql_query( 
                    ("SELECT diffrtt.*, anomaly, reliability " 
                    "FROM diffrtt "
                    "LEFT JOIN anomaly ON diffrtt.ts=anomaly.ts AND "
                    "diffrtt.expid=anomaly.expid AND "
                    "diffrtt.startpoint=anomaly.startpoint AND "
                    "diffrtt.endpoint=anomaly.endpoint "
                    "WHERE diffrtt.expid=? AND "
                    "diffrtt.startpoint like ? AND "
                    "diffrtt.endpoint like ? "
                    "ORDER BY ts"), 
                    conn, "ts", 
                    params=(expid, startpoint, endpoint), parse_dates=["ts"]) )
                print(all_df[-1])
            
        else:
            for conn in self.conn:
                all_df.append(pd.read_sql_query( 
                    ("SELECT * " 
                    "FROM diffrtt "
                    "WHERE expid=? and startpoint like ? and endpoint like ? "
                    "ORDER BY ts"), 
                    conn, "ts", params=(expid, startpoint, endpoint), parse_dates=["ts"]) )

            
        diffrtt = pd.concat(all_df)
        diffrtt.index = diffrtt.index.tz_localize("UTC")

        # Geolocate probes if needed
        if geo_resolution is not None:
            diffrtt, nb_probes_per_geo = self.replace_by_probe_geoloc(diffrtt, geo_resolution)

        diffrtt_grp = diffrtt.groupby(["startpoint","endpoint"])

        nbtracks_avg = diffrtt["nbtracks"].mean()
        nbhops = diffrtt["hop"].mean()
        logging.warning("{} average samples".format(nbtracks_avg))
        logging.warning("The two selected locations are on average {} hops away".format(nbhops))

        if group:
            fig = plt.figure(figsize=(6,3))

        for locations, data in diffrtt_grp:
            if data["nbtracks"].mean()<nbtracks_avg/2.0:
                continue
            if not group :
                fig = plt.figure(figsize=(6,3))
            # Ignore locations with a small number of samples
            if group:
                x_label = "{} to {}".format(
                        location2str(locations[0]), 
                        location2str(locations[1])) if label is None else label
                plt.plot(data[metric], label=x_label)
                # plt.plot(data["confhigh"], label=x_label)
                # plt.plot(data["conflow"], label=x_label)
            else:
                plt.plot(data[metric], label=label)
                # plt.plot(data["confhigh"], label=label)
                # plt.plot(data["conflow"], label=label)
                if title is None:
                    plt.title("{} to {} ({} probes)".format(
                        location2str(locations[0]), 
                        location2str(locations[1]), 
                        nb_probes_per_geo[locations[0]]))
                else:
                    plt.title(title)

            # Plot anomalous times
            if displayed_anomaly>=0:
                anomalies = diffrtt[diffrtt["anomaly"].notnull()] 
                anomalies = anomalies[ anomalies["reliability"]<=displayed_anomaly]
                if len(anomalies):
                    plt.plot(anomalies[metric], 'xr')
                
            
        
            plt.gca().xaxis_date(tz)
            if metric == "median":
                plt.ylabel("diff. RTT (ms)")
            else:
                plt.ylabel(metric)

            plt.xlabel("Time ({})".format(tz))
            if label is not None or (group and len(diffrtt_grp)>1):
                plt.legend(loc='best')
            
            if not group:
                fig.autofmt_xdate()
                plt.ylim(ylim)
                plt.tight_layout()
                fname = self.fig_directory+filename.format(locations[0], locations[1], metric, expid)
                fname = fname.replace(" ","_").replace(",","")
                plt.savefig(fname)

        if group:
            if title is None:
                plt.title("{} to {}".format(startpoint_label, endpoint_label))
            else:
                plt.title(title)
            fig.autofmt_xdate()
            plt.ylim(ylim)
            plt.tight_layout()
            fname = self.fig_directory+filename.format(startpoint_label, endpoint_label, metric, expid)
            fname = fname.replace(" ","_").replace(",","")
            plt.savefig(fname)


    def profile_endpoint(self, endpoint, filename="{}_{}_profile_{}_expid{}.pdf", 
            expid=1, tz="UTC", ylim=None, geo_resolution="cc"):
        """Plot the daily delay profile for the given endpoint.
        
        Compute the median delay for each hour of the day and plot a 24h profile.
        Weekdays and weekends are plotted separately.
        
        Args:
            endpoint (str): Select all differential RTTs to this end location.
            filename (str): Filename for the plots.
            expid (int): Experiment ID for selecting the correct results.
            tz (str): Project the profile in the given time zone.
            ylim (list): Force the lower and upper bound for the y axis.
            geo_resolution (str): Replace probes by their geolocation. Possible
            values are: 'name', 'admin1', 'admin2', or 'cc'.
        """


        all_df = []
        for conn in self.conn:
            all_df.append(pd.read_sql_query( 
                ("SELECT ts, startpoint, endpoint, median, confhigh, conflow, nbtracks "
                    "FROM diffrtt where expid=? AND endpoint=? "
                    "ORDER BY ts") , 
                    conn, "ts", params=(expid, endpoint), parse_dates=["ts"]) )
            
        diffrtt = pd.concat(all_df)
        diffrtt.index = diffrtt.index.tz_localize("UTC")
        if tz != "UTC":
            diffrtt.index = diffrtt.index.tz_convert(tz)

        diffrtt, nb_probes_per_geo = self.replace_by_probe_geoloc(diffrtt, geo_resolution)

        diffrtt_grp = diffrtt.groupby(["startpoint","endpoint"])

        nbtracks_avg= diffrtt["nbtracks"].mean()
        logging.warn("{} average samples".format(nbtracks_avg))
        for locations, data in diffrtt_grp:

            # Split weekday and weekend
            weekday = data[data.index.weekday<5]
            weekend = data[data.index.weekday>4]
            weekday_avg = weekday.groupby(weekday.index.hour).median()
            weekend_avg = weekend.groupby(weekend.index.hour).median()

            fig = plt.figure(figsize=(6,4))
            plt.plot(weekday_avg["median"])
            plt.plot(weekday_avg["confhigh"])
            plt.plot(weekday_avg["conflow"])
            plt.ylabel("Differential RTT (ms)")
            plt.xlabel("Time ({})".format(tz))
            plt.title("Weekday: {} ({}), {} probes".format(endpoint, locations[0], nb_probes_per_geo[locations[0]]))
            plt.ylim(ylim)
            # plt.tight_layout()
            fname = self.fig_directory+filename.format(locations[0], endpoint, "weekday", expid)
            fname = fname.replace(" ","_").replace(",","")
            plt.savefig(fname)

            fig = plt.figure(figsize=(6,4))
            plt.plot(weekend_avg["median"])
            plt.plot(weekend_avg["confhigh"])
            plt.plot(weekend_avg["conflow"])
            plt.ylabel("Average Differential RTT (ms)")
            plt.xlabel("Time ({})".format(tz))
            plt.title("Weekend: {} ({}), {} probes".format(endpoint, locations[0], nb_probes_per_geo[locations[0]]))
            plt.ylim(ylim)
            # plt.tight_layout()
            fname = self.fig_directory+filename.format(locations[0], endpoint, "weekend", expid)
            fname = fname.replace(" ","_").replace(",","")
            plt.savefig(fname)

    def first_hop_analysis(self, asns, geo_resolution="cc", expid=1, label=None, ylim=None, tz="UTC"):
        """Plot the median RTT over time and daily profile for all asns given in asns.
        Assume these ASNs are access networks hosting probes."""
        for asn in asns:
            logging.info("Plotting {}".format(asn))
            self.metric_over_time("%", asn, geo_resolution=geo_resolution, group=False, expid=expid, label=label, ylim=ylim)
            self.metric_over_time("%", asn, metric="nbtracks", geo_resolution=geo_resolution, group=False, expid=expid, label=label)
            self.metric_over_time("%", asn, metric="nbprobes", geo_resolution=geo_resolution, group=False, expid=expid, label=label)
            self.profile_endpoint(asn, expid=expid, geo_resolution=geo_resolution, tz=tz)


if __name__ == "__main__":
   
    if len(sys.argv)<4:
        print("usage: {} db startpoint enpoint [expid [title]]".format(sys.argv[0]) )
        sys.exit()

    db = sys.argv[1]
    startpoint=sys.argv[2]
    endpoint=sys.argv[3]
    expid=1
    title=None
    if len(sys.argv)>4:
        expid = int(sys.argv[4])
        print("Fetching results for expid={}".format(expid))
    if len(sys.argv)>5:
        title = sys.argv[5]

    pl = Plotter(db) 

    pl.metric_over_time(startpoint, endpoint, expid=expid, title=title)
    pl.metric_over_time(startpoint, endpoint, "nbtracks", expid=expid)
    pl.metric_over_time(startpoint, endpoint, "hop", expid=expid)
    pl.metric_over_time(startpoint, endpoint, "nbprobes", expid=expid)
    pl.metric_over_time(startpoint, endpoint, "entropy", expid=expid)
    pl.metric_over_time(startpoint, endpoint, "nbsamples", expid=expid)
    # pl.profile_endpoint(startpoint)
    # pl.profile_endpoint(endpoint)


