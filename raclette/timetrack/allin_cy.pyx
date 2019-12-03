#cython: boundscheck=False, nonecheck=False
# cython: language_level=3

import traceback
import logging
import tools
from cpython cimport bool
import re

# https://en.wikipedia.org/wiki/Private_network
priv_24 = re.compile("^10\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
priv_20 = re.compile("^192\.168\.\d{1,3}.\d{1,3}$")
priv_16 = re.compile("^172.(1[6-9]|2[0-9]|3[0-1]).[0-9]{1,3}.[0-9]{1,3}$")
priv_lo = re.compile("^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$")

cdef bool isPrivateIP(str ip):

    if priv_24.match(ip) or priv_20.match(ip) or priv_16.match(ip) or priv_lo.match(ip):
        return True
    else:
        return False

class TimeTrackConverter():
    """Convert traceroutes to time tracks with ASN, probes city, and IP space."""

    def __init__(self, i2a):

        self.i2a = i2a
        self.probe_info = {}
        self.probe_addresses = {}
        self.ipmap = {}
        for probe in tools.get_probes_info():
            try:
                prb_id = str(probe["id"])
                if "city" in probe:
                    probe["location"] = "|".join(["PB"+prb_id,probe["city"]])
                else:
                    probe["location"] = "|".join(["PB"+prb_id])

                self.probe_info[prb_id] = probe
                # will get city names only for these addresses
                self.probe_addresses[probe["address_v4"]]= prb_id
                self.probe_addresses[probe["address_v6"]]= prb_id
            except TypeError:
                continue

        for ip, city, state, country in tools.read_ipmap_data(50):
            self.ipmap[ip] = "CT{}, {}, {}".format(city, state, country)

        logging.info("Ready to convert traceroutes! (loaded {} probes info)"
                .format(len(self.probe_info)))


    def traceroute2timetrack(self, dict trace):
        """Read a single traceroute result and get rtts for the destination city
        """

        if "prb_id" not in trace or trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
            return None

        cdef long probe_asn, asn_tmp
        cdef double rtt_value
        cdef str location_str
        cdef str router_ip =""
        cdef str res_from
        cdef dict timetrack
        cdef str src_ip = trace["from"]
        cdef str prb_id = str(trace["prb_id"])
        cdef str prb_ip = trace.get("from","")
        cdef str asn_str = "asn_v"+str(trace["af"])
        cdef str ip_space_str = "v"+str(trace["af"])

        try:
            try:
                # Initialisation of the timetrack
                probe = self.probe_info[prb_id]
                timetrack = {"prb_id": "PB"+prb_id+ip_space_str,
                        "from_asn": "".join(["AS", str(probe[asn_str]),
                            ip_space_str]),
                    "msm_id": trace["msm_id"], "timestamp":trace["timestamp"],
                    "rtts":[]}

            except KeyError:
                if prb_id not in self.probe_info:
                    probe = self.probe_info.setdefault(prb_id, {
                        asn_str: "AS"+str(self.i2a.ip2asn(prb_ip)) \
                                if prb_ip else "Unk PB"+prb_id,
                        "location": "PB"+prb_id })
                    self.probe_info[prb_id] = probe

                elif asn_str not in probe:
                    probe[asn_str] = "AS"+str(self.i2a.ip2asn(prb_ip)) \
                            if prb_ip else "Unk PB"+prb_id

                timetrack = {"prb_id": "PB"+prb_id+ip_space_str,
                        "from_asn": "".join(["AS", str(probe[asn_str]),
                            ip_space_str]),
                    "msm_id": trace["msm_id"], "timestamp":trace["timestamp"],
                    "rtts":[]}


            timetrack["rtts"].append( [probe["location"]+ip_space_str, [0]] )

            for hopNb, hop in enumerate(trace["result"]):

                if "result" in hop :

                    for res in hop["result"]:
                        if not "from" in res or not "rtt" in res \
                                or res["rtt"] <= 0.0:
                            continue

                        res_from = res["from"]
                        rtt_value = res["rtt"]

                        if res_from != router_ip:
                            if isPrivateIP(res_from):
                                continue

                            asn_tmp = self.i2a.ip2asn(res_from)
                            if asn_tmp < 0:
                                location_str = "".join(["IX", str(asn_tmp*-1),
                                    ip_space_str, "|IP", ip_space_str])
                            else:
                                location_str = "".join(["AS", str(asn_tmp),
                                    ip_space_str, "|IP", ip_space_str])
                            router_ip = res_from

                            # Add city if needed
                            locations = [location_str]
                            if router_ip == trace["dst_addr"] \
                                    and trace["dst_addr"] in self.probe_addresses:

                                pid = self.probe_addresses[trace["dst_addr"]]
                                # locations.append("PB"+str(pid)+ip_space_str)
                                dest_city = self.probe_info[pid].get("city")
                                if dest_city is not None:
                                    locations.append(dest_city+ip_space_str)

                            elif router_ip in self.ipmap:
                                locations.append(self.ipmap[router_ip]+ip_space_str)

                            location_str= "|".join(locations)

                        idx = -1
                        # location comparisons are much faster with strings (than list of strings)!
                        if timetrack["rtts"][idx][0] != location_str:
                            if len(timetrack["rtts"])>1 \
                                    and timetrack["rtts"][idx-1][0] == location_str:
                                idx -= 1
                            else:
                                timetrack["rtts"].append( [location_str,[]] )
                        timetrack["rtts"][idx][1].append(rtt_value)

        except Exception as e:
            print("type error: " + str(e))
            print(traceback.format_exc())

        return timetrack

