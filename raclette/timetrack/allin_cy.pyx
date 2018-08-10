#cython: boundscheck=False, nonecheck=False
import logging
import tools
from ripe.atlas.cousteau import ProbeRequest
import reverse_geocoder as rg
from cpython cimport bool
import re

# https://en.wikipedia.org/wiki/Private_network
priv_24 = re.compile("^10\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
priv_20 = re.compile("^192\.168\.\d{1,3}.\d{1,3}$")
priv_16 = re.compile("^172.(1[6-9]|2[0-9]|3[0-1]).[0-9]{1,3}.[0-9]{1,3}$")
priv_lo = re.compile("^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$")

#TODO private network for ipv6
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
        logging.info("Loading probes info...")
        filters = {"tags": "system-anchor"}
        # probes = ProbeRequest(**filters)
        # for probe in probes:
            # try:
                # lon, lat = probe["geometry"]["coordinates"]
                # geoloc = rg.search((lat, lon))
                # probe["city"] = "{}, {}".format(geoloc[0]["name"], geoloc[0]["cc"])
                # if "asn_v4" not in probe:
                    # probe["asn_v4"] = "AS"+str(self.i2a.ip2asn(probe["address_v4"])) 
                # if "asn_v6" not in probe:
                    # probe["asn_v6"] = "AS"+str(self.i2a.ip2asn(probe["address_v6"]))
                # probe["location"] = "|".join(["PB"+probe[id],probe["city"]])
                # self.probe_info[probe["address_v4"]] = probe
                # self.probe_info[probe["address_v6"]] = probe
            # except TypeError:
                # continue
        logging.info("Ready to convert traceroutes!")


    def traceroute2timetrack(self, dict trace):
        """Read a single traceroute result and get rtts for the destination city
        """

        if "prb_id" not in trace or trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
            return None

        cdef int probe_asn, asn_tmp
        cdef double rtt_value
        cdef str location_str
        cdef str router_ip, res_from
        cdef dict timetrack
        cdef str src_ip = trace["from"]
        cdef str prb_id = str(trace["prb_id"])
        cdef str prb_ip = trace.get("from","")
        cdef str asn_str = "asn_v"+str(trace["af"])
        cdef str ip_space_str = "IPv"+str(trace["af"])

        try:
            probe = self.probe_info[prb_ip]
        except KeyError:
            probe = self.probe_info.setdefault(prb_ip, {
                asn_str: "AS"+str(self.i2a.ip2asn(prb_ip)) if prb_ip else "Unk PB"+prb_id,
                "location": "PB"+prb_id
                })

        # Initialisation of the timetrack
        timetrack = {"prb_id": "PB"+prb_id, "from_asn": probe[asn_str], 
            "msm_id": trace["msm_id"], "timestamp":trace["timestamp"], "rtts":[]}

        timetrack["rtts"].append( [probe["location"], [0]] )

        for hopNb, hop in enumerate(trace["result"]):

            if "result" in hop :

                for res in hop["result"]:
                    if not "from" in res or not "rtt" in res or res["rtt"] <= 0.0:
                        continue

                    res_from = res["from"] 
                    rtt_value = res["rtt"]

                    if res_from != router_ip:
                        if isPrivateIP(res_from):
                            continue

                        asn_tmp = self.i2a.ip2asn(res_from)
                        if asn_tmp==0:
                            continue
                        elif asn_tmp < 0:
                            location_str = "".join(["IX", str(asn_tmp*-1), "|", ip_space_str])
                        else:
                            location_str = "".join(["AS", str(asn_tmp), "|", ip_space_str])
                        router_ip = res_from
                        
                        # Add city if needed
                        if router_ip == trace["dst_addr"] and trace["dst_addr"] in self.probe_info:

                            dest_city = self.probe_info[trace["dst_addr"]].get("city") 
                            if dest_city is not None:
                                location_str+= "|"+dest_city

                    idx = -1
                    if len(timetrack["rtts"])==0 or timetrack["rtts"][idx][0] != location_str:
                        if len(timetrack["rtts"])>1 and timetrack["rtts"][idx-1][0] == location_str:
                            idx -= 1
                        else:
                            timetrack["rtts"].append( [location_str,[]] )
                    timetrack["rtts"][idx][1].append(rtt_value)


        return timetrack
