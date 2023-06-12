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
        """Retrieve delay from last private IP and first public IP 
        (usually home router to ISP's edge router)
        """

        if ("prb_id" not in trace or trace is None 
                or "error" in trace["result"][0] 
                or "err" in trace["result"][0]["result"]):
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
                        asn_str: self.i2a.ip2asn(prb_ip) \
                                if prb_ip else "Unk PB"+prb_id,
                        "location": "PB"+prb_id })
                    self.probe_info[prb_id] = probe

                elif asn_str not in probe:
                    probe[asn_str] = self.i2a.ip2asn(prb_ip) \
                            if prb_ip else "Unk PB"+prb_id

                timetrack = {"prb_id": "PB"+prb_id+ip_space_str,
                        "from_asn": "".join(["AS", str(probe[asn_str]),
                            ip_space_str]),
                    "msm_id": trace["msm_id"], "timestamp":trace["timestamp"],
                    "rtts":[]}

            # add address version to probe's locations
            startpoints = probe['location'].replace('|',ip_space_str+'|') + ip_space_str
            # add probe group location
            startpoints += '|PGAS'+str(probe[asn_str])+ip_space_str

            timetrack["rtts"].append( [startpoints, [0]] )

            prev_hop_rtt = None
            curr_hop_rtt = [0]
            for hopNb, hop in enumerate(trace["result"]):

                if "result" in hop :
                    if len(curr_hop_rtt):
                        prev_hop_rtt = curr_hop_rtt
                        curr_hop_rtt = []

                    for res in hop["result"]:
                        if not "from" in res or not "rtt" in res \
                                or res["rtt"] <= 0.0:
                            continue

                        res_from = res["from"]
                        rtt_value = res["rtt"]

                        if isPrivateIP(res_from):
                            curr_hop_rtt.append(rtt_value)
                            continue


                        if res_from != router_ip:
                
                            asn_tmp = self.i2a.ip2asn(res_from)
                            if asn_tmp < 0:
                                # We are not looking at ISPs here
                                return timetrack
                            else:
                                found_first_hop = True
                                location_str = 'EDAS'+str(asn_tmp)+ip_space_str

                            router_ip = res_from

                        if len(timetrack["rtts"])==0 or timetrack["rtts"][-1][0] != location_str:
                            timetrack["rtts"].append((location_str,[]))

                        timetrack["rtts"][-1][1].append(rtt_value)


                    if found_first_hop:
                        # Replace probes rtt by the previous hop RTT
                        # That mean we will compute last mile delay only,
                        # avoiding delay on the home/private network
                        timetrack["rtts"][0][1] = prev_hop_rtt

                    return timetrack


        except Exception as e:
            print("type error: " + str(e))
            print(traceback.format_exc())

        return timetrack
