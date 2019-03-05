import logging
import tools

class TimeTrackConverter():
    """Convert traceroutes to time tracks for only the first public hop."""

    def __init__(self, ip2asn):
        self.i2a = ip2asn
        self.probe_info = {}

        for probe in tools.get_probes_info():
            try:
                prb_id = str(probe["id"])
                probe["location"] = "|".join(["PB"+prb_id,probe["city"]])
                self.probe_info[prb_id] = probe
            except TypeError:
                continue


    def traceroute2timetrack(self, trace):
        """Read a single traceroute result and get rtts for the first public hop"""

        found_first_hop = False
        if "prb_id" not in trace:
            logging.warning("No probe ID given: %s" % trace)
            return None

        asn_str = "asn_v"+str(trace["af"])
        ip_space_str = "v"+str(trace["af"])
        prb_id = str(trace["prb_id"])
        prb_ip = trace.get("from", "")

        try:
            probe = self.probe_info[prb_id]
            timetrack = {"prb_id": "PB"+prb_id, 
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

            timetrack = {"prb_id": "PB"+prb_id, 
                    "from_asn": "".join(["AS", str(probe[asn_str]),
                        ip_space_str]),
                "msm_id": trace["msm_id"], "timestamp":trace["timestamp"], 
                "rtts":[]}


        timetrack["rtts"].append( [probe["location"], [0]] )

        for hopNb, hop in enumerate(trace["result"]):

            if "result" in hop :

                router_ip = ""
                for res in hop["result"]:
                    if not "from" in res  or tools.isPrivateIP(res["from"]) or not "rtt" in res or res["rtt"] <= 0.0:
                        continue

                    found_first_hop = True
                    if res["from"] != router_ip:
                        router_ip = res["from"]    
                        router_asn = str(self.i2a.ip2asn(router_ip))

                    if len(timetrack["rtts"])==0 or timetrack["rtts"][-1][0] != router_asn:
                        timetrack["rtts"].append((router_asn,[]))

                    timetrack["rtts"][-1][1].append(res["rtt"])


                if found_first_hop:
                    return timetrack

