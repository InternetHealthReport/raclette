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
                probe["location"] = "|".join(
                        ["PB"+prb_id, probe["city"], "PF"+probe["prefix_v4"]])
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
                    "location": "|".join("PB"+prb_id, "PF"+probe["prefix_v4"]) })
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

        prev_hop_rtt = None
        curr_hop_rtt = [0]
        for hopNb, hop in enumerate(trace["result"]):

            if len(curr_hop_rtt):
                prev_hop_rtt = curr_hop_rtt
                curr_hop_rtt = []

            if "result" in hop :

                router_ip = ""
                for res in hop["result"]:
                    if not "from" in res   or not "rtt" in res or res["rtt"] <= 0.0:
                        continue

                    if tools.isPrivateIP(res["from"]):
                        curr_hop_rtt.append(res["rtt"])
                        continue

                    found_first_hop = True
                    if res["from"] != router_ip:
                        router_ip = res["from"]    
                        router_asn = str(self.i2a.ip2asn(router_ip))
                        location_str = "".join(["AS", str(router_asn), ip_space_str])

                    if len(timetrack["rtts"])==0 or timetrack["rtts"][-1][0] != location_str:
                        timetrack["rtts"].append((location_str,[]))

                    timetrack["rtts"][-1][1].append(res["rtt"])


                if found_first_hop:
                    # Replace probes rtt by the previous hop RTT
                    # That mean we will compute last mile delay only,
                    # avoiding delay on the home/private network
                    timetrack["rtts"][0][1] = prev_hop_rtt

                    return timetrack

