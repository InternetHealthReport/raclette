import logging
import tools

class FirstHopTimeTrack():
    """Convert traceroutes to time tracks for only the first public hop."""

    def __init__(self, ip2asn):
        self.i2a = ip2asn

    def traceroute2timetrack(self, trace):
	"""Read a single traceroute result and get rtts for the first public hop
	"""

        found_first_hop = False
	if "prb_id" not in trace:
            logging.warning("No probe ID given: %s" % trace)
            return None

        probe_asn = 0 # Not needed here, in theory first link stays in the same ASN
        timetrack = {"prb_id": trace["prb_id"], "from_asn": probe_asn, 
                "msm_id": trace["msm_id"], "timestamp":trace["timestamp"], "rtts":[]}

	for hopNb, hop in enumerate(trace["result"]):

            if "result" in hop :

                router_ip = ""
                for res in hop["result"]:
                    if not "from" in res  or tools.isPrivateIP(res["from"]) or not "rtt" in res or res["rtt"] <= 0.0:
                        continue

                    found_first_hop = True
                    if res["from"] != router_ip:
                        router_ip = res["from"]    
                        router_asn = self.i2a.ip2asn(router_ip)

                    if len(timetrack["rtts"])==0 or timetrack["rtts"][-1][0] != router_asn:
                        timetrack["rtts"].append((router_asn,[]))

                    timetrack["rtts"][-1][1].append(res["rtt"])

                    if str(trace["prb_id"])=="11902" and str(router_asn) == "17676":
                        print trace

                if found_first_hop:
                    return timetrack

