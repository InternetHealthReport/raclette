import logging
import tools

class ASTimeTrack():
    """Convert traceroutes to time tracks for each AS on the path."""

    def __init__(self, ip2asn):
        self.i2a = ip2asn

    def traceroute2timetrack(self, trace):
	"""Read a single traceroute result and get rtts for each AS
	"""

	if "prb_id" not in trace:
            logging.warning("No probe ID given: %s" % trace)
            return None

        # Check if the traceroute is valid
        if trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
            return None

        probe_asn = self.i2a.ip2asn(trace["from"]) if (trace.get("from", "")) else "Unk"
        timetrack = {"prb_id": trace["prb_id"], "from_asn": probe_asn, 
                "msm_id": trace["msm_id"], "timestamp":trace["timestamp"], "rtts":[]}

	for hopNb, hop in enumerate(trace["result"]):

            if "result" in hop :

                router_ip = ""
                for res in hop["result"]:
                    if not "from" in res  or tools.isPrivateIP(res["from"]) or not "rtt" in res or res["rtt"] <= 0.0:
                        continue

                    if res["from"] != router_ip:
                        router_ip = res["from"]    
                        router_asn = self.i2a.ip2asn(router_ip)
                        if router_asn == "unknown":
                            router_asn = router_ip
                    
                    idx = -1
                    if len(timetrack["rtts"])==0 or timetrack["rtts"][idx][0] != router_asn:
                        if len(timetrack["rtts"])>1 and timetrack["rtts"][idx-1][0] == router_asn:
                            idx -= 1
                        else:
                            timetrack["rtts"].append((router_asn,[]))
                    timetrack["rtts"][idx][1].append(res["rtt"])
        
        return timetrack
