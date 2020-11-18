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
                probe["location"] = "PB"+prb_id
                self.probe_info[prb_id] = probe
            except (TypeError, KeyError) as e:
                logging.error('Initialisation problem with probe:\n{}'.format(probe))
                continue


    def traceroute2timetrack(self, trace):
        """Read a single traceroute result and get rtts for the first public hop"""

        found_first_hop = False
        if "prb_id" not in trace or "result" not in trace:
            logging.warning("No probe ID or result given: %s" % trace)
            return None

        af = trace.get('af', 4)
        # last-mile detection works only in IPv4
        if af != 4:
            return None

        asn_str = "asn_v"+str(af)
        ip_space_str = "v"+str(af)
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


        timetrack["rtts"].append( [probe["location"]+ip_space_str, [0]] )

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
                    router_ip = res["from"]    
                    router_asn = str(self.i2a.ip2asn(router_ip))

                    # router IP might not be announced on BGP
                    if router_asn == '0' or router_asn is None:
                        if isinstance(probe[asn_str], int):
                            router_asn = str(probe[asn_str])
                        elif isinstance(probe[asn_str], str) and probe[asn_str].startswith('AS'):
                            router_asn = probe[asn_str][2:]
                        else:
                            return None

                    # check that router ASN is same as the probe
                    elif router_asn != str(probe[asn_str]).rpartition('AS')[2]:
                        return None

                    location_str = "".join(["LM", router_asn, ip_space_str])

                    if len(timetrack["rtts"])==0 or timetrack["rtts"][-1][0] != location_str:
                        timetrack["rtts"].append((location_str,[]))

                    timetrack["rtts"][-1][1].append(res["rtt"])


                if found_first_hop:
                    # Replace probes rtt by the previous hop RTT
                    # That mean we will compute last mile delay only,
                    # avoiding delay on the home/private network
                    timetrack["rtts"][0][1] = prev_hop_rtt

                    return timetrack

        return None
