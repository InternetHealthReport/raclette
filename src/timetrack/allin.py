import logging
import tools
from ripe.atlas.cousteau import ProbeRequest
import reverse_geocoder as rg


class TimeTrackConverter():
    """Convert traceroutes to time tracks with ASN and probes city."""

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
                # self.probe_info[probe["address_v4"]] = probe
                # self.probe_info[probe["address_v6"]] = probe
            # except TypeError:
                # continue
        logging.info("Ready to convert traceroutes!")


    def traceroute2timetrack(self, trace):
        """Read a single traceroute result and get rtts for the destination city
        """

        # Check if the traceroute is valid
        if "prb_id" not in trace or trace is None or "error" in trace["result"][0] or "err" in trace["result"][0]["result"]:
            return None


        prb_id = str(trace["prb_id"])
        prb_ip = trace.get("from","")
        asn_str = "asn_v"+str(trace["af"])

        try:
            probe = self.probe_info[prb_ip]
        except KeyError:
            probe = self.probe_info.setdefault(prb_ip, {
                asn_str: self.i2a.ip2asn(prb_ip) if prb_ip else "Unk PB"+prb_id
                })

        if asn_str not in probe:
            probe[asn_str] = self.i2a.ip2asn(prb_ip) if prb_ip else "Unk PB"+prb_id

        # Initialisation of the timetrack
        timetrack = {"prb_id": "PB"+prb_id, "from_asn": probe[asn_str], 
            "msm_id": trace["msm_id"], "timestamp":trace["timestamp"], "rtts":[]}

        if "city" in probe:
            timetrack["rtts"].append((probe["city"], [0]))

        for hopNb, hop in enumerate(trace["result"]):

            if "result" in hop :

                router_ip = ""
                router_asn= ""
                for res in hop["result"]:
                    if not "from" in res or not "rtt" in res or res["rtt"] <= 0.0:
                        continue

                    if res["from"] != router_ip:
                        router_ip = res["from"]    
                        if tools.isPrivateIP(router_ip):
                            continue

                        router_asn = self.i2a.ip2asn(router_ip)
                        if router_asn<0:
                            router_asn = "IX"+str(router_asn*-1)
                        else:
                            router_asn = "AS"+str(router_asn)
                    
                        # if router_asn == "unknown":
                            # router_asn = router_ip
                    
                    idx = -1
                    if len(timetrack["rtts"])==0 or timetrack["rtts"][idx][0] != router_asn:
                        if len(timetrack["rtts"])>1 and timetrack["rtts"][idx-1][0] == router_asn:
                            idx -= 1
                        else:
                            timetrack["rtts"].append((router_asn,[]))
                    timetrack["rtts"][idx][1].append(res["rtt"])

                if router_ip == trace["dst_addr"] and trace["dst_addr"] in self.probe_info:
                    dest_city = self.probe_info[trace["dst_addr"]]["city"] if trace["dst_addr"] in self.probe_info else "Unk"
                    timetrack["rtts"].append( (dest_city, timetrack["rtts"][idx][1]) )

        return timetrack
