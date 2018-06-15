
class IPTimeTrack():

    def __init__(self, ip2asn):
        self.ip2asn = ip2asn
        pass

    def traceroute2timetrack(trace):
	"""Read a single traceroute result and compute a path 
	differential RTTs.
	"""

	probeIp = trace["from"]
	probeId = None
	msmId = None
	if "prb_id" in trace:
	    probeId = trace["prb_id"]
	if "msm_id" in trace:
	    msmId = trace["msm_id"]
	prevRttMed = {}

	for hopNb, hop in enumerate(trace["result"]):
	    try:

		if "result" in hop :

		    rttList = defaultdict(list) 
		    rttMed = {}

		    for res in hop["result"]:
			if not "from" in res  or tools.isPrivateIP(res["from"]) or not "rtt" in res or res["rtt"] <= 0.0:
			    continue

			# assert hopNb+1==hop["hop"] or hop["hop"]==255 

			rttList[res["from"]].append(res["rtt"])

		    for ip2, rtts in rttList.iteritems():
			rttAgg = np.median(rtts)
			rttMed[ip2] = rttAgg

			# Differential rtt
			if len(prevRttMed):
			    for ip1, pRttAgg in prevRttMed.iteritems():
				if ip1 == ip2 :
				    continue

				# data for (ip1, ip2) and (ip2, ip1) are put
				# together in mergeRttResults
				if not (ip1, ip2) in diffRtt:
				    diffRtt[(ip1,ip2)] = {"rtt": [],
							    "probe": [],
							    "msmId": defaultdict(set)
							    }

				i = diffRtt[(ip1,ip2)]
				i["rtt"].append(rttAgg-pRttAgg)
				i["probe"].append(probeIp)
				i["msmId"][msmId].add(probeId)
	    finally:
		prevRttMed = rttMed
		# TODO we miss 2 inferred links if a router never replies

	return diffRtt




