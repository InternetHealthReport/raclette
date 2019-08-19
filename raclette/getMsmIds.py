from ripe.atlas.cousteau import MeasurementRequest

probes = set([10843, 10855, 12168, 13707, 14900, 14951, 15357, 21632, 23471, 23730, 24311, 24600, 27448, 27842, 27862])

filters = {'fields':'probes', 'interval__lte':3600, 'start_time__lte':1454284800, 'stop_time__gte':1458000000, "type": "traceroute"}
# filters = {'interval__gte':1800, "status": 2, "type": "traceroute", "af": 4}
measurements = MeasurementRequest(**filters)

count = 0
stat = {}
msmIds = []
for msm in measurements:
    
    if len(msm['probes'])==0 or probes.intersection(set([p['id'] for p in msm['probes']])):
    # if (msm["participant_count"] is None or msm["participant_count"]>100) :
        # for k,v in msm.
        print(msm)
        msmIds.append(msm["id"])
        count += 1

# Print total count of found measurements
print("Found %s interesting measurement out of %s" % (count, measurements.total_count))
print(list(set(msmIds)))
