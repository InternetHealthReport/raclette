from ripe.atlas.cousteau import MeasurementRequest

probes = set([
234, 333, 2277, 2299, 2868, 2878, 2891, 2895, 2896, 2898, 3241, 3249, 3450, 3457, 3491, 3682, 3709, 3727, 3743, 3772, 4140, 4722, 10080, 11056, 11218, 11267, 11287, 11292, 11297, 11539, 11982, 12199, 12209, 12782, 12810, 13227, 13229, 13262, 13364, 13531, 13539, 13819, 13821, 13822, 13857, 14080, 14199, 14352, 14360, 14390, 14393, 14812, 14815, 14827, 14831, 14843, 14889, 15040, 15638, 15639, 15645, 15691, 15701, 15935, 15980, 16273, 17789, 18286, 18359, 18377, 19066, 19099, 19107, 19503, 19895, 20374, 20495, 21127, 22276, 22336, 22388, 22399, 22490, 23109, 23116, 23483, 24300, 24310, 24927, 25738, 25819, 25885, 26405, 26475, 26698, 26804, 26805, 26823, 26837, 26915, 26989, 27001, 27025, 27130, 27131, 27282, 27692, 27781, 27947, 28108, 28127, 28210, 28248, 28339, 28344, 28352, 28366, 28370, 28374, 28376, 28377, 28397, 28593, 28678, 28711, 28714, 28723, 28808, 28818, 28819, 28820, 28842, 28915, 29036, 29043, 29080, 30306, 30329, 30334, 30578, 30823, 30914, 32061, 32065, 32082, 32172, 32183, 32332, 32335, 32405, 32481, 32639, 32748, 32749, 32750, 32754, 32759, 32760, 32783, 32787, 32819, 33002, 33022, 33048, 33084, 33107, 33231, 33320, 33322, 33483, 33487, 33597, 33754, 34085, 34098, 34223, 34294, 34354, 34380, 34513, 34594, 34665, 34755, 35000, 35023, 50079, 50083, 50106, 50137, 50170, 50204, 50304, 50366, 50385, 50386, 50417, 50447, 50459, 50499, 50501, 50882, 51098, 51221, 51329, 51423, 51434, 51663, 51710, 51907, 52027, 52128, 52130, 52158, 52259
    ])

# filters = {'fields':'probes', 'interval__lte':3600, 'start_time__lte':1553829000, 'stop_time__gte':1554595200, "type": "traceroute", 'af': 4}
filters = {'fields':'probes', 'interval__lte':3600, 'start_time__lte':1553829000,  "type": "traceroute", 'af': 4}
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
