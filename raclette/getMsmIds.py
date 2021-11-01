from ripe.atlas.cousteau import MeasurementRequest
import arrow

# select all measurement that were active for at least a week
one_month_ago = int(arrow.utcnow().shift(days=7).timestamp())
# and include more than 100 probes
min_participants = 100

filters = {'interval__lte':3600, 'start_time__lte':one_month_ago,  "type": "traceroute", 'af': 4, 'status': 2, 'first_hop': 1}
measurements = MeasurementRequest(**filters)

count = 0
stat = {}
msmIds = []
for msm in measurements:
    
    if (
            msm["participant_count"] is None 
            or msm["participant_count"] > min_participants
            ) :
        msmIds.append(msm["id"])
        count += 1

# Output raclette config file
msm_str = [str(msm) for msm in set(msmIds)]

print(f"""[main]

[io]
# Options for the traceroute producer/consumer
kafka_topic=ihr_atlas_traceroutev4

start =
stop =
chunk_size=300
msm_ids= {', '.join(msm_str)}
probe_ids=
""")
