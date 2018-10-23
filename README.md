# raclette: Human-friendly monitoring of Internet delays

Raclette simplifies the analysis of numerous traceroute results by merging results and adding semantics. For example, one can use raclette to read millions traceroute results from RIPE Atlas API and then query raclette for delays between “Amsterdam, NL” and “AS15169” (Google) to obtain the median Round Trip Times (RTTs) between all vantage points in Amsterdam and all Google IP addresses found in traceroutes.

## Install
Get the latest source files:
```
git clone git@github.com:InternetHealthReport/raclette.git
```

Install dependencies and install raclette:
```
cd raclette
pip install -r requirements.txt 
python setup.py build_ext
sudo python setup.py install
```


## Example
In this example we'll look at delay changes for SMW3 cable failures and ASC cable activation. For that we look at Atlas anchoring measurements from September 3rd to 5th, 2018.

### Configuration file
The first step is to make a configuration file that will describe the start/end times, the measurement IDs, and the output directory. For this example the configuration file looks like this ([also available in the source files)](https://github.com/InternetHealthReport/raclette/blob/master/conf/asc-start.conf):
```
[main]

[io]
# Options for the Atlas REST API
start = 2018-09-02T12:00
stop = 2018-09-06T12:00
# Fetch data by chunks of chunk_size seconds. Set a smaller value if you have
# memory problems
chunk_size = 1800
msm_ids =  1748022, 1748024, 11645084, 11645087, 2244316, 2244318, 2244316, 2244318, 2435592, 2435594, 1796567, 1796569, 2904335, 2904338, 1618360, 1618362, 7970886, 7970889, 7970886, 7970889, 6886972, 6886975, 12237261 
probe_ids = 

# Options for the output
results = results/ASC_start/results_%(start)s.sql
log = results/ASC_start/log_%(start)s.log

[timetrack]
converter = allin_cy
add_probe = yes

[tracksaggregator]
expiration = 10000
window_size = 1800
significance_level = 0.05
# ignore links visited by small number of tracks/traceroutes
min_tracks = 3 

[lib]
ip2asn_directory = raclette/lib/
ip2asn_db = data/rib.20180701.pickle
ip2asn_ixp =
```

The most important options are:
- start, stop: The beginning and end time of the measurement period
- msm_ids: The atlas measurements for which we want traceroutes
- log: The log file used for this run
- results: All results are stored in this file. This a sqlite database.

For this example we save this file to conf/asc-start.conf.

### Traceroute analysis
To download traceroute and compute delays just run the following command:
```
python raclette/raclette.py -C conf/asc-start.conf
```
This is going to take about 30 minutes, it downloads four days of traceroutes.
You can follow the progress in the log file with the following command:
```
tail -F results/ASC_start/log_2018-09-02T12:00.log
```

### Looking at the results
All results are stored in a sqlite database. The filename is the one you gave in the configuration file (io/results).
Raclette comes with a script to easily plot graphs from that database. For example, to plot delays between Melbourne and Singapore use the following commands:
```
python raclette/plotter.py results/ASC_start/results_2018-09-02T12:00.sql  'CTSingapore, SG' 'CTMelbourne, AU'
```
If you have executed raclette.py several times with the same results file. The results for each instance are stored with a different experiment ID (cf. the field expid in the database). To plot with results from the third execution:
```
python raclette/plotter.py results/ASC_start/results_2018-09-02T12:00.sql  'CTSingapore, SG' 'CTMelbourne, AU' 3
```

You can also directly query the database with a sqlite client. The table you want to look at is diffrtt:
```
sqlite3 results/ASC_start/results_2018-09-02T12:00.sql
sqlite> .schema diffrtt
CREATE TABLE diffrtt (ts integer, startpoint text, endpoint text, median real, confhigh real, conflow real, nbtracks integer, nbprobes integer, entropy real, hop integer, expid integer, foreign key(expid) references experiment(id));
[...]
sqlite> .headers on
sqlite> select * from diffrtt limit 10;
ts|startpoint|endpoint|median|confhigh|conflow|nbtracks|nbprobes|entropy|hop|expid
1535890500|PB20092|AS48441v4|0.597|0.743|0.503|4|1|0.0|1|3
1535890500|PB20092|IPv4|311.129|344.056|185.155|22|1|0.0|3|3
1535890500|PB20092|AS12389v4|167.621|172.017|2.027|4|1|0.0|2|3
1535890500|PB20092|IX438v4|184.429|185.155|181.18|4|1|0.0|3|3
1535890500|PB20092|AS4637v4|340.924|348.105|264.141|4|1|0.0|4|3
1535890500|AS48441v4|AS12389v4|165.556|167.145|1.635|4|1|0.0|1|3
1535890500|AS48441v4|IPv4|346.223|348.654|343.459|18|1|0.0|3|3
1535890500|IPv4|AS12389v4|22.067|22.677|1.644|6|2|0.918295834054489|1|3
1535890500|AS48441v4|IX438v4|182.547|184.018|180.735|4|1|0.0|2|3
1535890500|IPv4|IX438v4|14.885|15.534|14.144|669|51|0.96986633428238|1|3
```

