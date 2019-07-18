import json
import argparse
from datetime import datetime

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
            description='Find Atlas probe IDs corresponding to a set of criteria.')
    parser.add_argument('--asns_v4', type=int, nargs='+', 
            help='IPv4 ASN of the probes')
    parser.add_argument('--city', nargs='+', help='City of the probes')
    parser.add_argument('--country', nargs='+', help='Country code of the probes')
    parser.add_argument('--anchor', dest="anchor", action="store_true", 
            help='Select only anchors')
    parser.add_argument('--no-anchor', dest="anchor", action="store_false", 
            help='Ignore anchors')
    parser.add_argument('--info', action="store_true", 
            help='Print all information for selected probes')
    parser.add_argument('--year', nargs='+', type=int, help='Year of probe activity')

    args = parser.parse_args()

    cache = json.load(open("cache/probe_info.json", "r"))
    probes = cache["probes"]

    selected_probes = []

    for probe in probes:
        
        selected = True

        if args.asns_v4 and probe["asn_v4"] not in args.asns_v4:
            selected = False

        if args.city and probe["city"] not in args.city:
            selected = False

        if args.country and probe["country_code"] not in args.country:
            selected = False

        if args.anchor is not None and probe["is_anchor"] != args.anchor:
            selected = False

        if args.year is not None:
            if probe['first_connected'] is None:
                selected = False
            else:
                startdate = datetime.utcfromtimestamp(probe['first_connected'])
                enddate = datetime.utcfromtimestamp(probe['last_connected'])
                if startdate.year > max(args.year) or enddate.year < min(args.year): 
                    selected = False

        if selected:
            if args.info:
                selected_probes.append(probe["id"])
            else:
                selected_probes.append(probe)


    print(json.dumps(selected_probes, indent=4))
