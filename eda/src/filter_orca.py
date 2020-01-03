#!/usr/bin/env python3
import gzip
import csv
from sys import argv
from math import inf
from collections import defaultdict
from os import makedirs
from typing import Dict

if len(argv) < 2:
    raise ValueError("Missing input file")

if len(argv) < 3:
    raise ValueError("Missing output file")

COLS_TO_KEEP = [
    'txn_id',
    #'trip_group_id',
    'prev_leg_txn_id',
    #'card_serial_number',
    #'institution_id',
    'institution_name',
    #'business_date',
    'txn_dtm_pacific',
    #'txn_type_id',
    #'txn_subtype_id',
    'txn_type_descr',
    'upgrade_indicator',
    #'product_id',
    'product_descr',
    #'txn_passenger_type_id',
    'txn_passenger_type_descr',
    'passenger_count',
    'ceffv',
    #'service_agency_id',
    'service_agency_name',
    #'source_agency_id',
    'source_agency_name',
    'transit_operator_abbrev',
    #'mode_id',
    #'mode_abbrev',
    'mode_descr',
    'route_number',
    #'direction_id',
    'direction_descr',
    'agency_trip_id',
    'device_id',
    #'device_type',
    'device_place_name',
    #'device_place_id',
    #'device_location_id',
    #'device_location_code',
    #'device_location_abbrev',
    'device_location_descr',
    #'origin_location_id',
    #'origin_location_code',
    #'origin_location_abbrev',
    'origin_location_descr',
    #'destination_location_id',
    #'destination_location_code',
    #'destination_location_abbrev',
    'destination_location_descr',
    #'device_id_filt',
    'stop_id',
    'stop_time',
    'stop_lat',
    'stop_lon',
    'stop_error',
    #'viaserviceareaid',
    #'viaserviceareaname',
    'trip_id',
    #'last_mode_id',
    #'last_route_number',
    'last_stop_id',
    #'last_stop_time',
    #'last_stop_lat',
    #'last_stop_lon'
]

def null_if_empty(s): return None if s == '' else s

def keep_row(row):
    # bus, bus rapid transit,  not washington state ferries, remove snow days
    return str(row['mode_id']) in ('128', '250')  \
            and str(row['service_agency_id']) != '8' \
            and str(row['business_date']) not in ('2019-02-03', '2019-02-04', '2019-02-08', '2019-02-09', '2019-02-10', '2019-02-11') \
            and int(row['passenger_count']) < 10

print(f"Processing file {argv[1]} into {argv[2]}")
n=0
preserved=0
with (gzip.open(argv[1], 'rt') if argv[1].endswith('.gz') else open(argv[1], 'rt')) as fh:
    with (gzip.open(argv[2], 'wt',  newline='') if argv[2].endswith('.gz') else open(argv[2], 'wt',  newline='')) as outfh:
        csvreader = csv.DictReader(fh)
        csvwriter = csv.writer(outfh, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(COLS_TO_KEEP)
        with open("filter_dropped.tsv", 'wt') as errfh:
            for row in csvreader:
                n += 1
                if n % 1e6 == 0:
                    print(f"    {n/1e6}M lines")

                if not keep_row(row):
                    errfh.write('\t'.join(row.values()))
                    continue
                
                csvwriter.writerow([null_if_empty(row[f]) for f in COLS_TO_KEEP])
                preserved += 1
                
print(f"Kept {preserved} of {n} lines ({preserved/n:%})")