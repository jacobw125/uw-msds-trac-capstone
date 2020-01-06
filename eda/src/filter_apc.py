#!/usr/bin/env python3
import gzip
import csv
from sys import argv
from math import inf
from collections import defaultdict
from os import makedirs
from typing import Dict
from datetime import datetime

if len(argv) < 2:
    raise ValueError("Missing input file")

if len(argv) < 3:
    raise ValueError("Missing output file")

COLS_TO_KEEP = [
    'opd_date',
    'stop_datetime',
    'rte',
    'trip_id',
    #'pattern_id',
    #'blk',
    'vehicle_id',
    'stop_id',
    'stop_name',
    'stop_seq',
    #'geom'
    'dir',
    'apc_veh',
    'sch_st_min',
    #'pattern_quality',
    #'pattern_quality_1',
    'sch_stop_sec',
    'act_stop_arr',
    'dwell_sec',
    'doors_open',
    'door_open_sec',
    'ons',
    'offs',
    'load',
    #'gps_lat',
    #'gps_long'
]

COLS_TO_GENERATE = {
    'day_of_week': lambda row: str(datetime.strptime(row['opd_date'], "%Y-%m-%d").weekday())  # Monday is 0, Sunday is 6
}

def null_if_empty(s): return None if s == '' else s

def keep_row(row):
    return str(row['opd_date']) not in ('2019-02-03', '2019-02-04', '2019-02-08', '2019-02-09', '2019-02-10', '2019-02-11') \
            and float(row['dwell_sec']) < 2000 \
            and float(row['door_open_sec']) >= 0 \
            and float(row['door_open_sec']) < 2000 \
            and row['apc_veh'] == 'Y' \
            and int(row['ons']) < 150 \
            and int(row['offs']) < 150

print(f"Processing file {argv[1]} into {argv[2]}")
n=0
preserved=0
with (gzip.open(argv[1], 'rt') if argv[1].endswith('.gz') else open(argv[1], 'rt')) as fh:
    with (gzip.open(argv[2], 'wt',  newline='') if argv[2].endswith('.gz') else open(argv[2], 'wt',  newline='')) as outfh:
        csvreader = csv.DictReader(fh)
        csvwriter = csv.writer(outfh, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(COLS_TO_KEEP + list(COLS_TO_GENERATE.keys()))
        with open("apc_filter_dropped.tsv", 'wt') as errfh:
            for row in csvreader:
                n += 1
                if n % 1e6 == 0:
                    print(f"    {n/1e6}M lines")

                for colname, generator_function in COLS_TO_GENERATE.items():
                    row[colname] = generator_function(row)

                if not keep_row(row):
                    errfh.write('\t'.join(row.values()))
                    continue
                
                csvwriter.writerow([null_if_empty(row[f]) for f in ( COLS_TO_KEEP + list(COLS_TO_GENERATE.keys()) )])
                preserved += 1
                
print(f"Kept {preserved} of {n} lines ({preserved/n:%})")