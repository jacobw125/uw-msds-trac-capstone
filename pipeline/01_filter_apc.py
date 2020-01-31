#!/usr/bin/env python3
# Filters and splits the APC file into multiple .tsv.gz files, one file per day in the 
# study period that we wish to preserve. Also nulls out some values that are suspected to be
# outliers.

# Kept 11,592,345 of 20,535,948 lines (56.449038%)

import gzip
import csv
from sys import argv
from math import inf
from collections import defaultdict
from os import makedirs
from typing import Dict
from datetime import datetime

IS_SUMMER = True

if len(argv) < 2:
    raise ValueError("Missing input file")

COLS_TO_KEEP = [
    #'daycode',
    'opd_date',
    'stop_datetime',
    'sch_stop_tm',
    'act_stop_tm',
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


def is_rapidride(row):
    try:
        return str(int(row['rte']) > 600)
    except:
        return str(False) # can't parse as int

def int_or_zero(x):
    try:
        return int(x)
    except:
        return 0

COLS_TO_GENERATE = {
    'day_of_week': lambda row: str(datetime.strptime(row['opd_date'], "%Y-%m-%d").weekday()),  # Monday is 0, Sunday is 6
    'is_rapidride': is_rapidride,
    'door_open_sec': lambda row: None if int_or_zero(row['door_open_sec']) < 0 or int_or_zero(row['door_open_sec']) > 2000 else row['door_open_sec'],
    'dwell_sec': lambda row: None if int_or_zero(row['dwell_sec']) < 0 or int_or_zero(row['dwell_sec']) > 2000 else row['dwell_sec'],
    'ons': lambda row: row['ons'] if int_or_zero(row['ons']) < 150 else None,
    'offs': lambda row: row['offs'] if int_or_zero(row['offs']) < 150 else None,
    'opd_txn_diff': lambda row: str((
            datetime.strptime(row['opd_date'], "%Y-%m-%d").date() - datetime.strptime(row['stop_datetime'], "%Y-%m-%d %H:%M:%S").date()
        ).days)
}

def null_if_empty(s): return None if s == '' else s

def keep_row(row):
    try:
        rte_num = int(row['rte'])
    except:
        rte_num = -1
    return row['apc_veh'] == 'Y' and (rte_num < 600 or (rte_num >= 671 and rte_num <= 676))

print(f"Processing file {argv[1]} into data/apc/*.tsv.gz")
makedirs('data/apc', exist_ok=True)

days_to_keep = [f'2019-01-{day:02d}' for day in range(7,32)] +  \
               [f'2019-02-{day:02d}' for day in range(1,3)] +  \
               [f'2019-02-{day:02d}' for day in range(13, 29)] + \
               [f'2019-03-{day:02d}' for day in range(1, 4)]

if IS_SUMMER:
    days_to_keep = [f'2019-07-{day:02d}' for day in range(1, 32)]  + \
                   [f'2019-08-{day:02d}' for day in range(1, 32)]

COLS_IN_OUTFILE = list(set(COLS_TO_KEEP + list(COLS_TO_GENERATE.keys())))

n, preserved = 0, 0
try: 
    file_handles = { day: gzip.open(f"data/apc/{day}.tsv.gz", 'wt', newline='') for day in days_to_keep }
    file_writers = { day: csv.writer(fh, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL) for day, fh in file_handles.items() }
    for writer in file_writers.values():
        writer.writerow(COLS_IN_OUTFILE)

    with (gzip.open(argv[1], 'rt') if argv[1].endswith('.gz') else open(argv[1], 'rt')) as fh:
        csvreader = csv.DictReader(fh)
        for row in csvreader:
            n += 1
            if n % 1e6 == 0:
                print(f"    {n/1e6}M lines")
            
            if not row['opd_date'] in days_to_keep: continue
            
            for colname, generator_function in COLS_TO_GENERATE.items():
                row[colname] = generator_function(row)

            if not keep_row(row): continue

            this_tsv = file_writers[row['opd_date']]
            this_tsv.writerow([null_if_empty(row[f]) for f in COLS_IN_OUTFILE])
            preserved += 1

finally:
    for fh in file_handles.values():
        fh.close()
                
print(f"Kept {preserved} of {n} lines ({preserved/n:%})")