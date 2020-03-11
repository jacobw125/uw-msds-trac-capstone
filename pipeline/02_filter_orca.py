# pylint: disable=C0103
"""
Filters and splits the APC file into multiple .tsv.gz files, one file per day in the
study period that we wish to preserve. Also nulls out some values that are suspected to be
outliers.
"""

import gzip
import csv
from sys import argv
from math import inf
from collections import defaultdict
from os import makedirs
from typing import Dict
from datetime import datetime

IS_SUMMER=False

if len(argv) < 2:
    raise ValueError("Missing input file")

COLS_TO_KEEP = [
    'txn_id',
    'trip_group_id',
    'prev_leg_txn_id',
    'card_serial_number',
    'institution_id',
    'institution_name',
    'business_date',
    'txn_dtm_pacific',
    'txn_type_id',
    'txn_subtype_id',
    'txn_type_descr',
    'upgrade_indicator',
    'product_id',
    'product_descr',
    'txn_passenger_type_id',
    'txn_passenger_type_descr',
    'passenger_count',
    'ceffv',
    'service_agency_id',
    'service_agency_name',
    'source_agency_id',
    'source_agency_name',
    'transit_operator_abbrev',
    'mode_id',
    'mode_abbrev',
    'mode_descr',
    'route_number',
    'direction_id',
    'direction_descr',
    'agency_trip_id',
    'device_id',
    'device_type',
    'device_place_name',
    'device_place_id',
    'device_location_id',
    'device_location_code',
    'device_location_abbrev',
    'device_location_descr',
    'origin_location_id',
    'origin_location_code',
    'origin_location_abbrev',
    'origin_location_descr',
    'destination_location_id',
    'destination_location_code',
    'destination_location_abbrev',
    'destination_location_descr',
    'device_id_filt',
    'stop_id',
    'stop_time',
    'stop_lat',
    'stop_lon',
    'stop_error',
    'viaserviceareaid',
    'viaserviceareaname',
    'trip_id',
    'last_mode_id',
    'last_route_number',
    'last_stop_id',
    'last_stop_time',
    'last_stop_lat',
    'last_stop_lon'
]


def is_rapidride(row):
    try:
        return str(int(row['route_number']) > 600)
    except:
        return str(False) # can't parse as int

TXN_ID_TO_DESC = {
    '1': 'Adult',
    '2': 'Youth',
    '4': 'Disabled',
    '3': 'Senior',
    '5': 'Low Income'
}

COLS_TO_GENERATE = {
    # 'day_of_week': lambda row: str(datetime.strptime(row['business_date'], "%Y-%m-%d").weekday()),  # Monday is 0, Sunday is 6
    # 'is_rapidride': is_rapidride,
    'txn_dtm_pacific': lambda row: row['txn_dtm_pacific'][:-7] if row['txn_dtm_pacific'].endswith('.000000') else row['txn_dtm_pacific'],
    'biz_txn_diff': lambda row: str((
            datetime.strptime(row['business_date'], "%Y-%m-%d").date() - datetime.strptime(row['txn_dtm_pacific'], "%Y-%m-%d %H:%M:%S").date()
        ).days),
    'txn_passenger_type_descr': lambda row: TXN_ID_TO_DESC[row['txn_passenger_type_id']]
}

def null_if_empty(s): return None if s == '' else s

def keep_row(row):
    # bus, bus rapid transit, KCM only, remove snow days
    try:
        rte_num = int(row['route_number'])
    except:
        rte_num = -1  # can't parse as int, i.e. has characters in it, so ignore this filtering condition
    return str(row['mode_id']) in ('128', '250') and str(row['service_agency_id']) == '4' and rte_num > -1 and (rte_num < 600 or (rte_num >= 671 and rte_num <= 676))


days_to_keep = [f'2019-01-{day:02d}' for day in range(7,32)] +  \
               [f'2019-02-{day:02d}' for day in range(1,3)] +  \
               [f'2019-02-{day:02d}' for day in range(13, 29)] + \
               [f'2019-03-{day:02d}' for day in range(1, 4)]

if IS_SUMMER:
    days_to_keep = [f'2019-07-{day:02d}' for day in range(1, 32)]  + \
                   [f'2019-08-{day:02d}' for day in range(1, 32)]

COLS_IN_OUTFILE = list(set(COLS_TO_KEEP + list(COLS_TO_GENERATE.keys())))

print(f"Processing file {argv[1]} into data/orca/*.tsv.gz")
makedirs('data/orca', exist_ok=True)
n, preserved = 0, 0
try:
    file_handles = { day: gzip.open(f"data/orca/{day}.tsv.gz", 'wt', newline='') for day in days_to_keep }
    file_writers = { day: csv.writer(fh, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL) for day, fh in file_handles.items() }
    for writer in file_writers.values():
        writer.writerow(COLS_IN_OUTFILE)

    with (gzip.open(argv[1], 'rt') if argv[1].endswith('.gz') else open(argv[1], 'rt')) as fh:
        csvreader = csv.DictReader(fh)
        for row in csvreader:
            n += 1
            if n % 1e6 == 0:
                print(f"    {n/1e6}M lines")

            if not row['business_date'] in days_to_keep: continue

            for colname, generator_function in COLS_TO_GENERATE.items():
                row[colname] = generator_function(row)

            if not keep_row(row): continue

            this_tsv = file_writers[row['business_date']]
            this_tsv.writerow([null_if_empty(row[f]) for f in COLS_IN_OUTFILE])
            preserved += 1

finally:
    for fh in file_handles.values():
        fh.close()

print(f"Kept {preserved} of {n} lines ({preserved/n:%})")
