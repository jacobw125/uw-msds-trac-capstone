# pylint: disable=C0103
"""
Filters and splits the APC file into multiple .tsv.gz files, one file per day in the
study period that we wish to preserve. Also nulls out some values that are suspected to be
outliers.
"""

import gzip
import csv
from sys import argv
from os import makedirs
from datetime import datetime

# Checks for input argument
if len(argv) < 3:
    raise ValueError("Missing input parameter. Needs APC file and boolean.")

# Define seasonal constants
IS_SUMMER = argv[2] == "True"

DAYS_TO_KEEP = [f'2019-01-{day:02d}' for day in range(7, 32)] + \
               [f'2019-02-{day:02d}' for day in range(1, 3)] + \
               [f'2019-02-{day:02d}' for day in range(13, 29)] + \
               [f'2019-03-{day:02d}' for day in range(1, 4)]

if IS_SUMMER:
    DAYS_TO_KEEP = [f'2019-07-{day:02d}' for day in range(1, 32)] + \
                   [f'2019-08-{day:02d}' for day in range(1, 32)]

COLS_TO_KEEP = [
    'daycode',
    'trip_id',
    'pattern_id',
    'pattern_quality',
    'blk',
    'rte',
    'dir',
    'sch_st_min',
    'opd_date',
    'pattern_quality_1',
    'vehicle_id',
    'stop_id',
    'stop_seq',
    'stop_name',
    'sch_stop_sec',
    'act_stop_arr',
    'sch_stop_tm',
    'act_stop_tm',
    'dwell_sec',
    'doors_open',
    'door_open_sec',
    'apc_veh',
    'ons',
    'offs',
    'load',
    'geom',
    'sch_stop_tm',
    'act_stop_tm',
    'stop_datetime',
    'gps_lat',
    'gps_long'
]

def is_rapidride(data_row):
    """
    Determines if route is a rapid ride.
    :params dictionary row with 'rte' key:
    :returns boolean:
    """
    result = False

    try:
        result = int(data_row['rte']) > 600
    except TypeError:
        # Unable to parse route as int
        pass

    return str(result)

def int_or_zero(x):
    """
    Tries to convert input to an int, else 0.
    :params variable:
    :returns int:
    """
    result = 0

    try:
        result = int(x)
    except TypeError:
        pass

    return result

def null_if_empty(x):
    """
    Sets empty variables as NULL.
    :params variable:
    :returns variable:
    """
    result = x

    if x == '':
        result = None

    return result

def keep_row(data_row):
    """
    Determines if both apc_vehicle and valid route number (<600 or 671-676)
    :params dictionary row with 'rte' and 'apc_veh' keys:
    :returns boolean:
    """
    rte_num = -1

    try:
        rte_num = int(data_row['rte'])
    except TypeError:
        pass

    return data_row['apc_veh'] == 'Y' and (rte_num < 600 or (671 <= rte_num <= 676))

print(f"Processing file {argv[1]} into data/apc/*.tsv.gz")
makedirs('data/apc', exist_ok=True)

COLS_TO_GENERATE = {
    # Monday is 0, Sunday is 6
    'day_of_week': lambda row: \
                   str(datetime.strptime(row['opd_date'], "%Y-%m-%d").weekday()),
    'is_rapidride': is_rapidride
}

COLS_IN_OUTFILE = list(set(COLS_TO_KEEP + list(COLS_TO_GENERATE.keys())))

N, PRESERVED = 0, 0
try:
    FILE_HANDLES = {day: gzip.open(f"data/apc/{day}.tsv.gz", \
                                   'wt', newline='') for day in DAYS_TO_KEEP}
    FILE_WRITERS = {day: csv.writer(fh, delimiter='\t', quotechar='"', \
                                    quoting=csv.QUOTE_MINIMAL) for day, fh in FILE_HANDLES.items()}

    for writer in FILE_WRITERS.values():
        writer.writerow(COLS_IN_OUTFILE)

    with (gzip.open(argv[1], 'rt') if argv[1].endswith('.gz') else open(argv[1], 'rt')) as fh:
        CSV_READER = csv.DictReader(fh)
        for row in CSV_READER:

            N += 1
            if N % 1e6 == 0:
                print(f"    {N/1e6}M lines")

            if not row['opd_date'] in DAYS_TO_KEEP:
                continue

            for colname, generator_function in COLS_TO_GENERATE.items():
                row[colname] = generator_function(row)

            if not keep_row(row):
                continue

            this_tsv = FILE_WRITERS[row['opd_date']]
            this_tsv.writerow([null_if_empty(row[f]) for f in COLS_IN_OUTFILE])
            PRESERVED += 1

finally:
    for fh in FILE_HANDLES.values():
        fh.close()

print(f"Kept {PRESERVED} of {N} lines ({PRESERVED/N:%})")
