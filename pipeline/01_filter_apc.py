# pylint: disable=C0103
"""
Filters and splits the APC file into multiple .tsv.gz files, one file per day in the
study period that we wish to preserve. Also nulls out some values that are suspected to be
outliers.

Fall: Kept 11737145 of 20535948 lines (57.154143%)
Summer: Kept 8368494 of 12201946 lines (68.583274%)
"""

import gzip
import csv
from sys import argv
from os import makedirs
from datetime import datetime

import constants as c
import functions as func

# checks for input argument
if len(argv) < 3:
    raise ValueError('Missing input parameter. Needs APC file and \
                     whether Summer data (True/False).')

# define seasonal constants
IS_SUMMER = argv[2] == 'True'

DAYS_TO_KEEP = c.WINTER_DAYS

if IS_SUMMER:
    DAYS_TO_KEEP = c.SUMMER_DAYS

COLS_TO_KEEP = c.APC_COLUMNS

# define additional freatures
COLS_TO_GENERATE = {
    # Monday is 0, Sunday is 6
    c.DAY_OF_WEEK: lambda row: \
                   str(datetime.strptime(row[c.APC_DATE], '%Y-%m-%d').weekday()),
    c.APC_RR: lambda row: func.is_rapidride(row[c.APC_RTE])
}

COLS_IN_OUTFILE = list(set(COLS_TO_KEEP + list(COLS_TO_GENERATE.keys())))

# print status update and make directory for date files
print(f'Processing file {argv[1]} into {c.APC_DIR}/*.tsv.gz')
makedirs(c.APC_DIR, exist_ok=True)

N, PRESERVED = 0, 0
try:
    # makes a file .tsv.gz file for each day with all features as columns
    FILE_HANDLES = {day: gzip.open(f'{c.APC_DIR}/{day}.tsv.gz', \
                                   'wt', newline='') for day in DAYS_TO_KEEP}
    FILE_WRITERS = {day: csv.writer(fh, delimiter='\t', quotechar='"', \
                                    quoting=csv.QUOTE_MINIMAL) for day, fh in FILE_HANDLES.items()}

    for writer in FILE_WRITERS.values():
        writer.writerow(COLS_IN_OUTFILE)

    # iterates through rows in APC data
    with (gzip.open(argv[1], 'rt') if argv[1].endswith('.gz') else open(argv[1], 'rt')) as fh:
        CSV_READER = csv.DictReader(fh)
        for row in CSV_READER:

            N += 1
            # prints progress
            if N % 1e6 == 0:
                PERCENT_PRESEVERED = '{:.1f}'.format(PRESERVED/1e6)
                print(f'   {PERCENT_PRESEVERED}M lines of {N/1e6}M lines')

            # checks if day is in keep list (ex: ignore days were weather was bad)
            if not row[c.APC_DATE] in DAYS_TO_KEEP:
                continue

            # generates the new features
            for colname, generator_function in COLS_TO_GENERATE.items():
                row[colname] = generator_function(row)

            # determines if row should be kept based on route number and if apc vehicle
            if not func.keep_row(row, c.APC_RTE, c.APC_VEH, None, 'APC'):
                continue

            # writes all rows from 1 day to same file
            this_tsv = FILE_WRITERS[row[c.APC_DATE]]
            this_tsv.writerow([func.null_if_empty(row[f]) for f in COLS_IN_OUTFILE])
            PRESERVED += 1

finally:
    for fh in FILE_HANDLES.values():
        fh.close()

print(f'Kept {PRESERVED} of {N} lines ({PRESERVED/N:%})')
