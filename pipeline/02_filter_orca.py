# pylint: disable=C0103
"""
Filters and splits the ORCA file into multiple .tsv.gz files, one file per day in the
study period that we wish to preserve.

Fall: Kept 9788082 of 27878358 lines (35.109966%)
Summer: Kept 11018674 of 11025382 lines (99.939159%)
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
    raise ValueError('Missing input parameter. Needs ORCA file \
                     and whether Summer data (True/False).')

# define seasonal constants
IS_SUMMER = argv[2] == 'True'

DAYS_TO_KEEP = c.WINTER_DAYS
COLS_TO_KEEP = c.ORCA_WINTER_COLUMNS

if IS_SUMMER:
    DAYS_TO_KEEP = c.SUMMER_DAYS
    COLS_TO_KEEP = c.ORCA_SUMMER_COLUMNS

# define additional freatures
COLS_TO_GENERATE = {
    c.ORCA_TDP: lambda row: row[c.ORCA_TDP][:-7] \
                       if row[c.ORCA_TDP].endswith('.000000') \
                       else row[c.ORCA_TDP],
    c.ORCA_BTD: lambda row: str((
        datetime.strptime(row[c.ORCA_DATE], '%Y-%m-%d').date() - \
        datetime.strptime(row[c.ORCA_TDP], '%Y-%m-%d %H:%M:%S').date()).days),
    c.ORCA_TPTD: lambda row: c.TXN_ID_TO_DESC[row[c.ORCA_TPTID]]
}

COLS_IN_OUTFILE = list(set(COLS_TO_KEEP + list(COLS_TO_GENERATE.keys())))

# print status update and make directory for date files
print(f'Processing file {argv[1]} into {c.ORCA_DIR}/*.tsv.gz')
makedirs(c.ORCA_DIR, exist_ok=True)

N, PRESERVED = 0, 0
try:
    # makes a file .tsv.gz file for each day with all features as columns
    FILE_HANDLES = {day: gzip.open(f'{c.ORCA_DIR}/{day}.tsv.gz', \
                                   'wt', newline='') for day in DAYS_TO_KEEP}
    FILE_WRITERS = {day: csv.writer(fh, delimiter='\t', quotechar='"', \
                                    quoting=csv.QUOTE_MINIMAL) for day, fh in FILE_HANDLES.items()}
    for writer in FILE_WRITERS.values():
        writer.writerow(COLS_IN_OUTFILE)

    with (gzip.open(argv[1], 'rt') if argv[1].endswith('.gz') else open(argv[1], 'rt')) as fh:
        CSV_READER = csv.DictReader(fh)
        for row in CSV_READER:

            N += 1
            # iterates through rows in ORCA data
            if N % 1e6 == 0:
                PERCENT_PRESEVERED = '{:.1f}'.format(PRESERVED/1e6)
                print(f'   {PERCENT_PRESEVERED}M lines of {N/1e6}M lines')

            # checks if day is in keep list (ex: ignore days were weather was bad)
            if not row[c.ORCA_DATE] in DAYS_TO_KEEP:
                continue

            # generates the new features
            for colname, generator_function in COLS_TO_GENERATE.items():
                row[colname] = generator_function(row)

            # determines if row should be kept based on route number, mode, and agency
            if not func.keep_row(row, c.ORCA_RTE, c.ORCA_MODE, c.ORCA_SERVICE_AGENCY_ID, 'ORCA'):
                continue

            # writes all rows from 1 day to same file
            this_tsv = FILE_WRITERS[row[c.ORCA_DATE]]
            this_tsv.writerow([func.null_if_empty(row[f]) for f in COLS_IN_OUTFILE])
            PRESERVED += 1

finally:
    for fh in FILE_HANDLES.values():
        fh.close()

print(f'Kept {PRESERVED} of {N} lines ({PRESERVED/N:%})')
