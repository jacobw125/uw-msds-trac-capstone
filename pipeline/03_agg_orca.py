# pylint: disable=C0103,C0121
"""
Remember, each line in the APC file represents one stop. But each line in the
Orca file represents a transaction! This script 'rolls up' the Orca file so
that each line represents a stop.

Fall: Kept 9788082 of 27878358 lines (35.109966%)
Summer: Kept 11018674 of 11025382 lines (99.939159%)
"""

from sys import argv
from os import makedirs
from os.path import basename
from multiprocessing import Pool
import pandas as pd

import constants as c

# checks for input argument
if len(argv) < 2:
    raise ValueError('Missing input parameter. Needs if summer data (True/False).')

# define seasonal constants
IS_SUMMER = argv[1] == 'True'

DAYS_TO_KEEP = c.WINTER_DAYS

if IS_SUMMER:
    DAYS_TO_KEEP = c.SUMMER_DAYS

FILES = [f'{c.ORCA_DIR}/{day}.tsv.gz' for day in DAYS_TO_KEEP]
makedirs(c.ORCA_DIR_AGG, exist_ok=True)

def process_file(fname):
    """
    Reads orca data day csv, deduplicates rows, adds features, and aggregates over trip, stop,
    and day. Saves the aggregated data to another file.

    :params fname string:
    """

    # reads orca file
    print(f'Aggregating {fname}')
    data = pd.read_csv(fname, sep='\t',
                       dtype={c.TRIP_ID: 'O', c.STOP_ID: 'O', c.ORCA_RTE: 'O'},
                       low_memory=False)

    # finds duplicates and removes them if they exist
    n_dups = sum(data.duplicated())
    if n_dups > 0:
        print(f'Found {n_dups} Orca duplicates on day {fname}')
        data = data.drop_duplicates()

    # filters out incomplete data, where trip or stop is missing
    data = data[(data[c.TRIP_ID].isna() == False) & (data[c.STOP_ID].isna() == False)]

    # adds demographic features
    data = data[[c.ORCA_DATE, c.TRIP_ID, c.STOP_ID, c.ORCA_TDP,
                 c.ORCA_BTD, c.ORCA_TPTD, c.ORCA_IN, c.ORCA_PC]]
    data[c.ORCA_TOTAL] = data[c.ORCA_PC]
    data[c.ORCA_ADULT] = data[c.ORCA_PC].where(data[c.ORCA_TPTD] == c.TXN_ID_TO_DESC['1'], 0)
    data[c.ORCA_YOUTH] = data[c.ORCA_PC].where(data[c.ORCA_TPTD] == c.TXN_ID_TO_DESC['2'], 0)
    data[c.ORCA_SENIOR] = data[c.ORCA_PC].where(data[c.ORCA_TPTD] == c.TXN_ID_TO_DESC['3'], 0)
    data[c.ORCA_DISABLED] = data[c.ORCA_PC].where(data[c.ORCA_TPTD] == c.TXN_ID_TO_DESC['4'], 0)
    data[c.ORCA_LOWINCOME] = data[c.ORCA_PC].where(data[c.ORCA_TPTD] == c.TXN_ID_TO_DESC['5'], 0)
    data[c.ORCA_UW] = data[c.ORCA_PC].where(data[c.ORCA_IN] == 'University of Washington', 0)
    data.drop([c.ORCA_TPTD, c.ORCA_IN, c.ORCA_PC], axis=1)

    # aggregates counts over trip, stop, and day and saves to file
    data.groupby([c.ORCA_DATE, c.TRIP_ID,
                  c.STOP_ID]).agg({c.ORCA_TDP: 'first', c.ORCA_BTD: 'first',
                                   c.ORCA_TOTAL: 'sum', c.ORCA_ADULT: 'sum',
                                   c.ORCA_DISABLED: 'sum', c.ORCA_SENIOR: 'sum',
                                   c.ORCA_YOUTH: 'sum', c.ORCA_LOWINCOME: 'sum',
                                   c.ORCA_UW: 'sum'}).to_csv( \
                                   f'{c.ORCA_DIR_AGG}/{basename(fname)}', index=True, sep='\t')

# parallelize file processing
with Pool(c.POOL) as p:
    p.map(process_file, FILES)
