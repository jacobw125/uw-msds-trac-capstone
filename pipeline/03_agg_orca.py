#!/usr/bin/env python3
# Remember, each line in the APC file represents one stop. But each line in the Orca file represents a transaction!
# This script 'rolls up' the Orca file so that each line represents a stop.

import pandas as pd
from multiprocessing import Pool
from os.path import basename
from os import makedirs

IS_SUMMER=False

days_to_keep = [f'2019-01-{day:02d}' for day in range(7,32)] +  \
               [f'2019-02-{day:02d}' for day in range(1,3)] +  \
               [f'2019-02-{day:02d}' for day in range(13, 29)] + \
               [f'2019-03-{day:02d}' for day in range(1, 4)]

if IS_SUMMER:
    days_to_keep = [f'2019-07-{day:02d}' for day in range(1, 32)]  + \
                   [f'2019-08-{day:02d}' for day in range(1, 32)]

files = [f'data/orca/{day}.tsv.gz' for day in days_to_keep]
makedirs('data/orca_agg', exist_ok=True)

def process_file(fname):
    print(f"Aggregating {fname}")
    data = pd.read_csv(fname, sep='\t', dtype={'trip_id': 'O', 'stop_id': 'O', 'route_number': 'O'}, low_memory=False)
    n_dups = sum(data.duplicated())

    if n_dups > 0:
        print(f"Found {n_dups} Orca duplicates on day {fname}")
        data = data.drop_duplicates()
    data = data[(data['trip_id'].isna() == False) & (data['stop_id'].isna() == False)]
    data = data[['business_date', 'trip_id', 'stop_id', 'txn_dtm_pacific', 'biz_txn_diff',
    'txn_passenger_type_descr', 'institution_name', 'passenger_count']]
    data['orca_total'] = data['passenger_count']
    data['orca_adult'] = data['passenger_count'].where(data['txn_passenger_type_descr'] == 'Adult', 0)
    data['orca_disabled'] = data['passenger_count'].where(data['txn_passenger_type_descr'] == 'Disabled', 0)
    data['orca_senior'] = data['passenger_count'].where(data['txn_passenger_type_descr'] == 'Senior', 0)
    data['orca_youth'] = data['passenger_count'].where(data['txn_passenger_type_descr'] == 'Youth', 0)
    data['orca_lowincome'] = data['passenger_count'].where(data['txn_passenger_type_descr'] == 'Low Income', 0)
    data['orca_uw'] = data['passenger_count'].where(data['institution_name'] == 'University of Washington', 0)
    data.drop(['txn_passenger_type_descr', 'institution_name', 'passenger_count'], axis = 1)
    data.groupby(['business_date', 'trip_id', 'stop_id']).agg({
        'txn_dtm_pacific': 'first',
        'biz_txn_diff': 'first',
        'orca_total': 'sum',
        'orca_adult': 'sum',
        'orca_disabled': 'sum',
        'orca_senior': 'sum',
        'orca_youth': 'sum',
        'orca_lowincome': 'sum',
        'orca_uw': 'sum'
    }).to_csv(f"data/orca_agg/{basename(fname)}", index=True, sep='\t')

with Pool(1) as p:
    p.map(process_file, files)
