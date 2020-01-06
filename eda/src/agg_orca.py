#!/usr/bin/env python3
import gzip
import csv
from sys import argv
import pandas as pd
import numpy as np

if len(argv) < 2:
    raise ValueError("Missing input file")

if len(argv) < 3:
    raise ValueError("Missing output file")

INF = argv[1]  # or INF='encrypted/filtered-orca-transactions.tsv.gz'
OTF = argv[2]  # or OTF='orca_route_agg.tsv'
print(f"Opening {INF}")
data = pd.read_csv(INF, sep='\t')

print(f"Performing aggregation")

data['txn_dt'] = pd.to_datetime(data['txn_dtm_pacific'])
data['txn_date'] = data['txn_dt'].apply(lambda d: d.strftime("%Y-%m-%d"))
data['stops'] = data['stop_id']
data['is_am'] = data['txn_dt'].apply(lambda dt: dt.hour <= 12)

data['orca_t'] =  1
data['adult_t'] = (data['txn_passenger_type_descr'] == 'Adult')*1
data['disabled_t'] = (data['txn_passenger_type_descr'] == 'Disabled')*1
data['senior_t'] = (data['txn_passenger_type_descr'] == 'Senior')*1
data['youth_t'] = (data['txn_passenger_type_descr'] == 'Youth')*1
data['lowincome_t'] = (data['txn_passenger_type_descr'] == 'Low Income')*1

data['orca_c'] = data['passenger_count']
data['adult_c'] = data['passenger_count']
data['disabled_c'] = data['passenger_count']
data['senior_c'] = data['passenger_count']
data['youth_c'] = data['passenger_count']
data['lowincome_c'] = data['passenger_count']

data.adult_c[data['txn_passenger_type_descr'] != 'Adult'] = 0
data.disabled_c[data['txn_passenger_type_descr'] != 'Disabled'] = 0
data.senior_c[data['txn_passenger_type_descr'] != 'Senior'] = 0
data.youth_c[data['txn_passenger_type_descr'] != 'Youth'] = 0
data.lowincome_c[data['txn_passenger_type_descr'] != 'Low Income'] = 0

aggd = data.groupby(['route_number', 'direction_descr', 'txn_date', 'is_am', 'day_of_week']).agg({
    'stops': 'nunique',
    'orca_t': 'sum',
    'adult_t': 'sum',
    'disabled_t': 'sum',
    'senior_t': 'sum',
    'youth_t': 'sum',
    'lowincome_t': 'sum',
    'orca_c': 'sum',
    'adult_c': 'sum',
    'disabled_c': 'sum',
    'senior_c': 'sum',
    'youth_c': 'sum',
    'lowincome_c': 'sum'
})

print(f"Produced an aggregate of shape {aggd.shape}")
print(f"Writing to {OTF}")
aggd.to_csv(OTF, sep='\t', index=True)