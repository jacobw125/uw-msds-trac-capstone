#!/usr/bin/env python3
import gzip
import csv
from sys import argv
import pandas as pd

if len(argv) < 2:
    raise ValueError("Missing input file")

if len(argv) < 3:
    raise ValueError("Missing output file")

INF = argv[1]  # or INF='filtered_avldata.tsv.gz'
OTF = argv[2]  # or OTF='avldata_route_agg.tsv'
print(f"Opening {INF}")
data = pd.read_csv(INF, sep='\t')

print(f"Performing aggregation")

data['stop_dt'] = pd.to_datetime(data['stop_datetime'])
data['is_am'] = data['stop_dt'].apply(lambda dt: dt.hour <= 12)
data['stops'] = 1
data['mean_door_open_sec'] = data['door_open_sec']
data['sum_door_open_sec'] = data['door_open_sec']
aggd = data.groupby(['rte', 'dir', 'opd_date', 'is_am']).agg({
    'stops': 'sum',
    'ons': 'sum',
    'offs': 'sum',
    'load': 'sum',
    'sum_door_open_sec': 'sum', 
    'mean_door_open_sec': 'mean', 
})

print(f"Produced an aggregate of shape {aggd.shape}")
print(f"Writing to {OTF}")
aggd.to_csv(OTF, sep='\t', index=True)