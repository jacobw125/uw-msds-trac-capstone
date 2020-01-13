#!/usr/bin/env python3
# Merge the APC/AVL file with the aggregated Orca file

import pandas as pd
from multiprocessing import Pool
from os.path import basename

days_to_keep = [f'2019-01-{day:02d}' for day in range(7,32)] +  \
               [f'2019-02-{day:02d}' for day in range(1,3)] +  \
               [f'2019-02-{day:02d}' for day in range(13, 29)] + \
               [f'2019-03-{day:02d}' for day in range(1, 3)]


cols_to_fillna = ['orca_total', 'orca_adult', 'orca_disabled', 'orca_senior', 'orca_youth', 'orca_lowincome', 'orca_uw']
desired_column_order = [
    'opd_date',
    'stop_datetime',   # apc
    'opd_txn_diff',    # apc
    'txn_dtm_pacific', # orca
    'biz_txn_diff',    # orca
    'day_of_week',
    'trip_id', 
    'stop_id',
    'vehicle_id',
    'rte', 
    'is_rapidride',
    'dir',
    'direction_descr',
    'stop_seq',
    'stop_name',
    'sch_stop_tm',
    'act_stop_tm', 
    'stop_datetime',
    'doors_open',
    'door_open_sec',
    'dwell_sec',
    'ons',
    'offs', 
    'load',
    'orca_total', 'orca_adult', 'orca_disabled', 'orca_senior', 'orca_youth', 'orca_lowincome', 'orca_uw'
]
sort_by = ['trip_id', 'stop_seq']
def process_file(day):
    print(f"Merging {day}")
    apc = pd.read_csv(f"data/apc/{day}.tsv.gz", sep='\t', dtype={'trip_id': 'O', 'stop_id': 'O', 'rte': 'O'}).drop(columns=['apc_veh'])
    orca = pd.read_csv(f"data/orca_agg/{day}.tsv.gz", sep='\t', dtype={'trip_id': 'O', 'stop_id': 'O', 'route_number': 'O'})
    # There is a lot of data 'missing' in the orca table. Sometimes entire trips. But they look like realistic cases where 
    # the bus either didn't stop, or didn't pick anyone up
    # missing_orca = data[data['_merge']=='left_only']
    # > (missing_orca.ons ==0).mean()
    # 0.8671458762520423
    # > missing_orca = data[(data['_merge']=='left_only') & (data['doors_open']=='Y') & (data['ons'] > 0)]
    # 2.5555095991452106
    data = pd.merge(
        apc, 
        orca, 
        left_on=['trip_id', 'stop_id'],
        right_on=['trip_id', 'stop_id'],
        how='inner',
        suffixes=('_apc', '_orca')
    ).drop(columns=['business_date', 'route_number', 'sch_st_min', 'sch_stop_sec', 'act_stop_arr',])[desired_column_order].sort_values(sort_by).rename(
        columns={'stop_datetime': 'apc_stop_dt', 'opd_txn_diff': 'apc_txn_dt_diff', 'txn_dtm_pacific': 'orca_txn_dtm_pac', 'biz_txn_diff': 'orca_txn_dt_diff'}
    )
    data[cols_to_fillna] = data[cols_to_fillna].fillna(value=0)
    return data

with Pool(4) as p:
    data = pd.concat(p.map(process_file, days_to_keep))

#(2567075, 30)
data.to_csv('data/merged_stops.tsv.gz', sep='\t', index=False)


# Roll-up, summing over all stops in the trip_id
data.groupby([
    'opd_date',
    'day_of_week',
    'trip_id', 
    'vehicle_id',
    'rte', 
    'is_rapidride',
    'dir',
    'direction_descr'
]).agg({
    'ons': 'sum',
    'offs': 'sum', 
    'orca_total': 'sum', 
    'orca_adult': 'sum', 
    'orca_disabled': 'sum', 
    'orca_senior': 'sum', 
    'orca_youth': 'sum', 
    'orca_lowincome': 'sum', 
    'orca_uw': 'sum'
}).to_csv('data/trip_id_rollups.tsv.gz', sep='\t', index=True)