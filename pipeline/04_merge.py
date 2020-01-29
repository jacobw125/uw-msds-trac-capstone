#!/usr/bin/env python3
# Merge the APC/AVL file with the aggregated Orca file

import pandas as pd
from multiprocessing import Pool
from os.path import basename

days_to_keep = [f'2019-01-{day:02d}' for day in range(7,32)] +  \
               [f'2019-02-{day:02d}' for day in range(1,3)] +  \
               [f'2019-02-{day:02d}' for day in range(13, 29)] + \
               [f'2019-03-{day:02d}' for day in range(1, 4)]


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
    n_dups = sum(apc.duplicated())
    if n_dups > 0:
        print(f"Found {n_dups} APC duplicates on day {day}")
        apc = apc.drop_duplicates()
    orca = pd.read_csv(f"data/orca_agg/{day}.tsv.gz", sep='\t', dtype={'trip_id': 'O', 'stop_id': 'O', 'route_number': 'O'})
    n_dups = sum(orca.duplicated())
    if n_dups > 0:
        print(f"Found {n_dups} Orca duplicates on day {day}")
        orca = orca.drop_duplicates()
    data = pd.merge(
        apc, 
        orca, 
        left_on=['trip_id', 'stop_id'],
        right_on=['trip_id', 'stop_id'],
        how='left',
        suffixes=('_apc', '_orca')
    )
    data = data.drop(columns=['business_date', 'route_number', 'sch_st_min', 'sch_stop_sec', 'act_stop_arr'])[desired_column_order].sort_values(sort_by)
    data = data.rename(columns={
        'stop_datetime': 'apc_stop_dt', 
        'opd_txn_diff': 'apc_txn_dt_diff', 
        'txn_dtm_pacific': 'orca_txn_dtm_pac', 
        'biz_txn_diff': 'orca_txn_dt_diff'
        })
    data[cols_to_fillna] = data[cols_to_fillna].fillna(value=0)
    return data

with Pool(4) as p:
    data = pd.concat(p.map(process_file, days_to_keep))

print(len(data))
# 11,740,098 rows
data['orca_total'].sum() / data['ons'].sum()
# Orca is 56.6% of apc's total

# For the following query, Maggie expects there to be one row, ons = 15 and orca_total = 4
data[(data['trip_id'] == '40684352') & (data['opd_date'] == '2019-03-01') & (data['stop_id'] == '1180')].T
# orca total is 4. 1 adult 1 disabled 2 senior
# trip_id                           40684352
# stop_id                               1180
# rte                                     49
# dir                                      N
# direction_descr                      Outbd
# stop_seq                                 0
# stop_name         PIKE ST & 4TH AVE (1180)
# ons                                     15
# offs                                     0
# orca_total                               4
# orca_adult                               1
# orca_disabled                            1
# orca_senior                              2

# Confirm that in the agg'd orca data
orca2 = pd.read_csv(f"data/orca_agg/2019-03-01.tsv.gz", sep='\t', dtype={'trip_id': 'O', 'stop_id': 'O', 'route_number': 'O'})
orca2[(orca2['trip_id'] == '40684352') & (orca2['stop_id'] == '1180')].T
# business_date             2019-03-01
# trip_id                     40684352
# stop_id                         1180
# orca_total                         4
# orca_adult                         1
# orca_disabled                      1
# orca_senior                        2

# Confirm in the raw orca data
orca3 = pd.read_csv(f"data/orca/2019-03-01.tsv.gz", sep='\t', dtype={'trip_id': 'O', 'stop_id': 'O', 'route_number': 'O'})
orca3[(orca3['trip_id'] == '40684352') & (orca3['stop_id'] == '1180')][['passenger_count', 'stop_id', 'trip_id', 'txn_passenger_type_descr']]
#         passenger_count stop_id   trip_id txn_passenger_type_descr
# 145850                1    1180  40684352                 Disabled
# 153466                1    1180  40684352                    Adult
# 161685                1    1180  40684352                   Senior
# 248018                1    1180  40684352                   Senior


# Nice. Save that data.
data.to_csv('data/merged_stops.tsv.gz', sep='\t', index=False)

# Roll-up, summing over all stops in the trip_id
trip_rollups = data.groupby([
    'opd_date',
    'trip_id', 
    'rte', 
    'dir'
]).agg({
    'day_of_week': 'first',
    'vehicle_id': 'first',
    'is_rapidride': 'first',
    'direction_descr': 'first',
    'apc_stop_dt': 'min',
    'ons': 'sum',
    'offs': 'sum', 
    'orca_total': 'sum', 
    'orca_adult': 'sum', 
    'orca_disabled': 'sum', 
    'orca_senior': 'sum', 
    'orca_youth': 'sum', 
    'orca_lowincome': 'sum', 
    'orca_uw': 'sum'
}).rename(columns={
    'apc_stop_dt': 'first_stop_dt'
})
trip_rollups.head()
trip_rollups.to_csv('data/trip_id_rollups.tsv.gz', sep='\t', index=True)

