# pylint=E0402
'''
The goal here is to send data for about 30 trips half on the same route, let's say 49, to
spot check our merging. The 49 data should be 1-2 days everything else is randomly chosen.
'''

import pandas as pd
from .. import contants as c

DAYS_TO_KEEP = [f'2019-03-{day:02d}' for day in range(1, 3)] # 2019-03-01, 02

# APC data
APC = pd.concat([pd.read_csv(f'{c.APC_DIR}/{DAYS_TO_KEEP[0]}.tsv.gz', sep='\t'),
                 pd.read_csv(f'{c.APC_DIR}/{DAYS_TO_KEEP[1]}.tsv.gz', sep='\t')])
APC = APC[APC.rte == 49]
APC.to_csv('data/for_mark/49_apc.tsv.gz', sep='\t', compression='gzip', index=False)

# ORCA data
ORCA = pd.concat([pd.read_csv(f'{c.ORCA_DIR}/{DAYS_TO_KEEP [0]}.tsv.gz', sep='\t'),
                  pd.read_csv(f'{c.ORCA_DIR}/{DAYS_TO_KEEP [1]}.tsv.gz', sep='\t')])
ORCA = ORCA[ORCA.route_number == 49][['passenger_count', 'device_id', 'stop_error',
                                      'trip_id', 'stop_time', 'service_agency_name',
                                      'stop_lon', 'direction_descr', 'source_agency_name',
                                      'stop_id', 'origin_location_descr', 'txn_type_descr',
                                      'business_date', 'day_of_week', 'upgrade_indicator',
                                      'mode_descr', 'device_place_name', 'is_rapidride',
                                      'route_number', 'biz_txn_diff', 'ceffv', 'stop_lat',
                                      'agency_trip_id', 'txn_dtm_pacific',
                                      'transit_operator_abbrev', 'destination_location_descr',
                                      'device_location_descr']]
ORCA.to_csv('data/for_mark/49_orca.tsv.gz', sep='\t', compression='gzip', index=False)

# ORCA aggregated data
ORCA_AGG = pd.concat([pd.read_csv(f'{c.ORCA_DIR_AGG}/{DAYS_TO_KEEP[0]}.tsv.gz', sep='\t'),
                      pd.read_csv(f'{c.ORCA_DIR_AGG}/{DAYS_TO_KEEP[1]}.tsv.gz', sep='\t')])
ORCA_AGG = ORCA_AGG[ORCA_AGG.route_number == 49]
ORCA_AGG.to_csv('data/for_mark/49_ORCA_AGGregated.tsv.gz', sep='\t',
                compression='gzip', index=False)

# merged stop level data
MERGED = pd.read_csv('data/winter_data/merged_stops.tsv.gz', sep='\t')
MERGED = MERGED[(MERGED.rte == 49) & (MERGED.opd_date.isin(DAYS_TO_KEEP))]
MERGED.to_csv('data/for_mark/49_merged_at_stop_level.tsv.gz', sep='\t',
              compression='gzip', index=False)

# Random sample a number of trip IDs from a single day.
APC = pd.read_csv(f'{c.APC_DIR}/2019-01-15.tsv.gz', sep='\t')
RANDOM_IDS = APC.trip_id.sample(5, random_state=1234)
APC = APC[APC.trip_id.isin(RANDOM_IDS)]
APC.to_csv('data/for_mark/RS_apc.tsv.gz', sep='\t', index=False, compression='gzip')

ORCA = pd.read_csv(f'{c.ORCA_DIR}/2019-01-15.tsv.gz', sep='\t')
ORCA = ORCA[ORCA.trip_id.isin(RANDOM_IDS)][['passenger_count', 'device_id', 'stop_error',
                                            'trip_id', 'stop_time', 'service_agency_name',
                                            'stop_lon', 'direction_descr', 'source_agency_name',
                                            'stop_id', 'origin_location_descr', 'txn_type_descr',
                                            'business_date', 'day_of_week', 'upgrade_indicator',
                                            'mode_descr', 'device_place_name', 'is_rapidride',
                                            'route_number', 'biz_txn_diff', 'ceffv', 'stop_lat',
                                            'agency_trip_id', 'txn_dtm_pacific',
                                            'transit_operator_abbrev', 'destination_location_descr',
                                            'device_location_descr']]
ORCA.to_csv('data/for_mark/RS_orca.tsv.gz', sep='\t', index=False, compression='gzip')

ORCA_AGG = pd.read_csv(f'{c.ORCA_DIR_AGG}/2019-01-15.tsv.gz', sep='\t')
ORCA_AGG = ORCA[ORCA.trip_id.isin(ORCA_AGG)]
ORCA.to_csv('data/for_mark/RS_ORCA_AGGregated.tsv.gz', sep='\t', index=False, compression='gzip')

MERGED = pd.read_csv('data/winter_data/merged_stops.tsv.gz', sep='\t')
MERGED = MERGED[(MERGED.trip_id.isin(RANDOM_IDS)) & (MERGED.opd_date == '2019-01-15')]
MERGED.to_csv('data/for_mark/RS_merged_at_stop_level.tsv.gz', sep='\t',
              compression='gzip', index=False)
