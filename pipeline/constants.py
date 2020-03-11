"""
Contains contants used throughout pipeline
"""

WINTER_DAYS = [f'2019-01-{day:02d}' for day in range(7, 32)] +  \
              [f'2019-02-{day:02d}' for day in range(1, 3)] +  \
              [f'2019-02-{day:02d}' for day in range(13, 29)] + \
              [f'2019-03-{day:02d}' for day in range(1, 4)]

SUMMER_DAYS = [f'2019-07-{day:02d}' for day in range(1, 32)]  + \
              [f'2019-08-{day:02d}' for day in range(1, 32)]

PC = 'passenger_count'
TPTD = 'txn_passenger_type_descr'
TSD = 'trip_start_dt'

APC_COLUMNS = ['daycode', 'trip_id', 'pattern_id', 'pattern_quality', 'blk', 'rte', 'dir',
               'sch_st_min', 'opd_date', 'pattern_quality_1', 'vehicle_id', 'stop_id',
               'stop_seq', 'stop_name', 'sch_stop_sec', 'act_stop_arr', 'sch_stop_tm',
               'act_stop_tm', 'dwell_sec', 'doors_open', 'door_open_sec', 'apc_veh', 'ons',
               'offs', 'load', 'geom', 'sch_stop_tm', 'act_stop_tm', 'stop_datetime',
               'gps_lat', 'gps_long']

ORCA_WINTER_COLUMNS = ['txn_id', 'trip_group_id', 'prev_leg_txn_id', 'card_serial_number',
                       'institution_id', 'institution_name', 'business_date',
                       'txn_dtm_pacific', 'txn_type_id', 'txn_subtype_id', 'txn_type_descr',
                       'upgrade_indicator', 'product_id', 'product_descr',
                       'txn_passenger_type_id', 'txn_passenger_type_descr', 'passenger_count',
                       'ceffv', 'service_agency_id', 'service_agency_name', 'source_agency_id',
                       'source_agency_name', 'transit_operator_abbrev', 'mode_id',
                       'mode_abbrev', 'mode_descr', 'route_number', 'direction_id',
                       'direction_descr', 'agency_trip_id', 'device_id', 'device_type',
                       'device_place_name', 'device_place_id', 'device_location_id',
                       'device_location_code', 'device_location_abbrev', 'device_location_descr',
                       'origin_location_id', 'origin_location_code', 'origin_location_abbrev',
                       'origin_location_descr', 'destination_location_id',
                       'destination_location_code', 'destination_location_abbrev',
                       'destination_location_descr', 'device_id_filt', 'stop_id', 'stop_time',
                       'stop_lat', 'stop_lon', 'stop_error', 'viaserviceareaid',
                       'viaserviceareaname', 'trip_id', 'last_mode_id', 'last_route_number',
                       'last_stop_id', 'last_stop_time', 'last_stop_lat', 'last_stop_lon']

ORCA_SUMMER_COLUMNS = ['institution_name', 'business_date', 'txn_dtm_pacific', 'txn_type_id',
                       'txn_passenger_type_id', 'service_agency_id', 'mode_id', 'route_number',
                       'stop_id', 'stop_time', 'stop_lat', 'stop_lon', 'trip_id',
                       'passenger_count', 'direction_descr']
