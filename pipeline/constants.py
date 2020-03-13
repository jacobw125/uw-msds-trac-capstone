'''
Contains contants used throughout pipeline
'''

AGG_COLUMNS = {'day_of_week': 'first', 'is_ns': 'first', 'is_rapid': 'first',
               'is_weekend': 'first', 'orca_total': 'sum', 'frac_disabled': 'mean',
               'frac_youth': 'mean', 'frac_senior': 'mean', 'frac_li': 'mean',
               'frac_uw': 'mean', 'ons': 'sum', 'region': 'first', 'start': 'first',
               'end' : 'first', 'type': 'first'}
AM = 'is_am'
APC_COLUMNS = ['daycode', 'trip_id', 'pattern_id', 'pattern_quality', 'blk', 'rte', 'dir',
               'sch_st_min', 'opd_date', 'pattern_quality_1', 'vehicle_id', 'stop_id',
               'stop_seq', 'stop_name', 'sch_stop_sec', 'act_stop_arr', 'sch_stop_tm',
               'act_stop_tm', 'dwell_sec', 'doors_open', 'door_open_sec', 'apc_veh', 'ons',
               'offs', 'load', 'geom', 'sch_stop_tm', 'act_stop_tm', 'stop_datetime',
               'gps_lat', 'gps_long']
APC_DATE = 'opd_date'
APC_DIR = 'data/apc'
APC_DIRECTION = 'dir'
APC_ONS = 'ons'
APC_RR = 'is_rapidride'
APC_RTE = 'rte'
APC_S = 'stop_datetime'
APC_SD = 'apc_stop_dt'
APC_VEH = 'apc_veh'

DAY_OF_WEEK = 'day_of_week'

HD = 'hour_of_day'
HDT = 'hour_of_day_text'
HQ = 'hr_quadrant'

MERGE_DIR = 'data/training_data/'

NS = 'is_ns'

ORCA_ADULT = 'orca_adult'
ORCA_AGENCY_ID = '4'
ORCA_APC_RATIO = 'orca_apc_ratio'
ORCA_BTD = 'biz_txn_diff'
ORCA_DATE = 'business_date'
ORCA_DIR = 'data/orca'
ORCA_DIR_AGG = 'data/orca_agg'
ORCA_DISABLED = 'orca_disabled'
ORCA_DISABLED_FRAC = 'frac_disabled'
ORCA_IN = 'institution_name'
ORCA_LOWINCOME = 'orca_lowincome'
ORCA_LOWINCOME_FRAC = 'frac_li'
ORCA_MODE = 'mode_id'
ORCA_MODE_IDS = ('128', '250')
ORCA_PC = 'passenger_count'
ORCA_RTE = 'route_number'
ORCA_SENIOR = 'orca_senior'
ORCA_SENIOR_FRAC = 'frac_senior'
ORCA_SERVICE_AGENCY_ID = 'service_agency_id'
ORCA_SUMMER_COLUMNS = ['institution_name', 'business_date', 'txn_dtm_pacific', 'txn_type_id',
                       'txn_passenger_type_id', 'service_agency_id', 'mode_id', 'route_number',
                       'stop_id', 'stop_time', 'stop_lat', 'stop_lon', 'trip_id',
                       'passenger_count', 'direction_descr']
ORCA_TDP = 'txn_dtm_pacific'
ORCA_TOTAL = 'orca_total'
ORCA_TPTD = 'txn_passenger_type_descr'
ORCA_TPTID = 'txn_passenger_type_id'
ORCA_TSD = 'trip_start_dt'
ORCA_UW = 'orca_uw'
ORCA_UW_FRAC = 'frac_uw'
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
ORCA_YOUTH = 'orca_youth'
ORCA_YOUTH_FRAC = 'frac_youth'

PARSED_DT = 'parsed_dt'
POOL = 4

REGION_PATH = 'data/rte_clean.csv'
RR = 'is_rapid'

STOP_ID = 'stop_id'
SUMMER_DAYS = [f'2019-07-{day:02d}' for day in range(1, 32)]  + \
              [f'2019-08-{day:02d}' for day in range(1, 32)]

TRIP_15 = 'trip_start_hr_15'
TRIP_30 = 'trip_start_hr_30'
TRIP_AM = 'trip_start_is_am'
TRIP_HR = 'trip_start_hr'
TRIP_ID = 'trip_id'
TXN_ID_TO_DESC = {'1': 'Adult', '2': 'Youth', '4': 'Disabled',
                  '3': 'Senior', '5': 'Low Income'}

WEATHER_FILE = 'boeing_field_2019.csv'
WINTER_DAYS = [f'2019-01-{day:02d}' for day in range(7, 32)] +  \
              [f'2019-02-{day:02d}' for day in range(1, 3)] +  \
              [f'2019-02-{day:02d}' for day in range(13, 29)] + \
              [f'2019-03-{day:02d}' for day in range(1, 4)]
WKD = 'is_weekend'
