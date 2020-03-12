# pylint: disable=C0103
'''
Merges the aggregated APC/AVL file with the aggregated Orca file. Then merges with
neighborhood data.

Uncomment code at bottom to get trip roll-up data.
'''

from sys import argv
from multiprocessing import Pool
import pandas as pd

import constants as c

# checks for input argument
if len(argv) < 2:
    raise ValueError('Missing input parameter. Needs if Summer data (True/False).')

# define seasonal constants
IS_SUMMER = argv[1] == 'True'

DAYS_TO_KEEP = c.WINTER_DAYS
TAG = 'winter_data'

if IS_SUMMER:
    DAYS_TO_KEEP = c.SUMMER_DAYS
    TAG = 'summer_data'

# read neighborhood data
REGION_DATA = pd.read_csv(c.REGION_PATH)

COLS_TO_FILLNA = [c.ORCA_TOTAL, c.ORCA_ADULT, c.ORCA_DISABLED,
                  c.ORCA_SENIOR, c.ORCA_YOUTH, c.ORCA_LOWINCOME, c.ORCA_UW]

DESIRED_COLUMN_ORDER = [c.APC_DATE, c.APC_SD, c.DAY_OF_WEEK, c.TRIP_ID, c.STOP_ID,
                        c.APC_RTE, c.APC_RR, c.APC_DIRECTION, c.APC_ONS, c.ORCA_TOTAL,
                        c.ORCA_ADULT, c.ORCA_DISABLED, c.ORCA_SENIOR, c.ORCA_YOUTH,
                        c.ORCA_LOWINCOME, c.ORCA_UW]
SORT_BY = [c.TRIP_ID]

def process_file(day):
    '''
    Reads apc data day csv, deduplicates rows, adds features, and aggregates over trip, stop,
    and day. Saves the aggregated to dataframe.

    :params fname string:
    :returns dataframe:
    '''

    # reads apc file
    print(f'Merging {day}')
    apc = pd.read_csv(f'{c.APC_DIR}/{day}.tsv.gz', sep='\t',
                      dtype={c.TRIP_ID: 'O', c.STOP_ID: 'O',
                             c.APC_RTE: 'O'}).drop(columns=[c.APC_VEH])

    # finds duplicates and removes them if they exist
    n_dups = sum(apc.duplicated())
    if n_dups > 0:
        print(f'Found {n_dups} APC duplicates on day {day}')
        apc = apc.drop_duplicates()

    # aggregate over trip, stop, and day
    apc[c.APC_ONS] = apc[c.APC_ONS].where(apc[c.APC_ONS] < 150, None)
    apc_gb = [c.APC_DATE, c.TRIP_ID, c.STOP_ID, c.APC_RR, c.APC_RTE, c.DAY_OF_WEEK, c.APC_DIRECTION]
    apc_so = [c.APC_ONS, c.APC_S]
    apc = apc[apc_gb + apc_so].groupby(apc_gb).agg({c.APC_S: 'first',
                                                    c.APC_ONS: 'sum'}).reset_index()

    # merges aggregated orca with aggregated apc
    orca = pd.read_csv(f'{c.ORCA_DIR_AGG}/{day}.tsv.gz', sep='\t',
                       dtype={c.TRIP_ID: 'O', c.STOP_ID: 'O'})
    merged_data = pd.merge(apc, orca,
                           left_on=[c.TRIP_ID, c.STOP_ID],
                           right_on=[c.TRIP_ID, c.STOP_ID],
                           how='left',
                           suffixes=('_apc', '_orca'))
    merged_data = merged_data.rename(columns={c.APC_S: c.APC_SD})
    merged_data = merged_data.drop(columns=[c.ORCA_DATE])[DESIRED_COLUMN_ORDER]
    merged_data = merged_data.sort_values(SORT_BY)
    merged_data[COLS_TO_FILLNA] = merged_data[COLS_TO_FILLNA].fillna(value=0)

    return merged_data

# parallelize file processing and concatenates returned dataframes
with Pool(4) as p:
    DATA = pd.concat(p.map(process_file, DAYS_TO_KEEP))

# print merged file totals
print(DATA.shape)
print(DATA[[c.APC_ONS, c.ORCA_TOTAL]].sum())

# merge neighborhood data with apc/orca data
DATA[c.APC_RTE] = DATA[c.APC_RTE].astype('str')
REGION_DATA[c.APC_RTE] = REGION_DATA[c.APC_RTE].astype('str')
DATA = pd.merge(DATA, REGION_DATA,
                left_on=[c.APC_RTE],
                right_on=[c.APC_RTE],
                how='left',
                suffixes=('_d', '_r'))

# print post merge characteristic to self check merge was healthy
print(DATA.shape)
print(DATA[[c.APC_ONS, c.ORCA_TOTAL]].sum())

if not IS_SUMMER:
    print(DATA[c.ORCA_TOTAL].sum() / DATA[c.APC_ONS].sum())

    # For the following query, Maggie expects there to be one row, ons = 15 and orca_total = 4
    print(DATA[(DATA[c.TRIP_ID] == '40684352') &
               (DATA[c.APC_DATE] == '2019-03-01') &
               (DATA[c.STOP_ID] == '1180')].T)
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
    ORCA_SUB_1 = pd.read_csv(f'{c.ORCA_DIR_AGG}/2019-03-01.tsv.gz', sep='\t',
                             dtype={c.TRIP_ID: 'O', c.STOP_ID: 'O', c.ORCA_RTE: 'O'})
    print(ORCA_SUB_1[(ORCA_SUB_1[c.TRIP_ID] == '40684352')
                     & (ORCA_SUB_1[c.STOP_ID] == '1180')].T)
    # business_date           p  2019-03-01
    # trip_id                     40684352
    # stop_id                         1180
    # orca_total                         4
    # orca_adult                         1
    # orca_disabled                      1
    # orca_senior                        2

    # Confirm in the raw orca data
    ORCA_SUB_2 = pd.read_csv(f'{c.ORCA_DIR}/2019-03-01.tsv.gz', sep='\t',
                             dtype={c.TRIP_ID: 'O', c.STOP_ID: 'O', c.ORCA_RTE: 'O'})
    print(ORCA_SUB_2[(ORCA_SUB_2[c.TRIP_ID] == '40684352') &
                     (ORCA_SUB_2[c.STOP_ID] == '1180')][[c.ORCA_PC, c.STOP_ID,
                                                         c.TRIP_ID, c.ORCA_TPTD]])
    #         passenger_count stop_id   trip_id txn_passenger_type_descr
    # 145850                1    1180  40684352                 Disabled
    # 153466                1    1180  40684352                    Adult
    # 161685                1    1180  40684352                   Senior
    # 248018                1    1180  40684352                   Senior


# save the data
DATA.to_csv(c.MERGE_DIR + TAG + '/merged_stops.tsv.gz', sep='\t', index=False)


## Roll-up, summing over all stops in the trip_id
# DATA.groupby([c.APC_DATE, c.TRIP_ID,
#               c.APC_DIRECTION, c.APC_RTE]).agg({c.DAY_OF_WEEK: 'first', c.APC_RR: 'first',
#                                                 c.APC_ONS: 'sum', c.ORCA_TOTAL: 'sum',
#                                                 c.ORCA_ADULT: 'sum', c.ORCA_DISABLED: 'sum',
#                                                 c.ORCA_SENIOR: 'sum', c.ORCA_YOUTH: 'sum',
#                                                 c.ORCA_LOWINCOME: 'sum', c.ORCA_UW: 'sum'}
#                                                 ).to_csv(c.MERGE_DIR + TAG + \
#                                                 '/trip_id_rollups.tsv.gz', sep='\t', index=True)
