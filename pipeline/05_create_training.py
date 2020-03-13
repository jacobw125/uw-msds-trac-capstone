# pylint: disable=C0103
'''
Create different levels of time aggregation (15min, 30min, 1hr, AM/PM, Day),
grouping on when the trip starts. Makes train, test, val split for each time
aggregation modeling sets.
'''

from sys import argv
from os import makedirs
from datetime import datetime
import pandas as pd
from numpy.random import seed, random

import constants as c
import functions as func

# checks for input argument
if len(argv) < 2:
    raise ValueError('Missing input parameter. Needs if Summer, \
                     Winter, or Both data (S/W/B).')

# define seasonal constants
SEASON = argv[1]

if SEASON == 'S':
    TAG1 = 'summer_data'
    TAG2 = TAG1
    TAG3 = TAG1
elif SEASON == 'W':
    TAG1 = 'winter_data'
    TAG2 = TAG1
    TAG3 = TAG1
else:
    TAG1 = 'summer_data'
    TAG2 = 'winter_data'
    TAG3 = 'combined_data'

# load merged ORCA/APC file
print('Loading merged dataset')
DATA = pd.read_csv(c.MERGE_DIR + TAG1 + '/merged_stops.tsv.gz', sep='\t')

if SEASON == 'B':
    DATA.append(pd.read_csv(c.MERGE_DIR + TAG2 + '/merged_stops.tsv.gz', sep='\t'))

# create additional features
print('Creating additional features')
DATA[c.ORCA_APC_RATIO] = (DATA[c.ORCA_TOTAL] / \
                          DATA[c.APC_ONS]).where(lambda x: ~func.inf_or_nan(x), 0)
DATA[c.ORCA_DISABLED_FRAC] = (DATA[c.ORCA_DISABLED] / \
                         DATA[c.ORCA_TOTAL]).where(lambda x: ~func.inf_or_nan(x), 0)
DATA[c.ORCA_YOUTH_FRAC] = (DATA[c.ORCA_YOUTH] / \
                      DATA[c.ORCA_TOTAL]).where(lambda x: ~func.inf_or_nan(x), 0)
DATA[c.ORCA_SENIOR_FRAC] = (DATA[c.ORCA_SENIOR] / \
                       DATA[c.ORCA_TOTAL]).where(lambda x: ~func.inf_or_nan(x), 0)
DATA[c.ORCA_LOWINCOME_FRAC] = (DATA[c.ORCA_LOWINCOME] / \
                   DATA[c.ORCA_TOTAL]).where(lambda x: ~func.inf_or_nan(x), 0)
DATA[c.ORCA_UW_FRAC] = (DATA[c.ORCA_UW] / \
                   DATA[c.ORCA_TOTAL]).where(lambda x: ~func.inf_or_nan(x), 0)
DATA[c.NS] = DATA.dir.isin(['N', 'S']) * 1.0
DATA[c.RR] = (DATA.is_rapidride) * 1.0
DATA[c.WKD] = (DATA[c.DAY_OF_WEEK] >= 5)*1.0  # 5 and 6 are Sat/Sun in python
DATA[c.PARSED_DT] = DATA[c.APC_SD].apply(lambda dt: datetime.strptime(dt, '%Y-%m-%d %H:%M:%S'))
DATA[c.AM] = DATA[c.PARSED_DT].apply(lambda dt: dt.hour < 12)
DATA[c.HD] = DATA[c.PARSED_DT].apply(lambda dt: dt.hour)
DATA[c.HDT] = DATA[c.PARSED_DT].apply(lambda dt: dt.strftime('%H'))
DATA[c.HQ] = DATA[c.PARSED_DT].apply(lambda dt: int(dt.minute / 15))

# find start of trip
print('Figuring out when each trip started')
TRIP_STARTS = DATA.groupby([c.APC_DATE, c.TRIP_ID])[c.PARSED_DT].min().reset_index()
TRIP_STARTS = TRIP_STARTS.rename(columns={c.PARSED_DT: 'trip_start_dt'})
TRIP_STARTS.head()

# merge start of trip to data
DATA = pd.merge(
    DATA,
    TRIP_STARTS,
    on=[c.APC_DATE, c.TRIP_ID],
    how='left'
)

# find valid trip, where APC >= 0 and APC >= ORCA
VALID_TRIPS = DATA[[c.APC_DATE, c.TRIP_ID,
                    c.APC_ONS, c.ORCA_TOTAL]].groupby([c.APC_DATE, c.TRIP_ID]).sum().reset_index()
VALID_TRIPS = VALID_TRIPS[(VALID_TRIPS[c.APC_ONS] >= 0) & \
                          (VALID_TRIPS[c.ORCA_TOTAL] <= VALID_TRIPS[c.APC_ONS])]
print(VALID_TRIPS.shape[0], DATA[[c.APC_ONS, c.ORCA_TOTAL]].sum(),
      VALID_TRIPS[[c.APC_ONS, c.ORCA_TOTAL]].sum())

DATA = pd.merge(
    DATA,
    VALID_TRIPS,
    on=[c.APC_DATE, c.TRIP_ID],
    suffixes=('_x', '_y'),
    how='inner'
)

print(DATA.columns)
print(DATA.shape[0], DATA[['ons_x', 'orca_total_x']].sum(),
      VALID_TRIPS[[c.APC_ONS, c.ORCA_TOTAL]].sum())
DATA = DATA.rename(columns={'ons_x': c.APC_ONS, 'orca_total_x' : c.ORCA_TOTAL})
assert not DATA.trip_start_dt.isnull().any(), 'All trip_start_dt should be non-null'
AGG_TYPES = c.AGG_COLUMNS

## Add weather data
# print('Adding weather data')
# WEATHER = pd.read_csv(c.MERGE_DIR + TAG1 + '/' + c.WEATHER_FILE)
# WEATHER = WEATHER[WEATHER['REPORT_TYPE'] == 'FM-15']
# WEATHER['dt'] = WEATHER['DATE'].apply(lambda dt: datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S'))
# WEATHER['date_part'] = WEATHER['dt'].apply(lambda dt: dt.strftime('%Y-%m-%d'))
# WEATHER['hour_part'] = WEATHER['dt'].apply(lambda dt: dt.strftime('%H'))
#
# WEATHER_COLS = ['HourlyDryBulbTemperature', 'HourlyPrecipitation',
#                 'HourlyRelativeHumidity', 'HourlySeaLevelPressure', 'HourlyWindSpeed']
# WEATHER = WEATHER[['date_part', 'hour_part'] + WEATHER_COLS]
#
# WEATHER['HourlyDryBulbTemperature'] = WEATHER.HourlyDryBulbTemperature.apply(func.remove_t_and_s)
# WEATHER['HourlyPrecipitation'] = WEATHER.HourlyPrecipitation.apply(func.remove_t_and_s)
# WEATHER['HourlyRelativeHumidity'] = WEATHER.HourlyRelativeHumidity.apply(func.remove_t_and_s)
# WEATHER['HourlySeaLevelPressure'] = WEATHER.HourlySeaLevelPressure.apply(func.remove_t_and_s)
# WEATHER['HourlyWindSpeed'] = WEATHER.HourlyWindSpeed.apply(func.remove_t_and_s)
#
# # mean inpute missing weather values, except precip which is assumed to be 0 when NA
# WEATHER['HourlyPrecipitation'] = WEATHER['HourlyPrecipitation'].fillna(0.)
# WEATHER['HourlyRelativeHumidity'] = func.mean_input(WEATHER['HourlyRelativeHumidity'])
# WEATHER['HourlySeaLevelPressure'] = func.mean_input(WEATHER['HourlySeaLevelPressure'])
# WEATHER['HourlyWindSpeed'] = func.mean_input(WEATHER['HourlyWindSpeed'])
#
# # In case I need to re-merge weather, here's how to drop those columns
# DATA.drop(columns=['HourlyDryBulbTemperature', 'HourlyPrecipitation', 'HourlyRelativeHumidity',
#                    'HourlySeaLevelPressure', 'HourlyWindSpeed',], inplace=True)
# DATA = pd.merge(DATA, WEATHER, how='left', left_on=[c.APC_DATE, c.HDT],
#                 right_on=['date_part', 'hour_part'])
# for col in WEATHER_COLS:
#     AGG_TYPES.add('col', 'mean')

print('Creating time-aggregate columns')
# Create time-level aggregates
DATA[c.TRIP_HR] = DATA[c.ORCA_TSD].apply(lambda dt: dt.strftime('%H'))
DATA[c.TRIP_AM] = DATA[c.ORCA_TSD].apply(lambda dt: 'am' if dt.hour < 12 else 'pm')
DATA[c.TRIP_30] = DATA[c.ORCA_TSD].apply(lambda dt: dt.strftime('%H') + \
                                            '_' + str(int(dt.minute/30)*30))
DATA[c.TRIP_15] = DATA[c.ORCA_TSD].apply(lambda dt: dt.strftime('%H') + \
                                            '_' + str(int(dt.minute/15)*15))

print('Performing aggregation (day)')

# Group by day, rte/dir
AGGR_DAY = func.agg_data(DATA, [c.APC_DATE, c.APC_RTE, c.APC_DIRECTION], AGG_TYPES)

# Group by day, AM/PM, rte/dir
print('Performing aggregation (am/pm)')
AGGR_DAY_AMPM = func.agg_data(DATA, [c.APC_DATE, c.TRIP_AM,
                                     c.APC_RTE, c.APC_DIRECTION], AGG_TYPES)

# Group by day, hour of day, rte/dir
print('Performing aggregation (hour)')
AGGR_DAY_HR = func.agg_data(DATA, [c.APC_DATE, c.TRIP_HR,
                                   c.APC_RTE, c.APC_DIRECTION], AGG_TYPES)

# Group by day, hour of day, half hour, rte/dir
print('Performing aggregation (half hour)')
AGGR_DAY_30 = func.agg_data(DATA, [c.APC_DATE, c.TRIP_30,
                                   c.APC_RTE, c.APC_DIRECTION], AGG_TYPES)

# Group by day, hour of day, hour quadrant, rte/dir
print('Performing aggregation (15-minute)')
AGGR_DAY_15 = func.agg_data(DATA, [c.APC_DATE, c.TRIP_15,
                                   c.APC_RTE, c.APC_DIRECTION], AGG_TYPES)

def train_test_split(dataset, prefix):
    '''
    Randomly split the dataset 80%, 10%, 10% for train, test, and validation set,
    respectively. Save training set. Important to use same training sets when comparing
    models to limit noise caused by the data.
    '''
    print('Performing train/test split for level: ' + prefix)
    seed(1337)
    is_train = (random(len(dataset)) < 0.8)
    is_xval = (~is_train) & (random(len(dataset)) < 0.5)  # half of the non training samples

    data_train = dataset[is_train]
    data_xval = dataset[is_xval]
    data_test = dataset[~is_train & ~is_xval]

    print(f'Train: {len(data_train)/len(dataset):%}')
    print(f'Xval: {len(data_xval)/len(dataset):%}')
    print(f'Test: {len(data_test)/len(dataset):%}')
    #
    makedirs(f'{c.MERGE_DIR}{TAG3}/{prefix}', exist_ok=True)
    print('Saving data')
    data_train.to_csv(f'{c.MERGE_DIR}{TAG3}/{prefix}/train.tsv.gz',
                      sep='\t', index=False, compression='gzip')
    data_xval.to_csv(f'{c.MERGE_DIR}{TAG3}/{prefix}/xval.tsv.gz',
                     sep='\t', index=False, compression='gzip')
    data_test.to_csv(f'{c.MERGE_DIR}{TAG3}/{prefix}/test.tsv.gz',
                     sep='\t', index=False, compression='gzip')
    #
    return data_train, data_xval, data_test

print('Performing train/test split')
TRAIN, XVAL, TEST = train_test_split(AGGR_DAY, 'day')
TRAIN, XVAL, TEST = train_test_split(AGGR_DAY_AMPM, 'ampm')
TRAIN, XVAL, TEST = train_test_split(AGGR_DAY_HR, 'hr')
TRAIN, XVAL, TEST = train_test_split(AGGR_DAY_30, '30min')
TRAIN, XVAL, TEST = train_test_split(AGGR_DAY_15, '15min')
