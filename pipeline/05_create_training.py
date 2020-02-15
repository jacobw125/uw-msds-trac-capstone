#!/usr/bin/env python3
import pandas as pd
import numpy as np
from numpy.random import seed, random
from datetime import datetime
from os import makedirs

# Create features
print("Loading merged dataset")
data = pd.read_csv("data/merged_stops.tsv.gz", sep="\t")

both = False
if both:
    data_1 = pd.read_csv("data/merged_stops_1.tsv.gz", sep="\t")
    data.append(data_1)

def inf_or_nan(x):
    return np.isnan(x) | np.isinf(x)

print("Creating additional features")
data['orca_apc_ratio'] = (data['orca_total'] / data['ons']).where(lambda x: ~inf_or_nan(x), 0)
data['frac_disabled'] = (data['orca_disabled'] / data['orca_total']).where(lambda x: ~inf_or_nan(x), 0)
data['frac_youth'] = (data['orca_youth'] / data['orca_total']).where(lambda x: ~inf_or_nan(x), 0)
data['frac_senior'] = (data['orca_senior'] / data['orca_total']).where(lambda x: ~inf_or_nan(x), 0)
data['frac_li'] = (data['orca_lowincome'] / data['orca_total']).where(lambda x: ~inf_or_nan(x), 0)
data['frac_uw'] = (data['orca_uw'] / data['orca_total']).where(lambda x: ~inf_or_nan(x), 0)
data['is_ns'] = data.dir.isin(['N', 'S']) * 1.0
#data['is_inbound'] = (data.direction_descr == "Inbd") * 1.0
data['is_rapid'] = (data.is_rapidride) * 1.0
data['is_weekend'] = (data['day_of_week'] >= 5)*1.0  # 5 and 6 are Sat/Sun in python
data['parsed_dt'] = data['apc_stop_dt'].apply(lambda dt: datetime.strptime(dt, "%Y-%m-%d %H:%M:%S"))
data['is_am'] = data['parsed_dt'].apply(lambda dt: dt.hour < 12)
data['hour_of_day'] = data['parsed_dt'].apply(lambda dt: dt.hour)
data['hour_of_day_text'] = data['parsed_dt'].apply(lambda dt: dt.strftime("%H"))
data['hr_quadrant'] = data['parsed_dt'].apply(lambda dt: int(dt.minute / 15))

print("Figuring out when each trip started")
trip_starts = data.groupby(['opd_date', 'trip_id'])['parsed_dt'].min().reset_index().rename(columns={'parsed_dt': 'trip_start_dt'})
trip_starts.head()

valid_trips = data[['opd_date', 'trip_id', 'ons', 'orca_total']].groupby(['opd_date', 'trip_id']).sum().reset_index()
valid_trips = valid_trips[(valid_trips['ons'] >= 0) & (valid_trips['orca_total'] <= valid_trips['ons'])]
print(valid_trips.shape[0], data[['ons', 'orca_total']].sum(), valid_trips[['ons', 'orca_total']].sum())

data = pd.merge(
    data,
    trip_starts,
    on=['opd_date', 'trip_id'],
    how='left'
)

data = pd.merge(
    data,
    valid_trips,
    on = ['opd_date', 'trip_id'],
    suffixes = ('_x', '_y'),
    how = 'inner'
)

print(data.columns)

print(data.shape[0], data[['ons_x', 'orca_total_x']].sum(), valid_trips[['ons', 'orca_total']].sum())
data = data.rename(columns={'ons_x': 'ons', 'orca_total_x' : 'orca_total'})
assert not data.trip_start_dt.isnull().any(), "All trip_start_dt should be non-null"

# Add weather data
# print("Adding weather data")
# weather = pd.read_csv("data/boeing_field_2019.csv")
# weather = weather[weather['REPORT_TYPE'] == 'FM-15']
# weather['dt'] = weather['DATE'].apply(lambda dt: datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S"))
# weather['date_part'] = weather['dt'].apply(lambda dt: dt.strftime("%Y-%m-%d"))
# weather['hour_part'] = weather['dt'].apply(lambda dt: dt.strftime("%H"))
#
# WEATHER_COLS = ['HourlyDryBulbTemperature', 'HourlyPrecipitation', 'HourlyRelativeHumidity', 'HourlySeaLevelPressure', 'HourlyWindSpeed']
# weather = weather[['date_part', 'hour_part'] + WEATHER_COLS]
#
# def remove_t_and_s(x):
#     if (type(x) == type(0.1)):
#         return x
#     if x.endswith('s'):
#         x = x[:-1]
#     if x in ('T', '*'):
#         return np.nan
#     return float(x)
#
# weather['HourlyDryBulbTemperature'] = weather.HourlyDryBulbTemperature.apply(remove_t_and_s)
# weather['HourlyPrecipitation'] = weather.HourlyPrecipitation.apply(remove_t_and_s)
# weather['HourlyRelativeHumidity'] = weather.HourlyRelativeHumidity.apply(remove_t_and_s)
# weather['HourlySeaLevelPressure'] = weather.HourlySeaLevelPressure.apply(remove_t_and_s)
# weather['HourlyWindSpeed'] = weather.HourlyWindSpeed.apply(remove_t_and_s)

# def mean_inpute(col):
#     return col.where(~pd.isnull(col), col.mean())
#
# # mean inpute missing weather values, except precip which is assumed to be 0 when NA
# weather['HourlyPrecipitation'] = weather['HourlyPrecipitation'].fillna(0.)
# weather['HourlyRelativeHumidity'] = mean_inpute(weather['HourlyRelativeHumidity'])
# weather['HourlySeaLevelPressure'] = mean_inpute(weather['HourlySeaLevelPressure'])
# weather['HourlyWindSpeed'] = mean_inpute(weather['HourlyWindSpeed'])

# In case I need to re-merge weather, here's how to drop those columns
# data.drop(columns=[
#     'HourlyDryBulbTemperature',
#     'HourlyPrecipitation',
#     'HourlyRelativeHumidity',
#     'HourlySeaLevelPressure',
#     'HourlyWindSpeed',
# ], inplace=True)
# data = pd.merge(data, weather, how="left", left_on=['opd_date', 'hour_of_day_text'], right_on=['date_part', 'hour_part'])

X_COLUMNS = {
    'day_of_week': 'first',
    'is_ns': 'first',
    # 'is_inbound': 'first',
    'is_rapid': 'first',
    'is_weekend': 'first',
    'orca_total': 'sum',
    # 'orca_apc_ratio': 'mean',
    'frac_disabled': 'mean',
    'frac_youth': 'mean',
    'frac_senior': 'mean',
    'frac_li': 'mean',
    'frac_uw': 'mean',
    'ons': 'sum',
    'region': 'first',
    'start': 'first',
    'end' : 'first',
    'type': 'first'
    # **{col: 'mean' for col in WEATHER_COLS}
}

print("Creating time-aggregate columns")
# Create time-level aggregates
data['trip_start_hr'] = data['trip_start_dt'].apply(lambda dt: dt.strftime("%H"))
data['trip_start_is_am'] = data['trip_start_dt'].apply(lambda dt: 'am' if dt.hour < 12 else 'pm')
data['trip_start_hr_30'] = data['trip_start_dt'].apply(lambda dt: dt.strftime("%H") + '_' + str(int(dt.minute/30)*30))
data['trip_start_hr_15'] = data['trip_start_dt'].apply(lambda dt: dt.strftime("%H") + '_' + str(int(dt.minute/15)*15))

def agg_data(agg_by):
    return data.groupby(agg_by).agg(X_COLUMNS).reset_index().sort_values(agg_by)

print("Performing aggregation (day)")
# Group by day, rte/dir
aggr_day = agg_data(['opd_date', 'rte', 'dir'])

# Group by day, AM/PM, rte/dir
print("Performing aggregation (am/pm)")
aggr_day_ampm = agg_data(['opd_date', 'trip_start_is_am', 'rte', 'dir'])

# Group by day, hour of day, rte/dir
print("Performing aggregation (hour)")
aggr_day_hr = agg_data(['opd_date', 'trip_start_hr', 'rte', 'dir'])

# Group by day, hour of day, half hour, rte/dir
print("Performing aggregation (half hour)")
aggr_day_30 = agg_data(['opd_date', 'trip_start_hr_30', 'rte', 'dir'])

# Group by day, hour of day, hour quadrant, rte/dir
print("Performing aggregation (15-minute)")
aggr_day_15 = agg_data(['opd_date', 'trip_start_hr_15', 'rte', 'dir'])


def train_test_split(data, prefix):
    print("Performing train/test split for level: " + prefix)
    seed(1337)
    is_train = (random(len(data)) < 0.8)
    is_xval = (~is_train) & (random(len(data)) < 0.5)  # half of the non training samples
    #
    data_train = data[is_train]
    data_xval = data[is_xval]
    data_test = data[~is_train & ~is_xval]
    #
    print(f"Train: {len(data_train)/len(data):%}")
    print(f"Xval: {len(data_xval)/len(data):%}")
    print(f"Test: {len(data_test)/len(data):%}")
    #
    makedirs(f"data/aggregates/{prefix}", exist_ok=True)
    print(" Saving data")
    data_train.to_csv(f"data/aggregates/{prefix}/train.tsv.gz", sep='\t', index=False, compression='gzip')
    data_xval.to_csv(f"data/aggregates/{prefix}/xval.tsv.gz", sep='\t', index=False, compression='gzip')
    data_test.to_csv(f"data/aggregates/{prefix}/test.tsv.gz", sep='\t', index=False, compression='gzip')
    #
    return data_train, data_xval, data_test

print("Performing train/test split")
train, xval, test = train_test_split(aggr_day, "day")
train, xval, test = train_test_split(aggr_day_ampm, "ampm")
train, xval, test = train_test_split(aggr_day_hr, "hr")
train, xval, test = train_test_split(aggr_day_30, "30min")
train, xval, test = train_test_split(aggr_day_15, "15min")
