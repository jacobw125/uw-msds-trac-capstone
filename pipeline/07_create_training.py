#!/usr/bin/env python3
import pandas as pd
import numpy as np
from numpy.random import seed, random
from datetime import datetime
from os import makedirs

# Create features, train/test split, and rollup into various aggregation levels
data = pd.read_csv("data/merged_stops.tsv.gz", sep="\t")
data['orca_apc_ratio'] = (data['orca_total'] / data['ons']).where(lambda x: ~np.isinf(x), 0)
data['frac_disabled'] = (data['orca_disabled'] / data['orca_total']).where(lambda x: ~np.isinf(x), 0)
data['frac_youth'] = (data['orca_youth'] / data['orca_total']).where(lambda x: ~np.isinf(x), 0)
data['frac_senior'] = (data['orca_senior'] / data['orca_total']).where(lambda x: ~np.isinf(x), 0)
data['frac_li'] = (data['orca_lowincome'] / data['orca_total']).where(lambda x: ~np.isinf(x), 0)
data['frac_uw'] = (data['orca_uw'] / data['orca_total']).where(lambda x: ~np.isinf(x), 0)
data['is_ns'] = data.dir.isin(['N', 'S']) * 1.0
data['is_inbound'] = (data.direction_descr == "Inbd") * 1.0
data['is_rapid'] = (data.is_rapidride) * 1.0
data['parsed_dt'] = data['apc_stop_dt'].apply(lambda dt: datetime.strptime(dt, "%Y-%m-%d %H:%M:%S"))
data['is_am'] = data['parsed_dt'].apply(lambda dt: dt.hour < 12)
data['hour_of_day'] = data['parsed_dt'].apply(lambda dt: dt.hour)
data['hour_of_day_text'] = data['parsed_dt'].apply(lambda dt: dt.strftime("%H"))
data['hr_quadrant'] = data['parsed_dt'].apply(lambda dt: int(dt.minute / 15))

weather = pd.read_csv("data/boeing_field_weather.csv")
weather['dt'] = weather['DATE'].apply(lambda dt: datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S"))
weather['date_part'] = weather['dt'].apply(lambda dt: dt.strftime("%Y-%m-%d"))
weather['hour_part'] = weather['dt'].apply(lambda dt: dt.strftime("%H"))

WEATHER_COLS = ['HourlyDryBulbTemperature', 'HourlyPrecipitation', 'HourlyRelativeHumidity', 'HourlySeaLevelPressure', 'HourlyWindSpeed']
weather = weather[['date_part', 'hour_part', WEATHER_COLS]]

def remove_t_and_s(x):
    if (type(x) == type(0.1)): 
        return x
    if x.endswith('s'): 
        x = x[:-1]
    if x == 'T':
        return np.nan
    return float(x)

weather['HourlyDryBulbTemperature'] = weather.HourlyDryBulbTemperature.apply(remove_t_and_s)
weather['HourlyPrecipitation'] = weather.HourlyPrecipitation.apply(remove_t_and_s)
weather['HourlyRelativeHumidity'] = weather.HourlyRelativeHumidity.apply(remove_t_and_s)
weather['HourlySeaLevelPressure'] = weather.HourlySeaLevelPressure.apply(remove_t_and_s)
weather['HourlyWindSpeed'] = weather.HourlyWindSpeed.apply(remove_t_and_s)


data = pd.merge(data, weather, how="left", left_on=['opd_date', 'hour_of_day_text'], right_on=['date_part', 'hour_part'])
data[WEATHER_COLS] = data[WEATHER_COLS].fillna(value=0.)

for col in WEATHER_COLS:
    print(col + " " + str(data[col].dtype))

X_COLUMNS = {
    'day_of_week': 'first',
    'is_ns': 'first',
    'is_inbound': 'first',
    'is_rapid': 'first',
    'orca_total': 'sum',
    'orca_apc_ratio': 'mean',
    'frac_disabled': 'mean',
    'frac_youth': 'mean',
    'frac_senior': 'mean',
    'frac_li': 'mean',
    'frac_uw': 'mean',
    'ons': 'sum',
    **{col: 'mean' for col in WEATHER_COLS}
}

# Group by day, trip_id (rte/dir)
aggr_day = data.groupby(['opd_date', 'trip_id', 'rte', 'dir']).agg(X_COLUMNS).reset_index().sort_values(['opd_date', 'trip_id'])

# Group by day, AM/PM, trip_id (rte/dir)
aggr_day_ampm = data.groupby(['opd_date', 'is_am', 'trip_id', 'rte', 'dir']).agg(X_COLUMNS).reset_index().sort_values(['opd_date', 'trip_id', 'is_am'])

# Group by day, hour of day, trip_id (rte/dir)
aggr_day_hr = data.groupby(['opd_date', 'hour_of_day', 'trip_id', 'rte', 'dir']).agg(X_COLUMNS).reset_index().sort_values(['opd_date', 'trip_id', 'hour_of_day'])
            
# Group by day, hour of day, hour quadrant, trip_id (rte/dir)
aggr_day_15 = data.groupby(['opd_date', 'hour_of_day', 'hr_quadrant', 'trip_id', 'rte', 'dir']).agg(X_COLUMNS).reset_index().sort_values(['opd_date', 'trip_id', 'hour_of_day', 'hr_quadrant'])


def train_test_split(data, prefix):
    seed(1337)
    is_train = (random(len(data)) < 0.8)
    is_xval = (~is_train) & (random(len(data)) < 0.5)  # half of the non training samples

    data_train = data[is_train]
    data_xval = data[is_xval]
    data_test = data[~is_train & ~is_xval]

    print(f"Train: {len(data_train)/len(data):%}")
    print(f"Xval: {len(data_xval)/len(data):%}")
    print(f"Test: {len(data_test)/len(data):%}")

    makedirs(f"data/aggregates/{prefix}", exist_ok=True)
    data_train.to_csv(f"data/aggregates/{prefix}/train.tsv.gz", sep='\t', index=False, compression='gzip')
    data_xval.to_csv(f"data/aggregates/{prefix}/xval.tsv.gz", sep='\t', index=False, compression='gzip')
    data_test.to_csv(f"data/aggregates/{prefix}/test.tsv.gz", sep='\t', index=False, compression='gzip')

    return data_train, data_xval, data_test

train, xval, test = train_test_split(aggr_day, "day")
train, xval, test = train_test_split(aggr_day_ampm, "ampm")
train, xval, test = train_test_split(aggr_day_hr, "hr")
train, xval, test = train_test_split(aggr_day_15, "15min")
