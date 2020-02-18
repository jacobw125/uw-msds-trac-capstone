# JW - Clustering similar routes and perf in those clusters, weekends / not, over time
# Alex - route type BRT / not, specific route examples.
# Goal wed next week 6

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from sys import argv
from os import makedirs

plt.style.use('seaborn')

rte_clusters = pd.read_csv('rte_clusters.tsv', sep='\t')

if len(argv) != 5:
    raise ValueError("Expected args: <training or crossvalidation.tsv[.gz]> <predictions.txt> <x time var> <batch name>")

_, xval_fname, predictions_fname, x_timevar, batch = argv

features = pd.merge(pd.read_csv(xval_fname, sep='\t'), rte_clusters, how='left', on='rte')

def make_pretty(x):
    hr, min = x.split("_", 1)
    hr = int(hr)
    min = int(min)
    if hr > 24:
        print(f"weird hour: {x}")
    ampm = "AM"
    if hr >= 12:
        ampm = 'PM'
    if hr == 0:
        hr = 12
    if hr > 12:
        hr = hr - 12
    
    return f'{hr}:{min:02d} {ampm}'

features['x_pretty'] = features[x_timevar].apply(make_pretty)

with open(predictions_fname, 'rt') as fh:
    predictions = np.array([float(l.strip('\[\] \n')) for l in fh])

assert len(predictions) == len(features), f"Data and predictions need to be the same shape. Data: {len(features)}, predictions: {len(predictions)}"

def mape(predicted, actual):
    ae = np.abs(actual-predicted)
    return (ae[actual != 0]/actual[actual != 0]).mean()

def mae(predicted, actual):
    ae = np.abs(actual-predicted)
    return ae.mean()

groups = list()
maes = list()
mapes = list()
print(f"group\tmae\tmape")
for trip_freq in rte_clusters.trip_freq.unique():
    actual = features.loc[features['trip_freq'] == trip_freq, 'ons']
    preds = predictions[features['trip_freq'] == trip_freq]
    groups.append(trip_freq + ' frequency route')
    maes.append(mae(preds, actual))
    mapes.append(mape(preds, actual))
    #print(f"{trip_freq} freq\t{mae(preds, actual):.2f}\t{mape(preds, actual):.2%}")


if "is_inbound" in features.columns:
    for x in (True, False):
        actual = features.loc[features['is_inbound'] == x, 'ons']
        preds = predictions[features['is_inbound'] == x]
        groups.append('Inbd trip' if x else 'Outbd trip')
        maes.append(mae(preds, actual))
        mapes.append(mape(preds, actual))
        #print(f"{'Inbd' if x else 'Outbd'}\t{mae(preds, actual):.2f}\t{mape(preds, actual):.2%}")

# Weekday / weekend
for x in (0., 1.):
    actual = features.loc[features['is_weekend'] == x, 'ons']
    preds = predictions[features['is_weekend'] == x]
    groups.append('Weekend' if x else 'Weekday')
    maes.append(mae(preds, actual))
    mapes.append(mape(preds, actual))

for x in (True, False):
    actual = features.loc[features['is_ns'] == x, 'ons']
    preds = predictions[features['is_ns'] == x]
    groups.append('NS route' if x else 'EW route')
    maes.append(mae(preds, actual))
    mapes.append(mape(preds, actual))
    #print(f"{'NS' if x else 'EW'}\t{mae(preds, actual):.2f}\t{mape(preds, actual):.2%}")

features['timeblock'] = np.nan
features['hr'] = features[x_timevar].apply(lambda x: int(x.split('_')[0]))
features.loc[(features['is_weekend'] == 0.) & (features['hr'] < 4), 'timeblock'] = 'Early Morning'
features.loc[(features['is_weekend'] == 0.) & (features['hr'] >= 4) & (features['hr'] < 10), 'timeblock'] = 'Morning Peak'
features.loc[(features['is_weekend'] == 0.) & (features['hr'] >= 10) & (features['hr'] < 15), 'timeblock'] = 'Midday'
features.loc[(features['is_weekend'] == 0.) & (features['hr'] >= 15) & (features['hr'] < 19), 'timeblock'] = 'Evening Peak'
features.loc[(features['is_weekend'] == 0.) & (features['hr'] >= 19), 'timeblock'] = 'Night'

features.loc[(features['is_weekend'] == 1.) & (features['hr'] < 6), 'timeblock'] = 'Weekend Morning'
features.loc[(features['is_weekend'] == 1.) & (features['hr'] >= 6) & (features['hr'] < 8), 'timeblock'] = 'Weekend Peak'
features.loc[(features['is_weekend'] == 1.) & (features['hr'] >= 8), 'timeblock'] = 'Weekend Evening'

time_blocks = ('Early Morning', 'Morning Peak', 'Midday', 'Evening Peak', 'Weekend Morning', 'Weekend Peak', 'Weekend Evening')
for x in time_blocks:
    actual = features.loc[features['timeblock'] == x, 'ons']
    preds = predictions[features['timeblock'] == x]
    groups.append(x)
    maes.append(mae(preds, actual))
    mapes.append(mape(preds, actual))
    #print(f"{x}\t{mae(preds, actual):.2f}\t{mape(preds, actual):.2%}")

err_rates = pd.DataFrame({'group': groups, 'mae': maes, 'mape': mapes}).sort_values(['mae'], ascending=False)
print(batch)
print(err_rates)

features['mae'] = (predictions - features['ons']).abs()
by_rte = features.groupby(['rte', 'dir']).agg({'mae': 'mean'}).sort_values('mae', ascending=True)
print("Best performing routes:")
print(by_rte.head())

print("Worst performing routes performing routes:")
by_rte = features.groupby(['rte', 'dir']).agg({'mae': 'mean'}).sort_values('mae', ascending=False)
print(by_rte.head())

if 'summer' in features.columns:  # if this is combined
    print("By season:")
    by_season = features.groupby(['summer']).agg({'mae': 'mean'}).sort_values('mae', ascending=True)
    print(by_season)

makedirs(f'./{batch}', exist_ok=True)
for time in time_blocks:
    plt.figure(figsize=(10,8))
    plt.xticks(rotation=45)
    plt.title(f"Residuals over time, {time}")
    
    selector = (features['timeblock'] == str(time)) & (features['is_ns'] == 1)
    feature_grp = features.loc[selector]
    preds_grp = predictions[selector]
    x = feature_grp[x_timevar]
    y = preds_grp - feature_grp['ons'].to_numpy()
    plt.scatter(x, y, alpha=0.2, label='NS route')

    selector = (features['timeblock'] == str(time)) & ~(features['is_ns'] == 1)
    feature_grp = features.loc[selector]
    preds_grp = predictions[selector]
    x = feature_grp[x_timevar]
    y = preds_grp - feature_grp['ons'].to_numpy()
    plt.scatter(x, y, alpha=0.2, label='EW route')

    plt.legend()
    grpname = time.lower().replace(" ", "_")
    plt.savefig(f'./{batch}/direction_{grpname}.png', bbox_inches='tight')
    plt.close()
    
if 'is_inbound' in features.columns:
    for time in time_blocks:
        plt.figure(figsize=(10,8))
        plt.xticks(rotation=45)
        plt.title(f"Residuals over time, {time}")
        
        selector = (features['timeblock'] == str(time)) & (features['is_inbound'] == 1)
        feature_grp = features.loc[selector]
        preds_grp = predictions[selector]
        x = feature_grp[x_timevar]
        y = preds_grp - feature_grp['ons'].to_numpy()
        plt.scatter(x, y, alpha=0.2, label='Inbound trip')

        selector = (features['timeblock'] == str(time)) & ~(features['is_inbound'] == 1)
        feature_grp = features.loc[selector]
        preds_grp = predictions[selector]
        x = feature_grp[x_timevar]
        y = preds_grp - feature_grp['ons'].to_numpy()
        plt.scatter(x, y, alpha=0.2, label='Outbound trip')

        plt.legend()
        grpname = time.lower().replace(" ", "_")
        plt.savefig(f'./{batch}/bnd_{grpname}.png', bbox_inches='tight')
        plt.close()

for time in time_blocks:
    plt.figure(figsize=(10,8))
    plt.xticks(rotation=45)
    plt.title(f"Residuals over time, {time}")
    for trip_freq in rte_clusters.trip_freq.unique():
        selector = (features['timeblock'] == str(time)) & (features['trip_freq'] == str(trip_freq))
        feature_grp = features.loc[selector]
        preds_grp = predictions[selector]
        x = feature_grp[x_timevar]
        y = preds_grp - feature_grp['ons'].to_numpy()
        plt.scatter(x, y, alpha=0.2, label=trip_freq + " frequency route")
    plt.legend()
    grpname = time.lower().replace(" ", "_")
    plt.savefig(f'./{batch}/freq_{grpname}.png', bbox_inches='tight')
    plt.close()

# One big overall plot
import matplotlib.ticker as ticker

ticks, labels = list(), list()
i = 0
for hr in range(0, 25):
    for min in ('00', '15', '30', '45'):
        ticks.append(i)
        labels.append(make_pretty(f'{hr}_{min}'))
        i+=1

plt.figure(figsize=(28, 12))
plt.xticks(ticks, labels, rotation=45)
plt.xlim(1, i-4)
plt.title("Cross-validation performance (Weekday)")
plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(base=1.0))
for trip_freq in rte_clusters.trip_freq.unique():
    selector = (features['is_weekend'] == 0.) & (features['trip_freq'] == trip_freq)
    plt.scatter(
        features.loc[selector, 'x_pretty'], 
        predictions[selector] - features.loc[selector, 'ons'], 
        alpha=0.2, 
        s=2*features.loc[selector, 'ons'], 
        label=f'{trip_freq} freq route')
plt.ylim(-250, 250)
plt.ylabel("Predicted - Actual")
plt.legend()
plt.savefig(f"./{batch}/overall_perf_weekdays.png", bbox_inches='tight')

# One big overall plot
plt.figure(figsize=(28, 12))
plt.xticks(ticks, labels, rotation=45)
plt.xlim(1, i-4)
plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(base=1.0))
plt.title("Cross-validation performance (Weekend)")
for trip_freq in rte_clusters.trip_freq.unique():
    selector = (features['is_weekend'] == 1.) & (features['trip_freq'] == trip_freq)
    plt.scatter(
        features.loc[selector, 'x_pretty'], 
        predictions[selector] - features.loc[selector, 'ons'], 
        alpha=0.3, 
        s=2*features.loc[selector, 'ons'], 
        label=f'{trip_freq} freq route')
plt.ylim(-100, 100)
plt.ylabel("Predicted - Actual")
plt.legend()
plt.savefig(f"./{batch}/overall_perf_weekends.png", bbox_inches='tight')


if 'summer' in features.columns:  # if this is combined
    for season in (1., 0.):
        # One big overall plot for summer or winter only
        plt.figure(figsize=(28, 12))
        plt.xticks(ticks, labels, rotation=45)
        plt.xlim(1, i-4)
        plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(base=1.0))
        plt.title(f"{'summer' if season == 1.0 else 'winter'} cross-validation performance (Weekend)")
        for trip_freq in rte_clusters.trip_freq.unique():
            selector = (features['is_weekend'] == 1.) & (features['trip_freq'] == trip_freq) & (features['summer'] == season)
            plt.scatter(
                features.loc[selector, 'x_pretty'], 
                predictions[selector] - features.loc[selector, 'ons'], 
                alpha=0.3, 
                s=2*features.loc[selector, 'ons'], 
                label=f'{trip_freq} freq route')
        plt.ylim(-100, 100)
        plt.ylabel("Predicted - Actual")
        plt.legend()
        plt.savefig(f"./{batch}/{'summer' if season == 1.0 else 'winter'}_perf_weekends.png", bbox_inches='tight')
