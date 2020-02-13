import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt
import matplotlib
from sys import argv
from os import makedirs

dowcNDict = {0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'}
RTC = [
    44, #ballard to udistrict
    71, #wedgewood to udistrict
    167, #renton to udistrict
    7 #rapid ride
]
imagePath = './images/'
dowcs = np.array([0,1,2,3,4,5,6]) # day of week codes

def genDOWCplots(rawData,routesToCheck = [44,71,167,7],save=False,offset=0.45):
    counter=0
    routeName=""
    fileName = imagePath
    for r in routesToCheck:
        print("Route: " + str(r))
        routeName=str(r)
        for d in dowcs:
            dowcn = dowcNDict[d]
            toPrintData = rawData.iloc[(rawData.rte==r).values & (rawData.day_of_week==d).values].sort_values(by='trip_start_hr_30')
            allAvg = toPrintData.groupby(by='trip_start_hr_30').mean()[['preds','ons']]
            plt.figure(figsize=(28, 12))
            plt.xticks(ticks, labels, rotation=45)
            plt.bar(x=allAvg.index,height=allAvg.values[:,0],label='preds',width=offset,align='edge')
            plt.bar(x=allAvg.index+offset,height=allAvg.values[:,1],width=offset,label='actuals',align='edge')
            plt.xlabel("Half hour increment")
            plt.ylabel("Passenger Count")
            plt.title("Passenger count on route "+routeName+" throughout " + dowcNDict[d])
            if(len(allAvg.values[:,0])>2):
                plt.legend()
            if(save):
                fileName = imagePath+routeName+"_"+dowcn+"barcount.jpg"
                plt.savefig(fileName)
            else:
                plt.show()
            plt.close()

def genAvgABSE(rawData,routesToCheck = [44,71,167,7],save=False,offset=0.45):
    counter=0
    routeName=""
    fileName = imagePath
    for r in routesToCheck:
        print("Route: " + str(r))
        routeName=str(r)
        for d in dowcs:
            dowcn = dowcNDict[d]
            toPrintData = rawData.iloc[(rawData.rte==r).values & (rawData.day_of_week==d).values].sort_values(by='trip_start_hr_30')
            toPrintData['adiff'] = abs(toPrintData.preds-toPrintData.ons)
            mDiffs = toPrintData.groupby('trip_start_hr_30').mean()
            plt.figure(figsize=(28, 12))
            plt.xticks(ticks, labels, rotation=45)
            plt.bar(x=mDiffs.index,height=mDiffs.adiff)
            plt.xlabel("Half hour increment")
            plt.ylabel("Absolute Error")
            plt.title("Avg Absolute Error on route "+routeName+" throughout " + dowcNDict[d])
            if(save):
                fileName = imagePath+routeName+"_"+dowcn+"abserror.jpg"
                plt.savefig(fileName)
            else:
                plt.show()
            plt.close()


def make_pretty(x):
    # stolen from jacob, thanks for this
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

argv = ['', './jacobsAgg/winterxval.tsv', './alexXGBpreds.csv']
if len(argv) < 3:
    raise ValueError("Expected args: <training or crossvalidation.tsv[.gz]>, predictions.csv")

dataFile = argv[1]
predFile = argv[2]
rawData = pd.read_csv(dataFile,sep='\t')
rawData['opd_date'] = pd.to_datetime(rawData['opd_date'])
rawData['month'] = rawData['opd_date'].dt.month
rawData['preds'] = np.genfromtxt(predFile, delimiter=',')
print(rawData.head())

conv = {}
counter =0
for x in (np.unique(rawData.trip_start_hr_30)):
    conv[x] = counter
    counter+=1
rawData['trip_start_hr_30'] = rawData['trip_start_hr_30'].replace(conv)


'''
This sets up our matplotlib environment
'''
ticks, labels = list(), list()
i = 0
for hr in range(0, 25):
    for min in ('00', '30'):
        ticks.append(i)
        labels.append(make_pretty(f'{hr}_{min}'))
        i+=1
offset=0.45
font = {'family' : 'normal',
        'size'   : 16}

matplotlib.rc('font', **font)

genAvgABSE(rawData,save=True)

genDOWCplots(rawData,save=True)
