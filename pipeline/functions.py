'''
Contains functions used throughout pipeline.
'''

import numpy as np
import pandas as pd

import constants as c

def is_rapidride(rte_number):
    '''
    Determines if route is a rapid ride. Routes > 600.

    :params rte_number variable:
    :returns boolean:
    '''
    result = False

    try:
        result = int(rte_number) > 600
    except ValueError:
        pass

    return str(result)

def inf_or_nan(var):
    '''
    Checks if variable is infinity or NAN. Returns boolean.

    :params var variable:
    :returns boolean:
    '''

    return np.isnan(var) | np.isinf(var)

def remove_t_and_s(var):
    '''
    Cleans data by removing trailing s's and returning null when containing T's or *'s

    :params var variable:
    :returns float:
    '''

    if isinstance(var) == isinstance(0.1):
        return var
    if var.endswith('s'):
        var = var[:-1]
    if var in ('T', '*'):
        return np.nan
    return float(var)

def mean_input(col):
    '''
    Fills NULL values with column mean.

    :params col pd.Series:
    :returns pd.Series:
    '''

    return col.where(~pd.isnull(col), col.mean())

def agg_data(dataframe, group_by, aggs):
    '''
    Groups and aggregates a dataframe based on provided parameters.

    :params dataframe dataframe:
    :params group_by list:
    :params aggs dictionary:
    :returns dataframe:
    '''

    return dataframe.groupby(group_by).agg(aggs).reset_index().sort_values(group_by)

def null_if_empty(var):
    '''
    Sets empty variables as NULL.

    :params var variable:
    :returns variable:
    '''
    result = var

    if var == '':
        result = None

    return result

def int_or_zero(var):
    '''
    Tries to convert input to an int, else 0.

    :params var variable:
    :returns int:
    '''
    result = 0

    try:
        result = int(var)
    except ValueError:
        pass

    return result

def keep_row(data_row, key1, key2, key3, src):
    '''
    Determines if data row should be keep. Route number should be <600
    or 671-676, and:
        If APC, data is an APC vehicle.
        If ORCA, is specific mode IDs andand service agency ID.

    :params data_row dictionary:
    :params key1 string
    :params key2 string
    :params key3 string
    :params src string
    :returns boolean:
    '''
    rte_num = -1
    result = False

    try:
        rte_num = int(data_row[key1])
    except ValueError:
        pass

    if -1 < rte_num < 600 or 671 <= rte_num <= 676:
        if src == 'APC':
            result = data_row[key2] == 'Y'
        else:
            result = str(data_row[key2]) in c.ORCA_MODE_IDS \
                     and str(data_row[key3]) == c.ORCA_AGENCY_ID

    return result
