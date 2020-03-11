"""
Contains functions used throughout pipeline.
"""

def is_rapidride(rte_number):
    """
    Determines if route is a rapid ride. Routes > 600.
    :params rte_number variable:
    :returns boolean:
    """
    result = False

    try:
        result = int(rte_number) > 600
    except ValueError:
        pass

    return str(result)

def null_if_empty(var):
    """
    Sets empty variables as NULL.
    :params var variable:
    :returns variable:
    """
    result = var

    if var == '':
        result = None

    return result

def int_or_zero(var):
    """
    Tries to convert input to an int, else 0.
    :params var variable:
    :returns int:
    """
    result = 0

    try:
        result = int(var)
    except ValueError:
        pass

    return result

def keep_row(data_row, key1, key2, key3, src):
    """
    Determines if row data row should be keep. Route number shouold be <600
    or 671-676. If APC data, 'apc_veh' = Y. If ORCA, 'mode_id' in ('128', '250')
    and service_agency_id = 4.
    :params data_row dictionary:
    :params key1 string
    :params key2 string
    :params key3 string
    :params src string
    :returns boolean:
    """
    rte_num = -1
    result = False

    try:
        rte_num = int(data_row[key1])
    except ValueError:
        pass

    if (-1 < rte_num < 600 or (671 <= rte_num <= 676)):
        if src == 'apc':
            result = data_row[key2] == 'Y'
        else:
            result = str(data_row[key2]) in ('128', '250') and str(data_row[key3]) == '4'

    return result
