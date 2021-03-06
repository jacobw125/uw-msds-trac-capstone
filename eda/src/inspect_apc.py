#!/usr/bin/env python3
import gzip
import csv
from sys import argv
from math import inf
from collections import defaultdict
from os import makedirs
from typing import Dict

if len(argv) != 2:
    raise ValueError("Missing input file")

COLS_TO_COUNT_UNIQUE = [
    'trip_id',
    'pattern_id',
    'blk',
    'vehicle_id',
    'stop_id',
    'geom'
]

COLS_TO_PRINT_UNIQUE = [
    'rte',
    'dir',
    'stop_seq',
    'apc_veh'
]

COLS_TO_CORRELATE = {
    'stop_id': 'stop_name'
}

NUM_COLS_TO_DESCRIBE = [
    'sch_st_min',
    'pattern_quality',
    'pattern_quality_1',
    'sch_stop_sec',
    'act_stop_arr',
    'dwell_sec',
    'doors_open',
    'door_open_sec',
    'ons',
    'offs',
    'load',
    'gps_lat',
    'gps_long'
]

class NumericDescription():
    def __init__(self):
        self._min = None
        self._max = None
        self._sum = 0.
        self._n = 0
        self._nan = 0
        self._blank = 0
        self._null = 0
    
    def process(self, val):
        if val is None:
            self._null += 1
            return
        elif str(val).strip() == '':
            self._blank += 1
            return
        try:
            val = float(val)
        except:
            self._nan += 1
            return
        if self._min is None or val < self._min: self._min = val
        if self._max is None or val > self._max: self._max = val
        self._sum += val
        self._n += 1

    def __str__(self):
        return f"""
        min/max/avg: {self._min} / {self._max} / {self._sum / self._n if self._n > 0 else 'NA'}
        n. null:    {self._null}
        n. blank:   {self._blank}
        n. nan:     {self._nan}
        n. numeric: {self._n}
        """


uniques = { col: set() for col in (COLS_TO_COUNT_UNIQUE + COLS_TO_PRINT_UNIQUE) }
correlates = { (col1, col2): defaultdict(set) for col1, col2 in COLS_TO_CORRELATE.items() }
num_descs = { col: NumericDescription() for col in NUM_COLS_TO_DESCRIBE }

def process_row(row: Dict):
    for col in uniques.keys():
        val = row[col]
        uniques[col].add(val if val is not None else 'NULL')
    for col1, col2 in COLS_TO_CORRELATE.items():
        val_col1, val_col2 = row[col1], row[col2]
        if val_col1 is None: val_col1 = 'NULL'
        if val_col2 is None: val_col2 = 'NULL'
        correlates[(col1, col2)][val_col1].add(val_col2)
    for col in NUM_COLS_TO_DESCRIBE:
        num_descs[col].process(row[col])

print(f"Processing file {argv[1]}")
n=0
with (gzip.open(argv[1], 'rt') if argv[1].endswith('.gz') else open(argv[1], 'rt')) as fh:
    for row in csv.DictReader(fh):
        process_row(row)
        n += 1
        if n % 1e6 == 0:
            print(f"    {n/1e6}M lines")
        
print(f"Finished processing {n} lines.")
print(f"Writing reports")

makedirs('reports/correlates', exist_ok=True)
for (c1, c2) in correlates.keys():
    with open(f'reports/correlates/{c1}-VS-{c2}.tsv', 'w') as fh:
        fh.write('\t'.join([c1, c2]) + '\n')
        for c1_val, c2_list in correlates[(c1, c2)].items():
            for c2_val in c2_list:
                fh.write('\t'.join([c1_val, c2_val]) + '\n')

with open("reports/unique_counts.tsv", 'w') as fh:
    fh.write('\t'.join(['col', 'n_unique']) + '\n')
    for col in COLS_TO_COUNT_UNIQUE:
        fh.write('\t'.join([col, str(len(uniques[col]))]) + '\n')

with open("reports/unique_vals.tsv", 'w') as fh:
    fh.write('\t'.join(['col', 'val']) + '\n')
    for col in COLS_TO_PRINT_UNIQUE:
        for val in sorted(uniques[col]):
            fh.write('\t'.join([col, val]) + '\n')

with open("reports/numerics.tsv", 'w') as fh:
    fh.write('\t'.join(['col', 'n_null', 'n_blank', 'n_nan', 'n', 'min', 'max', 'avg']) + '\n')
    for (col, num_desc) in num_descs.items():
        fh.write('\t'.join([
            col,
            str(num_desc._null),
            str(num_desc._blank),
            str(num_desc._nan),
            str(num_desc._n),
            str(num_desc._min),
            str(num_desc._max),
            '{:.4f}'.format((num_desc._sum / num_desc._n)) if num_desc._n > 0 else 'NA'
        ]) + '\n')
