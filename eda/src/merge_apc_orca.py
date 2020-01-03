import gzip
import csv
from sys import argv
import pandas as pd
import numpy as np
from csv import DictReader
from collections import defaultdict

if len(argv) != 3: raise ValueError("Expected two args: APC and Orca TSV files")

APCFILE = argv[1]  # or  APCFILE='data/apc_route_agg.tsv'
ORCAFILE = argv[2]  # or ORCAFILE='data/orca_route_agg.tsv'
RTE_FILE = 'data/rte_dir_desc.tsv'

rte_file = pd.read_csv(RTE_FILE, sep='\t')
apc_file = pd.read_csv(APCFILE, sep='\t').merge(rte_file, how="outer", on=['rte', 'dir'])
missing_directions = apc_file[pd.isnull(apc_file.description)][['rte', 'dir']].drop_duplicates()


# What if we went the other direction?
orca_file = pd.merge(pd.read_csv(ORCAFILE, sep='\t'), rte_file, how='outer', left_on=['route_number', 'direction_descr'], right_on=['rte','description'])
orca_missing_directions = orca_file[pd.isnull(orca_file.dir)][['rte', 'description']].drop_duplicates()

merged = pd.merge(apc_file, orca_file, how="outer", on={'rte': 'route_number', ''})

n=0
with get_fh(APCFILE) as fh:
    csvr = DictReader(fh, delimiter='\t')
    for row in csvr:
        apc_trips[row['trip_id']][row['rte']].add(row['dir'])
        n+=1

print(f"Processed {n} lines from {APCFILE}")

apc_trip, apc_rte, apc_dir = list(), list(), list()
for trip_id, rtedict in apc_trips.items():
    for rte_id, direct in rtedict.items():
        if len(direct) > 1: 
            print(f"Warning: APC trip {trip_id} route {rte_id} has >1 direction: {direct}")
            continue
        apc_trip.append(trip_id)
        apc_rte.append(rte_id)
        apc_dir.append(list(direct)[0])

apc = pd.DataFrame({'trip_id': apc_trip, 'rte': apc_rte, 'dir': apc_dir})


n=0
with get_fh(ORCAFILE) as fh:
    csvr = DictReader(fh, delimiter='\t')
    for row in csvr:
        orca_trips[row['trip_id']][row['route_number']].add(row['direction_descr'])
        n+=1

print(f"Processed {n} lines from {ORCAFILE}")


orca_trip, orca_rte, orca_dir = list(), list(), list()
for trip_id, rtedict in orca_trips.items():
    if trip_id == '': continue
    for rte_id, desc in rtedict.items():
        if len(desc) > 1:
            print(f"Warning: ORCA trip {trip_id} route {rte_id} has >1 description: {desc}")
            continue
        if len(desc) == 0:
            print(f"Warning: ORCA trip {trip_id} route {rte_id} has 0-length descriptions: {desc}")
            continue
        orca_trip.append(trip_id)
        orca_rte.append(rte_id)
        orca_dir.append(list(desc)[0])

orca = pd.DataFrame({'trip_id': orca_trip, 'rte': orca_rte, 'description': orca_dir})


merged = pd.merge(apc, orca, how='outer', on=['trip_id', 'rte'])[['rte', 'dir', 'description']].drop_duplicates()

merged.to_csv('data/rte_dir_desc.tsv', sep='\t', index=False)
