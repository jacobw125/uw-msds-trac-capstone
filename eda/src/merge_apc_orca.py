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
RTE_FILE = 'data/rte_bnd_to_cardinal_direction.csv'

rte_file = pd.read_csv(RTE_FILE, sep=',', dtype={'route_number': 'O'}) # headers source_agency_id,route_number,direction_descr,dir
apc_file = pd.read_csv(APCFILE, sep='\t', dtype={'rte': 'O'}).merge(rte_file, how="outer", left_on=['rte', 'dir'], right_on=['route_number', 'dir'])
missing_directions = apc_file[pd.isnull(apc_file.direction_descr)][['rte', 'dir']].drop_duplicates()
missing_directions.to_csv('data/apc_missing_rte_bounds.csv', sep=',', index=False)


# What if we went the other direction?
orca_file = pd.read_csv(ORCAFILE, sep='\t', dtype={'route_number': 'O'})
def fix_route_number(x):
    try:
        return str(int(x))
    except:
        try:
            return str(int(float(x)))
        except:
            return x

orca_file['route_number'] = orca_file['route_number'].apply(fix_route_number)
orca_file = pd.merge(
    orca_file,
    rte_file, 
    how='outer', 
    left_on=['route_number', 'direction_descr'], 
    right_on=['route_number', 'direction_descr']
)

orca_missing_directions = orca_file[pd.isnull(orca_file.dir)][['route_number', 'direction_descr']].drop_duplicates()
orca_missing_directions.to_csv('data/orca_missing_rte_bounds.csv', sep=',', index=False)

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
