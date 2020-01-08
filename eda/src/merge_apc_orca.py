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
APCFILE='data/apc_route_agg.tsv'
ORCAFILE='data/orca_route_agg.tsv'
RTE_FILE = 'data/rte_bnd_to_cardinal_direction.csv'

rte_file = pd.read_csv(RTE_FILE, sep=',', dtype={'route_number': 'O'}) # headers source_agency_id,route_number,direction_descr,dir
apc_file = pd.read_csv(APCFILE, sep='\t', dtype={'rte': 'O'}).merge(rte_file, how="outer", left_on=['rte', 'dir'], right_on=['route_number', 'dir'])

apc_file = apc_file[apc_file.rte != '201'] # unknown route
apc_file = apc_file[apc_file.rte != '200'] #Mark recommended removing

apc_file.loc[(apc_file.rte == '540') & (apc_file.dir == 'W'), 'direction_descr'] = 'Inbd'  # redmond to the ID
apc_file.loc[(apc_file.rte == '540') & (apc_file.dir == 'E'), 'direction_descr'] = 'Outbd'

apc_file.loc[(apc_file.rte == '204') & (apc_file.dir == 'N'), 'direction_descr'] = 'Inbd'  # mercer island
apc_file.loc[(apc_file.rte == '204') & (apc_file.dir == 'S'), 'direction_descr'] = 'Outbd'

apc_file.loc[(apc_file.rte == '224') & (apc_file.dir == 'W'), 'direction_descr'] = 'Inbd'  # Bellevue, Redmond to Duvall
apc_file.loc[(apc_file.rte == '224') & (apc_file.dir == 'E'), 'direction_descr'] = 'Outbd'

apc_file.loc[(apc_file.rte == '45') & (apc_file.dir == 'E'), 'direction_descr'] = 'Inbd'  # UW to loyal way
apc_file.loc[(apc_file.rte == '45') & (apc_file.dir == 'W'), 'direction_descr'] = 'Outbd'

apc_file.loc[(apc_file.rte == '676') & (apc_file.dir == 'W'), 'direction_descr'] = 'Inbd'  # RapidRide F Burian to Renton
apc_file.loc[(apc_file.rte == '676') & (apc_file.dir == 'E'), 'direction_descr'] = 'Outbd'

apc_file = apc_file[~pd.isnull(apc_file.opd_date)]
apc_file = apc_file[~((apc_file.rte=='45') & (apc_file.dir=='N'))]  # incorrect codings
apc_file = apc_file[~((apc_file.rte=='676') & (apc_file.dir=='N'))]  # incorrect codings
missing_directions = apc_file[pd.isnull(apc_file.direction_descr)][['rte', 'dir']].drop_duplicates()
assert missing_directions.shape[0] == 0  # YAY NO MORE MISSING DIRECTIONS
#missing_directions.to_csv('data/apc_missing_rte_bounds.csv', sep=',', index=False)


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
orca_file = orca_file[orca_file.route_number != 'BRT']
orca_file = orca_file[orca_file.route_number != '90'] # snow shuttle
orca_file = orca_file[orca_file.route_number != '201'] # ??? not a kcm route
orca_file = orca_file[orca_file.route_number != '444'] # ??? not a kcm route
orca_file = orca_file[orca_file.route_number != '200'] # Mark recommended removing
orca_file = pd.merge(
    orca_file,
    rte_file, 
    how='outer', 
    left_on=['route_number', 'direction_descr'], 
    right_on=['route_number', 'direction_descr']
)

orca_file.loc[(orca_file.route_number == '204') & (orca_file.direction_descr == 'Inbd'), 'dir'] = 'N'  # mercer island
orca_file.loc[(orca_file.route_number == '204') & (orca_file.direction_descr == 'Outbd'), 'dir'] = 'S'

orca_file.loc[(orca_file.route_number == '224') & (orca_file.direction_descr == 'Inbd'), 'dir'] = 'W'  # Bellevue, Redmond to Duvall
orca_file.loc[(orca_file.route_number == '224') & (orca_file.direction_descr == 'Outbd'), 'dir'] = 'E'

orca_file = orca_file[~pd.isnull(orca_file.txn_date)]
orca_file = orca_file[~pd.isnull(orca_file.txn_date)]
orca_file = orca_file[orca_file.txn_date <= '2019-03-03']
orca_missing_directions = orca_file[pd.isnull(orca_file.dir)][['route_number', 'direction_descr']].drop_duplicates()
assert orca_missing_directions.shape[0] == 0 # YAY NO MISSING DIRECTIONS
#orca_missing_directions.to_csv('data/orca_missing_rte_bounds.csv', sep=',', index=False)


apc_file['merge_key'] = apc_file.rte + '/' + apc_file.dir + '/' + apc_file.opd_date + '/' + apc_file.is_am.apply(lambda x: 'AM' if x else 'PM')
orca_file['merge_key'] = orca_file.route_number + '/' + orca_file.dir + '/' + orca_file.txn_date + '/' + orca_file.is_am.apply(lambda x: 'AM' if x else 'PM')

merged = pd.merge(
    apc_file, 
    orca_file, 
    how="outer",
    indicator=True,
    on="merge_key",
    suffixes=("_apc", "_orca")
)

# Missing from Orca in APC
merged[(merged['_merge'] == 'left_only')]  # 1414 route/direction/half-day combos
# examples:
# 2/S/2019-02-12/PM             Orca missing for this route from the 12th to the 14th
# 74/N/2019-01-07/AM            Orca has data for this route in PM but not AM
# 556/W/2019-02-26/AM           Route 556 entirely missing from Orca (Sound Transit express route?)

merged[(merged['_merge'] == 'left_only')][['merge_key', 'rte', 'dir_apc', 'opd_date', 'is_am_apc']].to_csv("data/apc_missing_in_orca.tsv", sep='\t', index=False)

# Missing from APC in Orca
merged[(merged['_merge'] == 'right_only')]  # 3927 route/dir/half-day combos
# 70/S/2019-02-02/AM            Feb 2,3,4 missing from APC for this route
# 73/S/2019-01-29/PM            Jan 29 missing from APC for this route
# 24/N/2019-02-08/AM            Feb 8 missing from APC for this route
# 186/W/2019-01-31/PM           Jan 30 and 31 missing from APC for this route

merged[(merged['_merge'] == 'right_only')][['merge_key', 'route_number_orca', 'dir_orca', 'txn_date', 'is_am_orca']].to_csv("data/orca_missing_in_apc.tsv", sep='\t', index=False)

merged.to_csv("data/merged_apc_and_orca.tsv", sep='\t', index=False)