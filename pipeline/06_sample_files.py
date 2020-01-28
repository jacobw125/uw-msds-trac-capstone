#!/usr/bin/env python3

import pandas as pd

# The goal here is to send data for about 30 trips 
# half on the same route, let's say 49
# the 49 data should be 1-2 days
# everything else is randomly chosen
# Send APC, Orca, and merged

days_to_keep = [f'2019-03-{day:02d}' for day in range(1, 3)] # 2019-03-01, 02
# Apc data
apc = pd.concat([
    pd.read_csv(f"data/apc/{days_to_keep[0]}.tsv.gz", sep="\t"),
    pd.read_csv(f"data/apc/{days_to_keep[1]}.tsv.gz", sep="\t")
])
apc = apc[apc.rte==49]
apc.to_csv("data/for_mark/49_apc.tsv.gz", sep="\t", compression="gzip", index=False)


# Orca data
orca = pd.concat([
    pd.read_csv(f"data/orca/{days_to_keep[0]}.tsv.gz", sep="\t"),
    pd.read_csv(f"data/orca/{days_to_keep[1]}.tsv.gz", sep="\t")
])
orca = orca[orca.route_number==49][['passenger_count', 'device_id', 'stop_error', 'trip_id', 'stop_time',
       'service_agency_name', 'stop_lon', 'direction_descr',
       'source_agency_name', 'stop_id', 'origin_location_descr',
       'txn_type_descr', 'business_date', 'day_of_week',
       'upgrade_indicator', 'mode_descr',
       'device_place_name', 'is_rapidride', 'route_number', 'biz_txn_diff',
       'ceffv', 'stop_lat', 'agency_trip_id',
       'txn_dtm_pacific', 'transit_operator_abbrev',
       'destination_location_descr', 'device_location_descr']]

orca.to_csv("data/for_mark/49_orca.tsv.gz", sep="\t", compression="gzip", index=False)

# Orca agg
orca_agg = pd.concat([
    pd.read_csv(f"data/orca_agg/{days_to_keep[0]}.tsv.gz", sep="\t"),
    pd.read_csv(f"data/orca_agg/{days_to_keep[1]}.tsv.gz", sep="\t")
])
orca_agg = orca_agg[orca_agg.route_number==49]
orca_agg.to_csv("data/for_mark/49_orca_aggregated.tsv.gz", sep="\t", compression="gzip", index=False)

# Merged stops level
merged = pd.read_csv("data/merged_stops.tsv.gz", sep="\t")
merged = merged[(merged.rte==49) & (merged.opd_date.isin(days_to_keep))]
merged.to_csv("data/for_mark/49_merged_at_stop_level.tsv.gz", sep="\t", compression="gzip", index=False)



# Random sample a number of trip IDs from a single day.

apc = pd.read_csv(f"data/apc/2019-01-15.tsv.gz", sep="\t")
random_ids = apc.trip_id.sample(5, random_state=1234)
apc = apc[apc.trip_id.isin(random_ids)]
apc.to_csv("data/for_mark/RS_apc.tsv.gz", sep="\t", index=False, compression="gzip")


orca = pd.read_csv(f"data/orca/2019-01-15.tsv.gz", sep="\t")
orca = orca[orca.trip_id.isin(random_ids)][['passenger_count', 'device_id', 'stop_error', 'trip_id', 'stop_time',
       'service_agency_name', 'stop_lon', 'direction_descr',
       'source_agency_name', 'stop_id', 'origin_location_descr',
       'txn_type_descr', 'business_date', 'day_of_week',
       'upgrade_indicator', 'mode_descr',
       'device_place_name', 'is_rapidride', 'route_number', 'biz_txn_diff',
       'ceffv', 'stop_lat', 'agency_trip_id',
       'txn_dtm_pacific', 'transit_operator_abbrev',
       'destination_location_descr', 'device_location_descr']]
orca.to_csv("data/for_mark/RS_orca.tsv.gz", sep="\t", index=False, compression="gzip")

orca_agg = pd.read_csv(f"data/orca_agg/2019-01-15.tsv.gz", sep="\t")
orca_agg = orca[orca.trip_id.isin(orca_agg)]
orca.to_csv("data/for_mark/RS_orca_aggregated.tsv.gz", sep="\t", index=False, compression="gzip")

merged = pd.read_csv("data/merged_stops.tsv.gz", sep="\t")
merged = merged[(merged.trip_id.isin(random_ids)) & (merged.opd_date == "2019-01-15")]
merged.to_csv("data/for_mark/RS_merged_at_stop_level.tsv.gz", sep="\t", compression="gzip", index=False)
