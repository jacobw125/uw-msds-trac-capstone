#!/bin/bash

# Plots for neural net

mkdir nn_15min_combined
python3 01_report_performance.py ../combined_data/15min/xval.tsv.gz ../predictions/nn_15min_combined_xval.txt trip_start_hr_15 nn_15min_combined | tee nn_15min_combined/perf.txt

mkdir nn_15min_winter
python3 01_report_performance.py ../winter_data/aggregates/15min/xval.tsv.gz ../predictions/nn_15min_winter_xval.txt trip_start_hr_15 nn_15min_winter | tee nn_15min_winter/perf.txt

mkdir nn_15min_summer
python3 01_report_performance.py ../summer_data/aggregates/15min/xval.tsv.gz ../predictions/nn_15min_summer_xval.txt trip_start_hr_15 nn_15min_summer | tee nn_15min_summer/perf.txt



mkdir nn_30min_combined
python3 01_report_performance.py ../combined_data/30min/xval.tsv.gz ../predictions/nn_30min_combined_xval.txt trip_start_hr_30 nn_30min_combined | tee nn_30min_combined/perf.txt

mkdir nn_30min_winter
python3 01_report_performance.py ../winter_data/aggregates/30min/xval.tsv.gz ../predictions/nn_30min_winter_xval.txt trip_start_hr_30 nn_30min_winter | tee nn_30min_winter/perf.txt

mkdir nn_30min_summer
python3 01_report_performance.py ../summer_data/aggregates/30min/xval.tsv.gz ../predictions/nn_30min_summer_xval.txt trip_start_hr_30 nn_30min_summer | tee nn_30min_summer/perf.txt



# Plots and analysis for xgboost
mkdir xgb_30min_summer
python3 01_report_performance.py ../summer_data/aggregates/30min/xval.tsv.gz ../predictions/.txt trip_start_hr_30 xgb_30min_summer | tee xgb_30min_summer/perf.txt

mkdir xgb_30min_winter
python3 01_report_performance.py ../winter_data/aggregates/30min/xval.tsv.gz ../predictions/.txt trip_start_hr_30 xgb_30min_winter | tee xgb_30min_winter/perf.txt

mkdir xgb_30min_combined
python3 01_report_performance.py ../combined_data/30min/xval.tsv.gz          ../predictions/.txt trip_start_hr_30 xgb_30min_combined | tee xgb_30min_combined/perf.txt



# Plots and analysis for SVM
mkdir nn_30min_summer
python3 01_report_performance.py ../summer_data/aggregates/30min/xval.tsv.gz ../predictions/nn_30min_summer_xval.txt trip_start_hr_30 nn_30min_summer | tee nn_30min_summer/perf.txt
