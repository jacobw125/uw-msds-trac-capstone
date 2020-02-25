#!/bin/bash
rm batch_compare.tsv

###################### Final models (test)
mkdir final_nn_15min
python3 01_report_performance.py ../combined_data/15min/test.tsv.gz ../predictions/final_nn_15min_test.txt trip_start_hr_15 final_nn_15min | tee final_nn_15min/perf.txt

mkdir final_nn_30min
python3 01_report_performance.py ../combined_data/30min/test.tsv.gz ../predictions/final_nn_30min_test.txt trip_start_hr_30 final_nn_30min | tee final_nn_30min/perf.txt

mkdir final_nn_hr
python3 01_report_performance.py ../combined_data/hr/test.tsv.gz ../predictions/final_nn_hr_test.txt trip_start_hr final_nn_hr | tee final_nn_hr/perf.txt


###################### Plots for neural net
#30m
mkdir nn_30min_combined
python3 01_report_performance.py ../combined_data/30min/xval.tsv.gz ../predictions/nn_30min_combined_xval.txt trip_start_hr_30 nn_30min_combined | tee nn_30min_combined/perf.txt

mkdir nn_30min_winter
python3 01_report_performance.py ../winter_data/aggregates/30min/xval.tsv.gz ../predictions/nn_30min_winter_xval.txt trip_start_hr_30 nn_30min_winter | tee nn_30min_winter/perf.txt

mkdir nn_30min_summer
python3 01_report_performance.py ../summer_data/aggregates/30min/xval.tsv.gz ../predictions/nn_30min_summer_xval.txt trip_start_hr_30 nn_30min_summer | tee nn_30min_summer/perf.txt

#15m
mkdir nn_15min_combined
python3 01_report_performance.py ../combined_data/15min/xval.tsv.gz ../predictions/nn_15min_combined_xval.txt trip_start_hr_15 nn_15min_combined | tee nn_15min_combined/perf.txt

mkdir nn_15min_winter
python3 01_report_performance.py ../winter_data/aggregates/15min/xval.tsv.gz ../predictions/nn_15min_winter_xval.txt trip_start_hr_15 nn_15min_winter | tee nn_15min_winter/perf.txt

mkdir nn_15min_summer
python3 01_report_performance.py ../summer_data/aggregates/15min/xval.tsv.gz ../predictions/nn_15min_summer_xval.txt trip_start_hr_15 nn_15min_summer | tee nn_15min_summer/perf.txt



###################### Plots and analysis for xgboost
# 30m
mkdir xgb_30min_summer
python3 01_report_performance.py ../summer_data/aggregates/30min/xval.tsv.gz ../predictions/xgb_summer30preds.csv trip_start_hr_30 xgb_30min_summer | tee xgb_30min_summer/perf.txt

mkdir xgb_30min_winter
python3 01_report_performance.py ../winter_data/aggregates/30min/xval.tsv.gz ../predictions/xgb_winter30preds.csv trip_start_hr_30 xgb_30min_winter | tee xgb_30min_winter/perf.txt

mkdir xgb_30min_combined
python3 01_report_performance.py ../combined_data/30min/xval.tsv.gz          ../predictions/xgb_combined30preds.csv trip_start_hr_30 xgb_30min_combined | tee xgb_30min_combined/perf.txt

# 15m
mkdir xgb_15min_summer
python3 01_report_performance.py ../summer_data/aggregates/15min/xval.tsv.gz ../predictions/xgb_summer15preds.csv trip_start_hr_15 xgb_15min_summer | tee xgb_15min_summer/perf.txt

mkdir xgb_15min_winter
python3 01_report_performance.py ../winter_data/aggregates/15min/xval.tsv.gz ../predictions/xgb_15min_winterval.txt trip_start_hr_15 xgb_15min_winter | tee xgb_15min_winter/perf.txt
# file not found no 15min winter

mkdir xgb_15min_combined
python3 01_report_performance.py ../combined_data/15min/xval.tsv.gz          ../predictions/xgb_combined15preds.csv trip_start_hr_15 xgb_15min_combined | tee xgb_15min_combined/perf.txt



###################### Plots and analysis for SVM
# 30m
mkdir svm_30min_combined
python3 01_report_performance.py ../combined_data/30min/xval.tsv.gz ../predictions/3ClusterSVR_30min_combined.txt trip_start_hr_30 svm_30min_combined | tee svm_30min_combined/perf.txt

mkdir svm_30min_winter
python3 01_report_performance.py ../winter_data/aggregates/30min/xval.tsv.gz ../predictions/3ClusterSVR_30min_winter.txt trip_start_hr_30 svm_30min_winter | tee svm_30min_winter/perf.txt

mkdir svm_30min_summer
python3 01_report_performance.py ../summer_data/aggregates/30min/xval.tsv.gz ../predictions/3ClusterSVR_30min_summer.txt trip_start_hr_30 svm_30min_summer | tee svm_30min_summer/perf.txt

# 15m
mkdir svm_15min_combined
python3 01_report_performance.py ../combined_data/15min/xval.tsv.gz ../predictions/3ClusterSVR_15min_combined.txt trip_start_hr_15 svm_15min_combined | tee svm_15min_combined/perf.txt

mkdir svm_15min_winter
python3 01_report_performance.py ../winter_data/aggregates/15min/xval.tsv.gz ../predictions/3ClusterSVR_15min_winter.txt trip_start_hr_15 svm_15min_winter | tee svm_15min_winter/perf.txt

mkdir svm_15min_summer
python3 01_report_performance.py ../summer_data/aggregates/15min/xval.tsv.gz ../predictions/3ClusterSVR_15min_summer.txt trip_start_hr_15 svm_15min_summer | tee svm_15min_summer/perf.txt

