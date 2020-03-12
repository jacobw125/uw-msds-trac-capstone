###### uw-msds-trac-capstone
# Modeling King County Bus Ridership
![Performance](https://github.com/jacobw125/uw-msds-trac-capstone/blob/master/docs/Performance.PNG)

## Background
Bus route planning and optimization is an integral part of city planning. Understanding bus ridership enables transit agencies to make planning decisions and better understand the impact of station closures, headway changes, and weather events. However, no model or data sources exist to measure total ridership across the King County Metro system. ORCA payment card transactions are available, but only measure riders who pay with ORCA. Automatic person counter (APC) measures all riders but is only present for ~60% of trips during a survey period of 3 months, twice a year. The goal of this project is to predict total ridership (APC) across route, direction, and time of day using ORCA transactions and route metadata.

We use APC data from the King County Metro system to create a machine learning model that accurately estimates actual ridership using the incomplete ORCA transactional data, which can estimate total actual ridership across all routes, directions, and times of day, regardless of whether the bus was outfitted with automated person counting technology. 

To do this we:
1. Created a data processing pipeline that merges APC data, ORCA data, and route metadata to create training sets.
2. Trained a shallow, wide neural network utilizing the sigmoid function in the first layer with a linear output neuron.

For more information on the methodology and results, see [here](https://github.com/jacobw125/uw-msds-trac-capstone/blob/master/docs).

## Installation
1. Clone the repo  
2. Create a new python environmet using the command:  
```conda env create UWTRAC```  
3. Activate UWTRAC by using the command:  
```conda activate UWTRAC```  
4. Install the required python packages using:  
```pip install –r requirements.txt```  

## How to Use/Examples
Use the [User Guide](https://github.com/jacobw125/uw-msds-trac-capstone/blob/master/examples/User_Guide.pdf) to get started.

## Directory Structure
```
uw-msds-trac-capstone/
  |- data/
     |- predictions/
	**Predictions from final neural network for
	  time aggregates(15min, 30min, hr) on the 
	  combined (summer + winter) dataset.**
	|- model_final_nn/
	   |- final_nn_[15min]_test.txt
	   |- final_nn_[15min]_xval.txt
	**Predictions from models(nn, clustered_svm, xgb),
	  time aggregates (15min, 30min), and season
	  (combined, summer, winter).**
	|- model_[nn]/
	   |- [nn]_[15min]_[season]_test.txt
	   |- [nn]_[15min]_[season]_xval.txt
     |- training_data/
	|- combined_data/
	   |- aggregates/
	      **Training datasets for various time aggregation
	        levels(15min, 30min, ampm, day, hr).**
	      |- [15min]/
	         |- test.tsv.gz
                 |- train.tsv.gz
                 |- val.tsv.gz	
	|- summer_data
	   |- aggregates/...
	   |- boeing_field_2019.csv
	|- winter_data
	   |- aggregates/...
	   |- boeing_field_2019.csv
     |- rte_clean.csv
  |- docs/
     |- Abstract [remaining]
     |- Paper [remaining]
     |- Performance.PNG
     |- Poster [remaining]
  |- eda/
     |- reports_apc/
	|- correlates/
	   |- stop_id-VS-stop_name.tsv
	|- numerics.tsv
	|- unique_counts.tsv
	|- unique_vals.tsv
     |- reports_orca/
	|- correlates/
	   |- device_location_id-VS-device_location_descr.tsv
	   |- direction_id-VS-direction_descr.tsv
	   |- mode_id-VS-mode_descr.tsv
	   |- origin_location_id-VS-origin_location_descr.tsv
	   |- product_id-VS-product_descr.tsv
	   |- service_agency_id-VS-service_agency_name.tsv
	   |- source_agency_id-VS-source_agency_name.tsv
	   |- txn_passenger_type_id-VS-txn_passenger_type_descr.tsv
	   |- txn_type_id-VS-txn_type_descr.tsv
	   |- viaserviceareaid-VS-viaserviceareaname.tsv
	|- numerics.tsv
	|- unique_counts.tsv
	|- unique_vals.tsv
     |- src/
	|- agg_apc.py
	|- agg_orca.py
	|- figure_out_direction.py
	|- filter_apc.py
	|- filter_orca.py
	|- generic_inspec.py
	|- inspect_apc.py
	|- inspect_orca.py
	|- merge_apc_orca.py
     |- winter_summer_EDA.ipynb
  |- evaluation/
     |- cluster_rte_frequency/
	|- 00_gather_route_info.ipynb
	|- rte_clusters.tsv
     |- model_bias/
	|- plots/
	   **Plots created by python notebook.**
	|- bias.ipynb
     |- model_feature_explain/
	|- contributions_by_feature.svg
	|- describe_nn.ipynb
     |- model_preformance/
	|- model_final_nn/
	   |- final_nn_15min/
	      **Plots created by python notebook.**
	   |- final_nn_15min_no_rr/...
	   |- final_nn_30min/...
           |- final_nn_30min_no_rr/...
	   |- final_nn_hr/...
	   |- final_nn_hr_no_rr/...
	Various models [nn, svm, xgb]
	|- model_[nn]/
	   |- [nn]_15min_combined/
	      **Plots created by python notebook.**
	   |- [nn]_15min_summer/...
	   |- [nn]_15min_winter/...
           |- [nn]_30min_combined/...
	   |- [nn]_30min_summer/...
	   |- [nn]_30min_winter/...
	|- plots/
	   **Plots created by python notebook.**
	|- 01_reports_performance.py
	|- 15m_test_group_perg.csv
	|- batch_compare.tsv
	|- compare_perf.ipynb
	|- evaluate.sh
	|- genBarPlots.py
     |- ridership_by_day/
	|- plots/ 
	   **Plots created by python notebook.**
	|- ridership_by_day_evaluation.ipynb
  |- examples/
     |- User_Guide.pdf [remaining]
  |- models/
     |- final_nn/
	**Model components per time aggregation modeled [15m, 30m, hr].**
	|- [15m]_column_labels.pkl
	|- [15m]_one_hot_encoder.pkl
	|- [15m]_standard_scaler.pkl
	|- model_[15m].json
	|- model_[15m]_weights_train.h5
	|- model_15m_weights_train_and_xval.h5
     |- model_iterations/
	|- clustered_linear.ipynb
        |- clustered_nn.ipynb
	|- linear.ipynb
	|- XGB [remaining]
     |- final_nn.ipynb
  |- pipeline/
     |- validate_pipeline/
	|- analyze_stopfile.py [remaining]
        |- sample_files.py
        |- pipeline.ipynb [remaining]
     |- 01_filter_apc.py
     |- 02_filter_apc.py
     |- 03_agg_orca.py
     |- 04_merge.py
     |- 05_create_training.py
     |- constants.py
     |- functions.py
  |- LICENSE
  |- README.md
  |- requirements.txt
```

## Licensing
1. [King Route Region Data](https://en.wikipedia.org/wiki/List_of_King_County_Metro_bus_routes) - [CC BY-SA 3.0](https://creativecommons.org/licenses/by-sa/3.0/).
2. [Weather Data](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd) - [Open Data Policy](https://project-open-data.cio.gov/)

The code in this repository is licensed under a [MIT](https://opensource.org/licenses/MIT) license.

## Acknowledgements
- Mark Hallenbeck at TRAC for his time and subject expertise.
- Dmitri Zyuzin for his help gathering the data for this project.
- Megan Hazen for her guidance in this capstone project and expertise on neural networks.

This analysis was done for the University of Washington's Master of Data Science's Capstone.
More information about the class can be found [here](https://www.washington.edu/datasciencemasters/capstone-projects/).
