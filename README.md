# Modeling King County Bus Ridership
##### uw-msds-trac-capstone
![Performance](https://github.com/jacobw125/uw-msds-trac-capstone/blob/master/docs/Performance.PNG)

## Background



## Installation
1. Clone the repo  
2. Create a new python environmet using the command:  
```conda env create UWTRAC```  
3. Activate UWTRAC by using the command:  
```conda activate UWTRAC```  
4. Install the required python packages using:  
```pip install –r requirements.txt```  

## How to Use/Examples
Use the [User Guide](https://github.com/jacobw125/uw-msds-trac-capstone/blob/master/examples/User_Guide.pdf) to get you started.

## Directory Structure
```
uw-msds-trac-capstone/
  |- data/
     |- predictions/ [NEEDS MORE]
     |- training_data/
	|- combined_data/
	   |- aggregates/
	      |- 15min/
	         |- test.tsv.gz
                 |- train.tsv.gz
                 |- val.tsv.gz
		[Remaining]	
	      |- 30min/...
              |- ampm/...
	      |- day/...
              |- hr/...
	|- summer_data
	   |- aggregates/...
	   |- boeing_field_2019.csv
	|- winter_data
	   |- aggregates/...
	   |- boeing_field_2019.csv
     |- rte_clean.csv
  |- docs/
     |- Abstract [remaining]
     |- Poster [remaining]
     |- Paper [remaining]
  |- eda/
     |- reports_apc/
	|- correlates/
	   |- stop_id-VS-stop_name.tsv
	|- numerics.tsv
	|- unique_counts.tsv
	|- unique_vals.tsv
     |- reports_orca/
	|- correlates/
	   |- [Remaining]
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
	   Plots created by python notebook.
	|- bias.ipynb
     |- model_feature_explain/
	|- contributions_by_feature.svg
	|- describe_nn.ipynb
     |- model_preformance/ [remaining]
     |- ridership_by_day/
	|- plots/ 
	   Plots created by python notebook.
	|- ridership_by_day_evaluation.ipynb
  |- examples/
     |- User_Guide.pdf [remaining]
  |- models/
     |- final_nn/
	Model components per time aggregation modeled [15m, 30m, hr].
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
This analysis was done for the University of Washington's Master of Data Science's Capstone.
More information about the class can be found [here](https://www.washington.edu/datasciencemasters/capstone-projects/).

- Mark Hallenbeck at TRAC for his time and subject expertise.
- Dmitri Zyuzin for his help gathering the data for this project.
- Megan Hazen for her guidance in this capstone project and expertise on neural networks.
