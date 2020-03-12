# uw-msds-trac-capstone


# Modeling King County Bus Ridership

## Background


## Installation
1. Clone the repo  
2. Create a new python environmet using the command:  
```conda env create UWTRAC```  
3. Activate UWTRAC by using the command:  
```conda activate UWTRAC```  
4. Install the required python packages using:  
```pip install –r requirements.txt```  

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
     |- Abstract
     |- Poster
     |- Paper
  |- eda
  |- evaluation
  |- examples/
     |- User_Guide.pdf
  |- models/
     |- final_nn/
	Model components per time aggregation modeled [15m, 30m, hr]
	|- [15m]_column_labels.pkl
	|- [15m]_one_hot_encoder.pkl
	|- [15m]_standard_scaler.pkl
	|- model_[15m].json
	|- model_[15m]_weights_train.h5
	|- model_15m_weights_train_and_xval.h5
 scaler, model, and weights per 
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
2. Weather Data ???

The code in this repository is licensed under a [MIT](https://opensource.org/licenses/MIT) license.

## How to Use/Examples
Use the [User Guide](???) to get you started.