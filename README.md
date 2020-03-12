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
     |- predictions/
     |- training_data/
     |- rte_clean.csv
  |- docs/
     |- Abstract
     |- Poster
     |- Paper
  |- eda
  |- evaluation
  |- examples/
     |- User_Guide.pdf
  |- models
  |- pipeline/
     |- validate_pipeline/
	|- analyze_stopfile.py
        |- sample_files.py
        |
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
1. [King Route Region Data](https://en.wikipedia.org/wiki/List_of_King_County_Metro_bus_routes) [CC BY-SA 3.0](https://creativecommons.org/licenses/by-sa/3.0/).
2. Weather Data ???

The code in this repository is licensed under a [MIT](https://opensource.org/licenses/MIT) license.

## How to Use/Examples
Use the [User Guide](???) to get you started.