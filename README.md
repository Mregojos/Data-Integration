# Data Integration

## Architecture

## Objective
* Process and clean the data
* Integrate and load data in csv and in parquet format
* Orchestrate the data pipeline
* Query the data

## Data
* Open Canada 
  * 3-Year Employment Outlooks -2022-2024 Employment Outlooks NOC 2021
  * 3-Year Employment Outlooks 2022-2024 Employment Outlooks NOC 2016
  * List of post-secondary academic institutions and programs validated by the Public Service Commission-2023-04-03
  
## Tech Stack
* Python, Pandas, Prefect Cloud

## Tasks
```sh
# Prefect Cloud and login
prefect cloud login

# Clone the repo
git clone https://github.com/Mregojos/Data-Integration

cd Data-Integration
pip install -U -r requirements.txt
python ./data-processing
```

![Prefect Cloud](https://github.com/Mregojos/Data-Integration/blob/main/images/Prefect%20Cloud%20v2.png)

