# IBF-drought-model
Drought forecast pipeline for Zimbabwe RCS.

## Introduction

This repo contains the code to:
1. Get latest ENSO data 
2. Get latest CHIRPS data
2. Forecast drought per province based on the latest ENSO data or a combination of the ENSO and CHIRPS data
3. Calculate impacts of drought province

Data is on MS Azure Datalake `ibf`.

## Functionalities
The pipeline is developed in Docker format. It can run locally in your desktop (with Dock installed, see below). A logic app is set up (510-ibf-drought) and to start the container and perform automated execution (monthly).

**`settings.py`** contains basic settings for the pipeline:
- To switch between operation and dummy mode using a boolean switch `dummy`
- To define lead-time based on the running mode and the month of execution
- To define a switch of the forecast model depending on the month of execution
- Data sources of ENSO and CHIRPS

**`utils.py`** contains main functions for the pipeline. For now in dummy mode, only the last 2 functions will be executed.
- `get_new_senso()`: get latest ENSO data from data source
- `get_new_chirps()`: get latest daily CHIRPS data from data source and calculate monthly accumulation per district
- `arrange_data()`: prepare an input data for model 2 by combining ENSO and CHIRPS data into one
- `forecast_model1()`: forecast drought per province based on the latest ENSO data, using trained XGBoost models
- `forecast_model2()`: forecast drought per province based on the latest ENSO data, using trained XGBoost models
- `calculate_impact()`: calculate exposed population, cattles, ruminants per drought-predicted province(s)
- `post_output()`: the processed data (drought forecast and impacts) will be posted to the IBF dashboard via IBF API 

## Setup

### with Docker
1. Install [Docker](https://www.docker.com/get-started)

2. Build the docker image from the root directory
```
docker build -t rodekruis/ibf-drought-model .
```
3. Run and access the docker container
```
docker run -it --entrypoint /bin/bash rodekruis/ibf-drought-model
```
4. Set necessary credentials (in bitwarden) for the pipeline


## Usage
Run the pipeline with the command:
```
run-drought-model
```
