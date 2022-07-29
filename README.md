# IBF-drought-model
Drought forecast pipeline for Zimbabwe RCS.

## Introduction

This repo contains the code to:
1. Get latest ENSO data 
2. Get latest CHIRPS data
3. Get latest VCI data
4. Forecast drought per province based on the latest data listed above.
5. Calculate impacts of drought province

Data is on MS Azure Datalake `ibf`.

## Functionalities
The pipeline is developed in Docker format. It can run locally in your desktop (with Dock installed, see below). A logic app is set up (510-ibf-drought) and to start the container and perform automated execution (monthly).

**`settings.py`** contains basic settings for the pipeline:
- To set test API for posting output and disable email notification
- To switch between operation and dummy mode using a boolean switch `dummy`
- To define lead-time based on the running mode and the month of execution
- To define a switch of the forecast model depending on the month of execution
- Data sources of ENSO, CHIRPS, VCI

**`utils.py`** contains main functions for the pipeline. For now in dummy mode, only the last 2 functions will be executed.
- `get_new_senso()`: get latest ENSO data from data source
- `get_new_chirps()`: get latest daily CHIRPS data from data source and calculate monthly accumulation and dryspell per district
- `get_new_vci()`: get latest observed VCI from data source and calculate monthly average VCI values per district
- `arrange_data()`: prepare an input data for model 2 by combining ENSO and CHIRPS (and VCI) data into one
- `forecast_model1()`: forecast drought per province based on the latest ENSO data, using trained XGBoost models
- `forecast_model2()`: forecast drought per province based on the latest ENSO, monthly rainfall and 14-day dry spell data, using trained XGBoost models
- `forecast_model3()`: forecast drought per province based on the latest ENSO, monthly rainfall, 14-day dry spell, and VCI data, using trained XGBoost models
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

## Versions
You can find the versions in the [tags](https://github.com/rodekruis/ibf-drought-model/tags) of the commits. See below table to find which version of the pipeline corresponds to which version of IBF-Portal.
| Drought Pipeline version  | IBF-Portal version | Changes |
| --- | --- | --- |
| 0.2.0 | 0.170.0 | Model 3 added to the pipeline <br> Function to download VCI data from NOAA added <br> Funtion to arrange data adjusted for VCI data <br> Enable downloading data in off-season|
| 0.1.3 | 0.152.0 | Add function to post non-trigger in off-season |
| 0.1.2 | 0.129.0 | Corrected generation of link to raw chirps file <br> Fixed misdownloading a processed rainfall from datalake <br> Fixed raw chirps files listing for calculating zonal statistics <br> Minor fixes |
| 0.1.1 | - | ENSO+rainfall model added <br> Minor fixes | 
| 0.1.0 | - | Initial version, ENSO-only model |