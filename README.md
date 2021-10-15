# IBF-drought-model
Drought forecast pipeline for Zimbabwe RCS.

## Introduction

This repo contains the code to:
1. Get latest ENSO data 
2. Forecast drought per province based on the latest ENSO data
3. Calculate impacts of drought province

Data is on MS Azure Datalake `ibf`.

## Functionalities
The pipeline is developed in Docker format. It can run locally in your desktop (with Dock installed, see below) or linked with MS Logic for continuous execution (monthly).
At the moment, the pipeline is set to run in dummy mode since the drought-model object is not yet completed.

**`settings.py`** contains basic settings for the pipeline:
- To switch between operation and dummy mode using a boolean switch `dummy`
- To define lead-time based on the running mode and the month of execution

**`utils.py`** contains main functions for the pipeline. For now in dummy mode, only the last 2 functions will be executed.
- `get_new_senso()` (not ready): get latest ENSO data from data source. It also ensures the ENSO data 
- `forecast()` (template built): forecast drought per province based on the latest ENSO data, using trained XGBoost models
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


## Usage
```
Usage: run-drought-model
```
