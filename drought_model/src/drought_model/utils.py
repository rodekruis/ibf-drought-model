import os
import json
import errno
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from dotenv import load_dotenv
import requests
from azure.storage.blob import BlobServiceClient, BlobClient
from drought_model.settings import *



# def get_new_enso():
#   '''
#   Function to download latest ENSO data.

#   '''

#   return()


def forecast():
  '''
  Function to load trained model and run the forecast with new input data per province.
  An output csv contained PCODE and so-called alert_threshold will be saved in the datalake.
  
  '''

  # call admin boundary blobstorage
  with open("credentials/admboundary_blobstorage_secrets.json") as file:
    admboundary_blobstorage_secrets = json.load(file)
  blob_service_client = BlobServiceClient.from_connection_string(admboundary_blobstorage_secrets['connection_string'])

  # load country shapefile
  blob_client = blob_service_client.get_blob_client(container='admin-boundaries',
                                                    blob='Silver/zwe/zwe_admbnda_adm1_zimstat_ocha_20180911.csv')
  locations_path = "./data_in"
  os.makedirs(locations_path, exist_ok=True)
  location_file_path = os.path.join(locations_path, 'zwe_admbnda_adm1_zimstat_ocha_20180911.csv')
  with open(location_file_path, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())
  zwe_adm1 = pd.read_csv(location_file_path)

  regions = np.unique(zwe_adm1['ADM1_PCODE'])

  # call ibf blobstorage
  with open("credentials/ibf_blobstorage_secrets.json") as file:
    ibf_blobstorage_secrets = json.load(file)
  blob_service_client = BlobServiceClient.from_connection_string(ibf_blobstorage_secrets['connection_string'])

  # load enso data
  blob_client = blob_service_client.get_blob_client(container='ibf',
                                                    blob='drought/Bronze/enso/enso_table.csv')
  locations_path = "./data_in"
  os.makedirs(locations_path, exist_ok=True)
  location_file_path = os.path.join(locations_path, 'enso_table.csv')
  with open(location_file_path, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())
  enso_table = pd.read_csv(location_file_path, sep=' ')

  
  # extract enso of the month
  if month == 8:
    df_enso = enso_table.drop(columns=['ASO', 'SON', 'OND', 'NDJ',
                                        'DJF', 'JFM', 'FMA', 'MAM', 'date'])
    df_enso = df_enso[df_enso['Year']==year].drop(columns='Year')

  elif month == 9:
    df_enso = enso_table.drop(columns=['SON', 'OND', 'NDJ',
                                        'DJF', 'JFM', 'FMA', 'MAM', 'date'])
    df_enso = df_enso[df_enso['Year']==year].drop(columns='Year')

  elif month == 10:
    df_enso = enso_table.drop(columns=['OND', 'NDJ',
                                        'DJF', 'JFM', 'FMA', 'MAM', 'date'])
    df_enso = df_enso[df_enso['Year']==year].drop(columns='Year')
  
  elif month == 11:
    df_enso = enso_table.drop(columns=['NDJ',
                                        'DJF', 'JFM', 'FMA', 'MAM', 'date'])
    df_enso = df_enso[df_enso['Year']==year].drop(columns='Year')

  elif month == 12:
    df_enso = enso_table.drop(columns=['DJF', 'JFM', 'FMA', 'MAM', 'date'])
    df_enso = df_enso[df_enso['Year']==year].drop(columns='Year')

  elif month == 1:
    df_enso1 = enso_table[enso_table['Year']==year-1].\
      drop(columns=['DJF', 'JFM', 'FMA', 'MAM', 'date']).reset_index()
    df_enso2 = enso_table[enso_table['Year']==year][['DJF']].reset_index()
    df_enso = pd.concat(df_enso1, df_enso2, axis=1)

  elif month == 2:
    df_enso1 = enso_table[enso_table['Year']==year-1].\
      drop(columns=['DJF', 'JFM', 'FMA', 'MAM', 'date']).reset_index()
    df_enso2 = enso_table[enso_table['Year']==year][['DJF','JFM']].reset_index()
    df_enso = pd.concat(df_enso1, df_enso2, axis=1)

  elif month == 3:
    df_enso1 = enso_table[enso_table['Year']==year-1].\
      drop(columns=['DJF', 'JFM', 'FMA', 'MAM', 'date']).reset_index()
    df_enso2 = enso_table[enso_table['Year']==year][['DJF','JFM','FMA']].reset_index()
    df_enso = pd.concat(df_enso1, df_enso2, axis=1)


  # forecast based on crop-yield
  df_pred_provinces = {}

  for region in regions:
    df_pred = pd.DataFrame()
    
    # load model
    blob_client = blob_service_client.get_blob_client(container='ibf',
                                                      blob='drought/Silver/zwe/model_precipitation.json') # this is not the real model!!!
    locations_path = "./model"
    os.makedirs(locations_path, exist_ok=True)
    location_file_path = os.path.join(locations_path, 'model_precipitation.json')
    with open(location_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    model = XGBClassifier()
    model.load_model(location_file_path)

    # forecast
    pred = model.predict(df_enso)
    df_pred['alert_threshold'] = pred
    df_pred['region'] = region
    df_pred['leadtime'] = leadtime
    df_pred_provinces = df_pred_provinces.append(pd.DataFrame(data=df_pred, index=[0]))


  # save processed tweets locally
  locations_path = './data_out'
  predict_file_path = os.path.join(locations_path, 'zwe_m1_crop_predict.csv')
  df_pred_provinces.to_csv(predict_file_path, index=False)

  # upload processed tweets
  blob_client = blob_service_client.get_blob_client(container='ibf',
                                                    blob='drought/Gold/zwe/zwe_m1_crop_predict.csv')
  with open(predict_file_path, "rb") as data:
      blob_client.upload_blob(data, overwrite=True)


  # forecast based on impact database
  




def calculate_impact():
  '''
  Function to calculate impacts of drought per provinces.
  Drought areas are defined by the above function forecast() which is saved in the datalake.
  Impacts include affected population and affected ruminants.
  If a drought is forecasted in a province, entire population and ruminants of the province are considered to be impacted.
  
  '''


  # call ibf blobstorage
  with open("credentials/ibf_blobstorage_secrets.json") as file:
    ibf_blobstorage_secrets = json.load(file)
  blob_service_client = BlobServiceClient.from_connection_string(ibf_blobstorage_secrets['connection_string'])

  # download to-be-uploaded data: alert_threshold
  if dummy:
    blob_client = blob_service_client.get_blob_client(container='ibf',
                                                      blob='drought/Gold/zwe/zwe_m1_crop_predict_dummy.csv')
    locations_path = "./data_out"
    os.makedirs(locations_path, exist_ok=True)
    location_file_path = os.path.join(locations_path, 'zwe_m1_crop_predict_dummy.csv')
    with open(location_file_path, "wb") as download_file:
      download_file.write(blob_client.download_blob().readall())
    df_pred_provinces = pd.read_csv(location_file_path)
  else:
    blob_client = blob_service_client.get_blob_client(container='ibf',
                                                      blob='drought/Gold/zwe/zwe_m1_crop_predict.csv')
    locations_path = "./data_out"
    os.makedirs(locations_path, exist_ok=True)
    location_file_path = os.path.join(locations_path, 'zwe_m1_crop_predict.csv')
    with open(location_file_path, "wb") as download_file:
      download_file.write(blob_client.download_blob().readall())
    df_pred_provinces = pd.read_csv(location_file_path)
  df_pred_provinces = df_pred_provinces.rename(columns={'drought': 'alert_threshold'})

  # load to-be-uploaded data: affected population
  blob_client = blob_service_client.get_blob_client(container='ibf',
                                                    blob='drought/Gold/zwe/zwe_population_adm1.csv')
  locations_path = "./data_out"
  os.makedirs(locations_path, exist_ok=True)
  location_file_path = os.path.join(locations_path, 'zwe_population_adm1')
  with open(location_file_path, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())
  df_pop_provinces = pd.read_csv(location_file_path)
  df_pred_provinces = df_pred_provinces.merge(df_pop_provinces, left_on='region', right_on='ADM1_PCODE')
  df_pred_provinces['population_affected'] = df_pred_provinces['alert_threshold'] * df_pred_provinces['total_pop']


  # load to-be-uploaded data: exposed ruminents
  blob_client = blob_service_client.get_blob_client(container='ibf',
                                                      blob='drought/Gold/zwe/zwe_ruminants_adm1.csv')
  locations_path = "./data_out"
  os.makedirs(locations_path, exist_ok=True)
  location_file_path = os.path.join(locations_path, 'zwe_ruminants_adm1.csv')
  with open(location_file_path, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())
  df_pop_provinces = pd.read_csv(location_file_path)
  df_pred_provinces = df_pred_provinces.merge(df_pop_provinces, left_on='region', right_on='pcode')
  df_pred_provinces['small_ruminants_exposed'] = df_pred_provinces['alert_threshold'] * df_pred_provinces['small_reminant_lsu']


  # load to-be-uploaded data: exposed cattle
  blob_client = blob_service_client.get_blob_client(container='ibf',
                                                      blob='drought/Gold/zwe/zwe_cattle_adm1.csv')
  locations_path = "./data_out"
  os.makedirs(locations_path, exist_ok=True)
  location_file_path = os.path.join(locations_path, 'zwe_cattle_adm1.csv')
  with open(location_file_path, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())
  df_pop_provinces = pd.read_csv(location_file_path)
  df_pred_provinces = df_pred_provinces.merge(df_pop_provinces, left_on='region', right_on='pcode')
  df_pred_provinces['cattle_exposed'] = df_pred_provinces['alert_threshold'] * df_pred_provinces['cattle_lsu']

  return(df_pred_provinces)


def post_output(df_pred_provinces):
  '''
  Function to post layers into IBF System.
  For every layer, the function calls IBF API and post the layer in the format of json.
  The layers are alert_threshold (drought or not drought per provinces), population_affected and ruminants_affected.
  
  '''

  # load credentials to IBF API
  ibf_credentials = os.path.join('credentials', 'ibf-credentials.env')
  if not os.path.exists(ibf_credentials):
    print(f'ERROR: IBF credentials not found in {ibf_credentials}')
    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), ibf_credentials)
  load_dotenv(dotenv_path=ibf_credentials)
  IBF_API_URL = os.environ.get("IBF_API_URL")
  ADMIN_LOGIN = os.environ.get("ADMIN_LOGIN")
  ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD")


  # log in to IBF API
  login_response = requests.post(f'{IBF_API_URL}/api/user/login',
                                data=[('email', ADMIN_LOGIN), ('password', ADMIN_PASSWORD)])
  token = login_response.json()['user']['token']


  # loop over layers to upload
  for layer in ['alert_threshold', 'population_affected', 'small_ruminants_exposed', 'cattle_exposed']:
    
    # prepare layer
    exposure_data = {'countryCodeISO3': 'ZWE'}
    exposure_place_codes = []
    for ix, row in df_pred_provinces.iterrows():
      exposure_entry = {'placeCode': row['region'],
                        'amount': row[layer]}
      exposure_place_codes.append(exposure_entry)
    exposure_data['exposurePlaceCodes'] = exposure_place_codes
    exposure_data["adminLevel"] = 1
    exposure_data["leadTime"] = leadtime_str
    exposure_data["dynamicIndicator"] = layer
    exposure_data["disasterType"] = 'drought'
    
    # upload layer
    r = requests.post(f'{IBF_API_URL}/api/admin-area-dynamic-data/exposure',
                      json=exposure_data,
                      headers={'Authorization': 'Bearer '+ token,
                              'Content-Type': 'application/json',
                              'Accept': 'application/json'})
    if r.status_code >= 400:
      print(r.text)
      raise ValueError()

  # send email
  if 1 in df_pred_provinces['alert_threshold'].values:
    print(df_pred_provinces['alert_threshold'].values)
    # logging.info(f"SENDING ALERT EMAIL")
    email_response = requests.post(f'{IBF_API_URL}/api/notification/send',
                                    json={'countryCodeISO3': 'ZWE',
                                          'disasterType': 'drought'},
                                    headers={'Authorization': 'Bearer ' + token,
                                             'Content-Type': 'application/json',
                                             'Accept': 'application/json'})
    if email_response.status_code >= 400:
      # logging.error(f"PIPELINE ERROR AT EMAIL {email_response.status_code}: {email_response.text}")
      # print(r.text)
      raise ValueError()
      exit(0)