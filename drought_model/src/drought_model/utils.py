import os
import io
import json
import errno
import pandas as pd
import numpy as np
# import geopandas as gpd
import subprocess
from rasterstats import zonal_stats
from xgboost import XGBClassifier
from bs4 import BeautifulSoup
import requests
import urllib.error
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient, BlobClient
from drought_model.settings import *
import datetime
import time
import calendar
import logging



def get_secretVal(secret_name):

    kv_url = f'https://ibf-keys.vault.azure.net'
    kv_accountname = 'ibf-keys'

    # Authenticate with Azure
    az_credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)

    # Retrieve primary key for blob from the Azure Keyvault
    kv_secretClient = SecretClient(vault_url=kv_url, credential=az_credential)
    secret_value = kv_secretClient.get_secret(secret_name).value

    return secret_value



def basic_data():
    '''
    Function to prepare folders in container and retrieve basic data from datalake to there.
    Data are adm (shp, csv). Folders are for 

    '''

    logging.info('basic_data: creating folders in container')

    # create folders 
    data_in_path = "./data_in"
    os.makedirs(data_in_path, exist_ok=True)
    adm_path = "./shp"
    os.makedirs(adm_path, exist_ok=True)
    rawchirps_path = "./data_in/chirps_tif"
    os.makedirs(rawchirps_path, exist_ok=True)
    model_path = "./model"
    os.makedirs(model_path, exist_ok=True)
    data_out_path = './data_out'
    os.makedirs(data_out_path, exist_ok=True)

    logging.info('basic_data: retrieving basic data from datalake to folders in container')

    # call admin boundary blobstorage
    adm_blobstorage_secrets = get_secretVal('admboundary-blobstorage-secrets')
    adm_blobstorage_secrets = json.loads(adm_blobstorage_secrets)
    blob_service_client = BlobServiceClient.from_connection_string(adm_blobstorage_secrets['connection_string'])

    # load adm1 country shapefile
    admboundary1_blob_client = blob_service_client.get_blob_client(container='admin-boundaries',
                                                                  blob='Bronze/zwe/zwe_admbnda_adm1_zimstat_ocha_20180911/zwe_admbnda_adm1_zimstat_ocha_20180911.geojson')
    adm1_shp_path = os.path.join(adm_path, 'zwe_admbnda_adm1_zimstat_ocha_20180911.geojson')
    with open(adm1_shp_path, "wb") as download_file:
        download_file.write(admboundary1_blob_client.download_blob().readall())
    
    # load country csv file
    adm1_blob_client = blob_service_client.get_blob_client(container='admin-boundaries',
                                                          blob='Silver/zwe/zwe_admbnda_adm1_zimstat_ocha_20180911.csv')
    adm1_csv_path = os.path.join(adm_path, 'zwe_admbnda_adm1_zimstat_ocha_20180911.csv')
    with open(adm1_csv_path, "wb") as download_file:
        download_file.write(adm1_blob_client.download_blob().readall())

    # load adm2 country shapefile
    admboundary2_blob_client = blob_service_client.get_blob_client(container='admin-boundaries',
                                                                  blob='Bronze/zwe/zwe_admbnda_adm2_zimstat_ocha_20180911/zwe_admbnda_adm2_zimstat_ocha_20180911.geojson')
    adm2_shp_path = os.path.join(adm_path, 'zwe_admbnda_adm2_zimstat_ocha_20180911.geojson')
    with open(adm2_shp_path, "wb") as download_file:
        download_file.write(admboundary2_blob_client.download_blob().readall())
    
    # load adm2 country csv file
    adm2_blob_client = blob_service_client.get_blob_client(container='admin-boundaries',
                                                          blob='Silver/zwe/zwe_admbnda_adm2_zimstat_ocha_20180911.csv')
    adm2_csv_path = os.path.join(adm_path, 'zwe_admbnda_adm2_zimstat_ocha_20180911.csv')
    with open(adm2_csv_path, "wb") as download_file:
        download_file.write(adm2_blob_client.download_blob().readall())

    logging.info('basic_data: done')




def access_enso(url):
    '''
    Function to access and get ENSO data.
    Retry to access to the datasource in 10 min if failed.
    Stop retrying if after 1 hour.

    '''

    logging.info('access_enso: accessing ENSO data source')

    accessDone = False
    timeToTryAccess = 6000
    timeToRetry = 600

    start = time.time()
    end = start + timeToTryAccess

    while (accessDone == False) and (time.time() < end):
        try:
            page = requests.get(url).text
            accessDone = True
        except urllib.error.URLError:
            logging.info("ENSO data source access failed. "
                          "Trying again in 10 minutes")
            time.sleep(timeToRetry)
    if accessDone == False:
        logging.error('ERROR: ENSO data source access failed for ' +
                      str(timeToTryAccess / 3600) + ' hours')
        raise ValueError()
    
    return(page)



def get_new_enso():
    '''
    Function to download and extract latest ENSO data.
    Defending on the month of execution (lead time), the function will extract data of corresponding month(s).

    '''

    # today = datetime.date.today()

    # folder
    data_in_path = "./data_in"

    enso_filename = 'enso_' + today.strftime("%Y-%m") + '.csv'
    enso_file_path = os.path.join(data_in_path, enso_filename)
    
    # call ibf blobstorage
    ibf_blobstorage_secrets = get_secretVal('ibf-blobstorage-secrets')
    ibf_blobstorage_secrets = json.loads(ibf_blobstorage_secrets)
    blob_service_client = BlobServiceClient.from_connection_string(ibf_blobstorage_secrets['connection_string'])
    blob_client = blob_service_client.get_blob_client(container='ibf',
                                                      blob='drought/Silver/zwe/enso/'+ enso_filename)

    # read new enso data
    logging.info('get_new_enso: downloading new ENSO dataset')
    page = access_enso(enso_url)
    df = pd.read_csv(io.StringIO(page), delim_whitespace=True)
    df['YR'] = df['YR'].shift(-1).fillna(year)

    df1 = df.copy()
    df1.index=[0]*len(df1)
    df1 = df1.pivot(columns='SEAS', values='ANOM', index='YR')

    columns = ['FMA', 'MAM', 'AMJ', 'MJJ', 'JJA', 'JAS', 'ASO', 'SON', 'OND',
                'NDJ', 'DJF', 'JFM']
    df1 = df1.reindex(columns, axis=1).reset_index()

    df1[['NDJ', 'DJF', 'JFM']] = df1[['NDJ', 'DJF', 'JFM']].shift(-1)

    # pick and arrange enso data
    logging.info('get_new_enso: extracting ENSO of corressponding month(s)')
    if month == 9: # lead time 7 month
        if (df.tail(1)['SEAS'] == 'JJA').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(['YR', 'index',
                                    'JAS', 'ASO', 'SON', 'OND', 'NDJ', 'DJF', 'JFM'], axis=1)
            df_enso.to_csv(enso_file_path)
            with open(enso_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 10: # lead time 6 month
        if (df.tail(1)['SEAS'] == 'JAS').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'ASO', 'SON', 'OND', 'NDJ', 'DJF', 'JFM'], axis=1)
            # df_enso = df_enso[df_enso['Year']==year].drop(columns='Year')
            df_enso.to_csv(enso_file_path)
            with open(enso_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
        else:
            logging.error('ENSO data not updated')
            raise ValueError()
    
    elif month == 11: # lead time 5 month
        if (df.tail(1)['SEAS'] == 'ASO').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'SON', 'OND', 'NDJ', 'DJF', 'JFM'], axis=1)
            # df_enso = df_enso[df_enso['Year']==year].drop(columns='Year')
            df_enso.to_csv(enso_file_path)
            with open(enso_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 12: # lead time 4 month
        if (df.tail(1)['SEAS'] == 'SON').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'OND', 'NDJ', 'DJF', 'JFM'], axis=1)
            # df_enso = df_enso[df_enso['Year']==year].drop(columns='Year')
            df_enso.to_csv(enso_file_path)
            with open(enso_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 1: # lead time 3 month
        if (df.tail(1)['SEAS'] == 'OND').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'NDJ', 'DJF', 'JFM'], axis=1)
            df_enso.to_csv(enso_file_path)
            with open(enso_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 2: # lead time 2 month
        if (df.tail(1)['SEAS'] == 'NDJ').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'DJF', 'JFM'], axis=1)
            df_enso.to_csv(enso_file_path)
            with open(enso_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 3: # lead time 1 month
        if (df.tail(1)['SEAS'] == 'DJF').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'JFM'], axis=1)
            df_enso.to_csv(enso_file_path)
            with open(enso_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 4: # lead time
        if (df.tail(1)['SEAS'] == 'JFM').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index'], axis=1)
            df_enso.to_csv(enso_file_path)
            with open(enso_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
        else:
            logging.error('ENSO data not updated')
            raise ValueError()
    
    logging.info('get_new_enso: done')
    # return df_enso



def access_chirps(url):
    
    logging.info('access_chirps: accessing chirps data source')

    accessDone = False
    timeToTryAccess = 6000
    timeToRetry = 600

    start = time.time()
    end = start + timeToTryAccess

    while (accessDone == False) and (time.time() < end):
        try:
            page = requests.get(url).text
            accessDone = True
        except urllib.error.URLError:
            logging.info("ENSO data source access failed. "
                          "Trying again in 10 minutes")
            time.sleep(timeToRetry)
    if accessDone == False:
        logging.error('ERROR: ENSO data source access failed for ' +
                      str(timeToTryAccess / 3600) + ' hours')
        raise ValueError()
    
    soup = BeautifulSoup(page, 'html.parser')

    return [url + node.get('href') for node in soup.find_all('a') if node.get('href')]



def get_new_chirps():
    '''
    Function to download raw daily CHIPRS data
    and return monthly cumulative per adm2
    '''

    # today = datetime.date.today()
    
    # folders 
    data_in_path = "./data_in"
    adm_path = "./shp"
    rawchirps_path = "./data_in/chirps_tif"

    # load country file path
    adm_shp_path = os.path.join(adm_path, 'zwe_admbnda_adm2_zimstat_ocha_20180911.geojson')
    adm_csv_path = os.path.join(adm_path, 'zwe_admbnda_adm2_zimstat_ocha_20180911.csv')
    
    df_chirps = pd.read_csv(adm_csv_path)[['ADM2_PCODE']]

    # call ibf blobstorage
    ibf_blobstorage_secrets = get_secretVal('ibf-blobstorage-secrets')
    ibf_blobstorage_secrets = json.loads(ibf_blobstorage_secrets)
    blob_service_client = BlobServiceClient.from_connection_string(ibf_blobstorage_secrets['connection_string'])
    
    # access CHIRPS data source
    logging.info('get_new_chirps: downloading new CHIRPS dataset')
    
    if month == 1:
        year_data = year - 1 
        month_data = month - 1
    else:
        year_data = year
        month_data = month - 1
        
    chirps_url1 = chirps_url + str(year_data) + '/'
    urls = access_chirps(chirps_url1)#[1:]
    file_urls = sorted([i for i in urls if i.split('/')[-1].startswith('chirps-v2.0.{0}.{1:02d}'.format(year_data, month_data))], reverse=True)
    if not file_urls:
        logging.error('CHIRPS data not updated')
    
    filename_list = [i.split('/')[-1] for i in file_urls]
    
    # download new CHIRPS data
    for i in np.arange(0,len(filename_list)):
        batch_ex_download = "wget -nd -e robots=off -A %s %s" %(
                filename_list[i], file_urls[i]) 
        subprocess.call(batch_ex_download, cwd=rawchirps_path, shell=True)
        rawdata_file_path = os.path.join(rawchirps_path, filename_list[i])
        with open(rawdata_file_path, "rb") as data:
            blob_client = blob_service_client.get_blob_client(container='ibf',
                                                            blob='drought/Bronze/chirps/new_download/' + filename_list[i])
            blob_client.upload_blob(data, overwrite=True)

    days = np.arange(1, calendar.monthrange(year_data, month_data)[1]+1, 1)

    # calculate monthly cumulative
    logging.info('get_new_chirps: calculating monthly cumulative rainfall')

    i = 0
    for filename in filename_list:
        raster_path = os.path.abspath(os.path.join(rawchirps_path, filename))
        mean = zonal_stats(adm_shp_path, raster_path, stats='mean')
        df_chirps['{0:02d}'.format(days[i])] = pd.DataFrame(data=mean)['mean']
        i += 1

    df_chirps['{0}'.format(month_data)] = df_chirps.loc[:,"01":"{0}".format(days[-1])].sum(axis=1)
    df_chirps = df_chirps[['ADM2_PCODE', '{0}'.format(month_data)]]
    
    processeddata_filename = 'chirps_' + today.strftime("%Y-%m") + '.csv'
    processeddata_file_path = os.path.join(data_in_path, processeddata_filename)
    df_chirps.to_csv(processeddata_file_path)
    with open(processeddata_file_path, "rb") as data:
        blob_client = blob_service_client.get_blob_client(container='ibf',
                                                        blob='drought/Silver/zwe/chirps/' + processeddata_filename)
        blob_client.upload_blob(data, overwrite=True)

    logging.info('get_new_chirps: done')
    # return df_chirps



def arrange_data():
    '''
    Function to arrange ENSO anf CHIRPS data. This is for forecast_model2() only.
    '''
    
    # today = datetime.date.today()

    # folder of processed CHIRPS csv
    data_in_path = "./data_in"
    adm_path = "./shp"

    # load country file path
    adm_csv_path = os.path.join(adm_path, 'zwe_admbnda_adm2_zimstat_ocha_20180911.csv')
    df_adm = pd.read_csv(adm_csv_path)[['ADM1_PCODE', 'ADM2_PCODE']]

    # load enso data
    enso_filename = 'enso_' + today.strftime("%Y-%m") + '.csv'
    enso_file_path = os.path.join(data_in_path, enso_filename)
    df_enso = pd.read_csv(enso_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')
    df_data = df_adm.merge(df_enso, how='cross')

    # call ibf blobstorage
    ibf_blobstorage_secrets = get_secretVal('ibf-blobstorage-secrets')
    ibf_blobstorage_secrets = json.loads(ibf_blobstorage_secrets)
    blob_service_client = BlobServiceClient.from_connection_string(ibf_blobstorage_secrets['connection_string'])

    # load last month(s) chirps data (if available)
    if month == 11:
        month_data = 9
        this_year = today.year
        chirps_filename = 'chirps_' + '{0}-{1:02}'.format(this_year, month_data) + '.csv'
        blob_client = blob_service_client.get_blob_client(container='ibf',
                                                        blob='drought/Silver/zwe/chirps/'+ chirps_filename)
        chirps_file_path = os.path.join(data_in_path, chirps_filename)
        with open(chirps_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        df_chirps = pd.read_csv(chirps_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')
        df_data = df_data.merge(df_chirps, on='ADM2_PCODE')

    elif month == 12:
        for month_data in [9, 10]:
            this_year = today.year
            chirps_filename = 'chirps_' + '{0}-{1:02}'.format(this_year, month_data) + '.csv'
            blob_client = blob_service_client.get_blob_client(container='ibf',
                                                            blob='drought/Silver/zwe/chirps/'+ chirps_filename)
            chirps_file_path = os.path.join(data_in_path, chirps_filename)
            with open(chirps_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            df_chirps = pd.read_csv(chirps_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')
            df_data = df_data.merge(df_chirps, on='ADM2_PCODE')
    
    elif month == 1:
        for month_data in [9, 10, 11]:
            year_data = today.year - 1
            chirps_filename = 'chirps_' + '{0}-{1:02}'.format(year_data, month_data) + '.csv'
            blob_client = blob_service_client.get_blob_client(container='ibf',
                                                            blob='drought/Silver/zwe/chirps/'+ chirps_filename)
            chirps_file_path = os.path.join(data_in_path, chirps_filename)
            with open(chirps_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            df_chirps = pd.read_csv(chirps_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')
            df_data = df_data.merge(df_chirps, on='ADM2_PCODE')

    elif month == 2:
        for month_data in [9, 10, 11, 12]:
            year_data = today.year - 1
            chirps_filename = 'chirps_' + '{0}-{1:02}'.format(year_data, month_data) + '.csv'
            blob_client = blob_service_client.get_blob_client(container='ibf',
                                                            blob='drought/Silver/zwe/chirps/'+ chirps_filename)
            chirps_file_path = os.path.join(data_in_path, chirps_filename)
            with open(chirps_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            df_chirps = pd.read_csv(chirps_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')
            df_data = df_data.merge(df_chirps, on='ADM2_PCODE')

    elif month == 3:
        for month_data in [9, 10, 11, 12]:
            year_data = today.year - 1
            chirps_filename = 'chirps_' + '{0}-{1:02}'.format(year_data, month_data) + '.csv'
            blob_client = blob_service_client.get_blob_client(container='ibf',
                                                            blob='drought/Silver/zwe/chirps/'+ chirps_filename)
            chirps_file_path = os.path.join(data_in_path, chirps_filename)
            with open(chirps_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            df_chirps = pd.read_csv(chirps_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')
            df_data = df_data.merge(df_chirps, on='ADM2_PCODE')
        month_data = 1
        this_year = today.year
        chirps_filename = 'chirps_' + '{0}-{1:02}'.format(this_year, month_data) + '.csv'
        blob_client = blob_service_client.get_blob_client(container='ibf',
                                                        blob='drought/Silver/zwe/chirps/'+ chirps_filename)
        chirps_file_path = os.path.join(data_in_path, chirps_filename)
        with open(chirps_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        df_chirps = pd.read_csv(chirps_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')
        df_data = df_data.merge(df_chirps, on='ADM2_PCODE')
    
    elif month == 4:
        for month_data in [9, 10, 11, 12]:
            year_data = today.year - 1
            chirps_filename = 'chirps_' + '{0}-{1:02}'.format(year_data, month_data) + '.csv'
            blob_client = blob_service_client.get_blob_client(container='ibf',
                                                            blob='drought/Silver/zwe/chirps/'+ chirps_filename)
            chirps_file_path = os.path.join(data_in_path, chirps_filename)
            with open(chirps_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            df_chirps = pd.read_csv(chirps_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')
            df_data = df_data.merge(df_chirps, on='ADM2_PCODE')
        for month_data in [1, 2]:
            this_year = today.year
            chirps_filename = 'chirps_' + '{0}-{1:02}'.format(this_year, month_data) + '.csv'
            blob_client = blob_service_client.get_blob_client(container='ibf',
                                                            blob='drought/Silver/zwe/chirps/'+ chirps_filename)
            chirps_file_path = os.path.join(data_in_path, chirps_filename)
            with open(chirps_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            df_chirps = pd.read_csv(chirps_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')
            df_data = df_data.merge(df_chirps, on='ADM2_PCODE')

    # load latest chirps data
    chirps_filename = 'chirps_' + today.strftime("%Y-%m") + '.csv'
    chirps_file_path = os.path.join(data_in_path, chirps_filename)
    if not os.path.isfile(chirps_file_path):
        blob_client = blob_service_client.get_blob_client(container='ibf',
                                                        blob='drought/Silver/zwe/chirps/'+ chirps_filename)
        with open(chirps_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
    df_chirps = pd.read_csv(chirps_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')
    df_data = df_data.merge(df_chirps, on='ADM2_PCODE').drop(columns=['Unnamed: 0_x', 'Unnamed: 0_y'])

    # save data
    input_filename = 'data_' + today.strftime("%Y-%m") + '.csv'
    input_file_path = os.path.join(data_in_path, input_filename)
    df_data.to_csv(input_file_path)
    with open(input_file_path, "rb") as data:
        blob_client = blob_service_client.get_blob_client(container='ibf',
                                                        blob='drought/Silver/zwe/enso+chirps/'+ input_filename)
        blob_client.upload_blob(data, overwrite=True)

    # return df_data



# Hey there, cookie?



def forecast_model1():
    '''
    Function to load trained model 1 (ENSO) and run the forecast with new input data per province.
    An output csv contained PCODE and so-called alert_threshold will be saved in the datalake.
    
    '''

    # today = datetime.date.today()

    data_in_path = "./data_in"
    adm_path = './shp'
    model_path = "./model"
    data_out_path = './data_out'

    # load adm data
    adm_csv_path = os.path.join(adm_path, 'zwe_admbnda_adm1_zimstat_ocha_20180911.csv')
    zwe_adm1 = pd.read_csv(adm_csv_path)

    regions = np.unique(zwe_adm1['ADM1_PCODE'])

    # load enso data
    enso_filename = 'enso_' + today.strftime("%Y-%m") + '.csv'
    enso_file_path = os.path.join(data_in_path, enso_filename)
    df_enso = pd.read_csv(enso_file_path).drop(columns='Unnamed: 0')#, sep=' ')

    # call ibf blobstorage
    ibf_blobstorage_secrets = get_secretVal('ibf-blobstorage-secrets')
    ibf_blobstorage_secrets = json.loads(ibf_blobstorage_secrets)
    blob_service_client = BlobServiceClient.from_connection_string(ibf_blobstorage_secrets['connection_string'])

    # forecast based on crop-yield
    df_pred_provinces = pd.DataFrame()

    for region in regions:
        df_pred = pd.DataFrame()
        
        # load model
        model_filename = 'zwe_m1_crop_' + region + '_' + str(leadtime) + '_model.json'
        blob_client = blob_service_client.get_blob_client(container='ibf',
                                                          blob='drought/Gold/zwe/model1/' + model_filename)
        model_file_path = os.path.join(model_path, model_filename)
        with open(model_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

        model = XGBClassifier()
        model.load_model(model_file_path)

        # forecast
        pred = model.predict(df_enso)
        df_pred['alert_threshold'] = pred
        df_pred['region'] = region
        df_pred['leadtime'] = leadtime
        df_pred_provinces = df_pred_provinces.append(pd.DataFrame(data=df_pred, index=[0]))

    # save output locally
    predict_file_path = os.path.join(data_out_path, '{0}_zwe_predict.csv'.format(today.strftime("%Y-%m")))
    df_pred_provinces.to_csv(predict_file_path, index=False)

    # upload processed output
    blob_client = blob_service_client.get_blob_client(container='ibf',
                                                      blob='drought/Gold/zwe/{0}_zwe_predict.csv'.format(today.strftime("%Y-%m")))
    with open(predict_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)


    # forecast based on impact database: TBD



def forecast_model2():
    '''
    Function to load trained model 2 (ENSO+CHIRPS) and run the forecast with new input data per province.
    An output csv contained PCODE and so-called alert_threshold will be saved in the datalake.
    
    '''

    # today = datetime.date.today()

    data_in_path = "./data_in"
    adm_path = './shp'
    model_path = "./model"
    data_out_path = './data_out'

    # load adm data
    adm_csv_path = os.path.join(adm_path, 'zwe_admbnda_adm1_zimstat_ocha_20180911.csv')
    zwe_adm1 = pd.read_csv(adm_csv_path)

    regions = np.unique(zwe_adm1['ADM1_PCODE'])

    # load input data
    input_filename = 'data_' + today.strftime("%Y-%m") + '.csv'
    input_file_path = os.path.join(data_in_path, input_filename)
    df_input = pd.read_csv(input_file_path).drop(columns=['Unnamed: 0', 'ADM2_PCODE'])#, sep=' ')
    
    # call ibf blobstorage
    ibf_blobstorage_secrets = get_secretVal('ibf-blobstorage-secrets')
    ibf_blobstorage_secrets = json.loads(ibf_blobstorage_secrets)
    blob_service_client = BlobServiceClient.from_connection_string(ibf_blobstorage_secrets['connection_string'])

    # forecast based on crop-yield
    df_pred_provinces = pd.DataFrame()

    for region in regions:
        # df_pred = pd.DataFrame()
        
        # load model
        model_filename = 'zwe_m2_crop_' + str(leadtime) + '_model.json'
        blob_client = blob_service_client.get_blob_client(container='ibf',
                                                          blob='drought/Gold/zwe/model2/' + model_filename)
        model_file_path = os.path.join(model_path, model_filename)
        with open(model_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

        model = XGBClassifier()
        model.load_model(model_file_path)

        # forecast
        df_input_region = df_input[df_input['ADM1_PCODE']==region].drop(columns='ADM1_PCODE')
        pred = model.predict(df_input_region)
        pred = max(list(pred))
        df_pred = {'alert_threshold': pred,
                   'region': region,
                   'leadtime': leadtime}
        df_pred_provinces = df_pred_provinces.append(pd.DataFrame(data=df_pred, index=[0]))

    # save output locally
    predict_file_path = os.path.join(data_out_path, '{0}_zwe_predict.csv'.format(today.strftime("%Y-%m")))
    df_pred_provinces.to_csv(predict_file_path, index=False)

    # upload processed output
    blob_client = blob_service_client.get_blob_client(container='ibf',
                                                      blob='drought/Gold/zwe/{0}_zwe_predict.csv'.format(today.strftime("%Y-%m")))
    with open(predict_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)


    # forecast based on impact database: TBD



def calculate_impact():
    '''
    Function to calculate impacts of drought per provinces.
    Drought areas are defined by the above function forecast() which is saved in the datalake.
    Impacts include affected population and affected ruminants.
    If a drought is forecasted in a province, entire population and ruminants of the province are considered to be impacted.
    
    '''

    # today = datetime.date.today()

    data_out_path = "./data_out"

    # call ibf blobstorage
    ibf_blobstorage_secrets = get_secretVal('ibf-blobstorage-secrets')
    ibf_blobstorage_secrets = json.loads(ibf_blobstorage_secrets)
    blob_service_client = BlobServiceClient.from_connection_string(ibf_blobstorage_secrets['connection_string'])

    # download to-be-uploaded data: alert_threshold
    if dummy_data:
        blob_client = blob_service_client.get_blob_client(container='ibf',
                                                          blob='drought/Gold/zwe/zwe_m1_crop_predict_dummy.csv')
        predict_file_path = os.path.join(data_out_path, 'zwe_m1_crop_predict_dummy.csv')
        with open(predict_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        df_pred_provinces = pd.read_csv(predict_file_path)
    else:
        predict_file_path = os.path.join(data_out_path, '{0}_zwe_predict.csv'.format(today.strftime("%Y-%m")))
        df_pred_provinces = pd.read_csv(predict_file_path)
    df_pred_provinces = df_pred_provinces.rename(columns={'drought': 'alert_threshold'})

    # load to-be-uploaded data: affected population
    blob_client = blob_service_client.get_blob_client(container='ibf',
                                                      blob='drought/Gold/zwe/zwe_population_adm1.csv')
    pop_file_path = os.path.join(data_out_path, 'zwe_population_adm1')
    with open(pop_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
    df_pop_provinces = pd.read_csv(pop_file_path)
    df_pred_provinces = df_pred_provinces.merge(df_pop_provinces, left_on='region', right_on='ADM1_PCODE')
    df_pred_provinces['population_affected'] = df_pred_provinces['alert_threshold'] * df_pred_provinces['total_pop']

    # load to-be-uploaded data: exposed ruminents
    blob_client = blob_service_client.get_blob_client(container='ibf',
                                                      blob='drought/Gold/zwe/zwe_ruminants_adm1.csv')
    rumi_file_path = os.path.join(data_out_path, 'zwe_ruminants_adm1.csv')
    with open(rumi_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
    df_pop_provinces = pd.read_csv(rumi_file_path)
    df_pred_provinces = df_pred_provinces.merge(df_pop_provinces, left_on='region', right_on='pcode')
    df_pred_provinces['small_ruminants_exposed'] = df_pred_provinces['alert_threshold'] * df_pred_provinces['small_reminant_lsu']


    # load to-be-uploaded data: exposed cattle
    blob_client = blob_service_client.get_blob_client(container='ibf',
                                                      blob='drought/Gold/zwe/zwe_cattle_adm1.csv')
    catt_file_path = os.path.join(data_out_path, 'zwe_cattle_adm1.csv')
    with open(catt_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
    df_pop_provinces = pd.read_csv(catt_file_path)
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
    ibf_credentials = get_secretVal('ibf-credentials-zwe')
    ibf_credentials = json.loads(ibf_credentials)
    IBF_API_URL = ibf_credentials["IBF_API_URL"]
    ADMIN_LOGIN = ibf_credentials["ADMIN_LOGIN"]
    ADMIN_PASSWORD = ibf_credentials["ADMIN_PASSWORD"]

    # log in to IBF API
    login_response = requests.post(f'{IBF_API_URL}/api/user/login',
                                   data=[('email', ADMIN_LOGIN), ('password', ADMIN_PASSWORD)])
    token = login_response.json()['user']['token']

    # loop over layers to upload
    for layer in ['population_affected', 'small_ruminants_exposed', 'cattle_exposed', 'alert_threshold']:
        
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
            # logging.error(f"PIPELINE ERROR AT EMAIL {email_response.status_code}: {email_response.text}")
            # print(r.text)
            raise ValueError()

    # send email
    if 1 in df_pred_provinces['alert_threshold'].values:
        logging.info(f"SENDING ALERT EMAIL")
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