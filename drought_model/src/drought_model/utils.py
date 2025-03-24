import os
import io
import glob
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



def get_secret_keyvault(secret_name):
    kv_url = f'https://ibf-keys.vault.azure.net'
    az_credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    kv_secretClient = SecretClient(vault_url=kv_url, credential=az_credential)
    secret_value = kv_secretClient.get_secret(secret_name).value
    return secret_value


def get_blob_service_client(blob_path, container_name):
    blobstorage_secrets = get_secret_keyvault('ibf-blobstorage-secrets')
    blobstorage_secrets = json.loads(blobstorage_secrets)
    blob_service_client = BlobServiceClient.from_connection_string(blobstorage_secrets['connection_string'])
    # container = blobstorage_secrets['container']
    return blob_service_client.get_blob_client(container=container_name, blob=blob_path)


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
    rawvci_path = "./data_in/vci_tif"
    os.makedirs(rawvci_path, exist_ok=True)
    model_path = "./model"
    os.makedirs(model_path, exist_ok=True)
    data_out_path = './data_out'
    os.makedirs(data_out_path, exist_ok=True)

    logging.info('basic_data: retrieving basic data from datalake to folders in container')

    # call admin boundary blobstorage
    adm_blobstorage_secrets = get_secret_keyvault('admboundary-blobstorage-secrets')
    adm_blobstorage_secrets = json.loads(adm_blobstorage_secrets)
    blob_service_client = BlobServiceClient.from_connection_string(adm_blobstorage_secrets['connection_string'])

    # load adm1 country shapefile
    shape_name = 'zwe_admbnda_adm1_zimstat_ocha_20180911.geojson'
    blob_path = 'Bronze/zwe/zwe_admbnda_adm1_zimstat_ocha_20180911/' + shape_name
    adm1_shp_path = os.path.join(adm_path, shape_name)
    download_data_from_remote('admin-boundaries', blob_path, adm1_shp_path)
    
    # load country csv file
    csv_name = 'zwe_admbnda_adm1_zimstat_ocha_20180911.csv'
    blob_path = 'Silver/zwe/' + csv_name
    adm1_csv_path = os.path.join(adm_path, csv_name)
    download_data_from_remote('admin-boundaries', blob_path, adm1_csv_path)

    # load adm2 country shapefile
    shape_name = 'zwe_admbnda_adm2_zimstat_ocha_20180911.geojson'
    blob_path = 'Bronze/zwe/zwe_admbnda_adm2_zimstat_ocha_20180911/' + shape_name
    adm2_shp_path = os.path.join(adm_path, 'zwe_admbnda_adm2_zimstat_ocha_20180911.geojson')
    download_data_from_remote('admin-boundaries', blob_path, adm2_shp_path)
    
    # load adm2 country csv file
    csv_name = 'zwe_admbnda_adm2_zimstat_ocha_20180911.csv'
    blob_path = 'Silver/zwe/' + csv_name
    adm2_csv_path = os.path.join(adm_path, csv_name)
    download_data_from_remote('admin-boundaries', blob_path, adm2_csv_path)

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
    blob_path = 'drought/Silver/zwe/enso/'+ enso_filename

    # read new enso data
    logging.info('get_new_enso: downloading new ENSO dataset')
    page = access_enso(enso_url)
    df = pd.read_csv(io.StringIO(page), delim_whitespace=True)
    if month == 1:
        year_enso = year + 1
    else:
        year_enso = year
    df['YR'] = df['YR'].shift(-1).fillna(year_enso)

    df1 = df.copy()
    df1.index=[0]*len(df1)
    df1 = df1.pivot(columns='SEAS', values='ANOM', index='YR')

    columns = ['FMA', 'MAM', 'AMJ', 'MJJ', 'JJA', 'JAS', 'ASO', 'SON', 'OND',
                'NDJ', 'DJF', 'JFM']
    df1 = df1.reindex(columns, axis=1).reset_index()

    df1[['NDJ', 'DJF', 'JFM']] = df1[['NDJ', 'DJF', 'JFM']].shift(-1)
    # df1 = df1.drop(columns=['FMA', 'MAM', 'AMJ', 'MJJ', 'JJA'])
    df1.dropna(subset=columns, how='all', inplace=True)

    # pick and arrange enso data
    logging.info('get_new_enso: extracting ENSO of corressponding month(s)')
    if month == 9: # lead time 7 month
        if (df.tail(1)['SEAS'] == 'JJA').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(['YR', 'index',
                                    'JAS', 'ASO', 'SON', 'OND', 'NDJ', 'DJF', 'JFM'], axis=1)
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 10: # lead time 6 month
        if (df.tail(1)['SEAS'] == 'JAS').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'ASO', 'SON', 'OND', 'NDJ', 'DJF', 'JFM'], axis=1)
            # df_enso = df_enso[df_enso['Year']==year].drop(columns='Year')
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()
    
    elif month == 11: # lead time 5 month
        if (df.tail(1)['SEAS'] == 'ASO').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'SON', 'OND', 'NDJ', 'DJF', 'JFM'], axis=1)
            # df_enso = df_enso[df_enso['Year']==year].drop(columns='Year')
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 12: # lead time 4 month
        if (df.tail(1)['SEAS'] == 'SON').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'OND', 'NDJ', 'DJF', 'JFM'], axis=1)
            # df_enso = df_enso[df_enso['Year']==year].drop(columns='Year')
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 1: # lead time 3 month
        if (df.tail(1)['SEAS'] == 'OND').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'NDJ', 'DJF', 'JFM'], axis=1)
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 2: # lead time 2 month
        if (df.tail(1)['SEAS'] == 'NDJ').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'DJF', 'JFM'], axis=1)
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 3: # lead time 1 month
        if (df.tail(1)['SEAS'] == 'DJF').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index',
                                        'JFM'], axis=1)
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 4: # lead time
        if (df.tail(1)['SEAS'] == 'JFM').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index'], axis=1)
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()

    elif month == 5: # lead time
        if (df.tail(1)['SEAS'] == 'FMA').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index'], axis=1)
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()
    
    elif month == 6: # lead time
        if (df.tail(1)['SEAS'] == 'MAM').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index'], axis=1)
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()
    
    elif month == 7: # lead time
        if (df.tail(1)['SEAS'] == 'AMJ').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index'], axis=1)
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()
    
    elif month == 8: # lead time
        if (df.tail(1)['SEAS'] == 'MJJ').values:
            df_enso = df1.tail(1).reset_index()
            df_enso = df_enso.drop(columns=['YR', 'index'], axis=1)
            df_enso.to_csv(enso_file_path, index=False)
            save_data_to_remote(enso_file_path, blob_path, 'ibf')
        else:
            logging.error('ENSO data not updated')
            raise ValueError()
    
    logging.info('get_new_enso: done')
    # return df_enso


def access_chirps(url):
    
    logging.info('access_chirps: accessing CHIRPS data source')

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
            logging.info("CHIRPS data source access failed. "
                          "Trying again in 10 minutes")
            time.sleep(timeToRetry)
    if accessDone == False:
        logging.error('ERROR: CHIRPS data source access failed for ' +
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
    
    df_chirps_raw = pd.read_csv(adm_csv_path)[['ADM2_PCODE']]

    # access CHIRPS data source
    logging.info('get_new_chirps: downloading new CHIRPS dataset')
    
    if month == 1:
        year_data = year - 1 
        month_data = 12
    else:
        year_data = year
        month_data = month - 1
    days = np.arange(1, calendar.monthrange(year_data, month_data)[1]+1, 1)
        
    chirps_url1 = chirps_url + str(year_data) + '/'
    urls = access_chirps(chirps_url1)#[1:]
    file_urls = sorted([i for i in urls if i.split('/')[-1].startswith(f'chirps-v2.0.{year_data}.{month_data:02d}')], reverse=True)
    if not file_urls:
        logging.error('CHIRPS data not updated')
    
    filename_list = [i.split('/')[-1] for i in file_urls]
    
    # download new CHIRPS data
    for i in np.arange(0,len(filename_list)):
        wget_download(file_urls[i], rawchirps_path,filename_list[i])
        batch_unzip = "gzip -d -f %s" %(filename_list[i])
        subprocess.call(batch_unzip, cwd=rawchirps_path, shell=True)
        rawdata_file_path = os.path.join(rawchirps_path, filename_list[i].replace('.gz', ''))
        blob_path = 'drought/Bronze/chirps/new_download/' + filename_list[i].replace('.gz', '')
        save_data_to_remote(rawdata_file_path, blob_path, 'ibf')

    filename_list = sorted(glob.glob(rawchirps_path + '/*.tif'), reverse=False)
    i = 0
    for filename in filename_list:
        # raster_path = os.path.abspath(os.path.join(rawchirps_path, filename))
        mean = zonal_stats(adm_shp_path, filename, stats='mean', nodata=-9999)
        df_chirps_raw[f'{days[i]:02d}'] = pd.DataFrame(data=mean)['mean']
        i += 1
    
    # calculate monthly cumulative
    logging.info('get_new_chirps: calculating monthly cumulative rainfall')
    df_chirps = cumulative_and_dryspell(df_chirps_raw, 'ADM2_PCODE', month_data)

    processeddata_filename = 'chirps_' + today.strftime("%Y-%m") + '.csv'
    processeddata_file_path = os.path.join(data_in_path, processeddata_filename)
    df_chirps.to_csv(processeddata_file_path, index=False)
    blob_path = 'drought/Silver/zwe/chirps/' + processeddata_filename
    save_data_to_remote(processeddata_file_path, blob_path, 'ibf')

    logging.info('get_new_chirps: done')
    # return df_chirps


def access_vci(url):
    '''
    Function to access and get VCI data.
    Retry to access to the datasource in 10 min if failed.
    Stop retrying if after 1 hour.

    '''

    logging.info('access_vci: accessing VCI data source')

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
            logging.info("VCI data source access failed. "
                          "Trying again in 10 minutes")
            time.sleep(timeToRetry)
    if accessDone == False:
        logging.error('ERROR: VCI data source access failed for ' +
                      str(timeToTryAccess / 3600) + ' hours')
        raise ValueError()
    
    soup = BeautifulSoup(page, 'html.parser')

    return [url + node.get('href') for node in soup.find_all('a') if node.get('href')]


def get_new_vci():
    '''
    Function to download raw daily VCI data
    and return monthly average per adm2
    '''
    # folders 
    data_in_path = "./data_in"
    adm_path = "./shp"
    rawvci_path = "./data_in/vci_tif"

    # load country file path
    adm_shp_path = os.path.join(adm_path, 'zwe_admbnda_adm2_zimstat_ocha_20180911.geojson')
    adm_csv_path = os.path.join(adm_path, 'zwe_admbnda_adm2_zimstat_ocha_20180911.csv')
    
    df_vci = pd.read_csv(adm_csv_path)[['ADM2_PCODE']]

    if month == 1:
        year_data = year - 1 
        month_data = 12
    else:
        year_data = year
        month_data = month - 1
    week_numbers = list_week_number(year_data, month_data)

    logging.info('get_new_vci: downloading new VCI dataset')

    file_urls = []
    filename_list = []
    filepath_list = []
    for week_number in week_numbers:
        # # specify file name and its url
        # if week_number-min(week_numbers) >= 50:
        #     year_data_vci = year - 1
        # else:
        #     year_data_vci = year_data
        filename = f'VHP.G04.C07.j01.P{year_data}{week_number:03d}.VH.VCI.tif'
        filepath_local = os.path.join(rawvci_path, filename)
        file_url = vci_url + filename
        file_urls.append(file_url)
        filename_list.append(filename)
        filepath_list.append(filepath_local)

        # download file
        wget_download(file_url, rawvci_path, filename)
    
    for week_number, filename, filepath_local in zip(week_numbers, filename_list, filepath_list):
        blob_path = 'drought/Bronze/vci/' + filename
        save_data_to_remote(filepath_local, blob_path, 'ibf')

        # calculate average vci per admin
        mean = zonal_stats(adm_shp_path, filepath_local, stats='mean', nodata=-9999)
        df_vci[f'{week_number:02d}'] = pd.DataFrame(data=mean)['mean']

    # calculate montly mean
    df_vci[f'{month_data:02}_vci'] = df_vci.loc[:,f"{week_numbers[0]:02d}":f"{week_numbers[-1]:02d}"].mean(axis=1)
    df_vci = df_vci[['ADM2_PCODE', f'{month_data:02}_vci']]
    
    processeddata_filename = 'vci_' + today.strftime("%Y-%m") + '.csv'
    processeddata_file_path = os.path.join(data_in_path, processeddata_filename)
    df_vci.to_csv(processeddata_file_path, index=False)
    blob_path = 'drought/Silver/zwe/vci/' + processeddata_filename
    save_data_to_remote(processeddata_file_path, blob_path, 'ibf')

    logging.info('get_new_vci: done')
    # return df_vci


def arrange_data():
    '''
    Function to arrange ENSO, CHIRPS and VCI data depending on the month.
    This is only for forecast_model2() and forecast_model3().
    '''
    
    # today = datetime.date.today()

    # folder of processed data csv
    data_in_path = "./data_in"
    adm_path = "./shp"
    
    # desired order of columns
    cols_order = ['ADM1_PCODE', 'ADM2_PCODE',\
        'JAS', 'ASO', 'SON', 'OND', 'NDJ', 'DJF', 'JFM', \
        '09_p_cumul', '10_p_cumul', '11_p_cumul', '12_p_cumul', \
        '01_p_cumul', '02_p_cumul', '03_p_cumul', \
        '09_dryspell', '10_dryspell', '11_dryspell', '12_dryspell', \
        '01_dryspell', '02_dryspell', '03_dryspell', \
        '09_vci', '10_vci', '11_vci', '12_vci', \
        '01_vci', '02_vci', '03_vci', \
        'p_cumul', 'vci_avg'] 

    # specify processed data file name
    input_filename = 'data_' + today.strftime("%Y-%m") + '.csv'
    input_file_path = os.path.join(data_in_path, input_filename)

    # load country file path
    adm_csv_path = os.path.join(adm_path, 'zwe_admbnda_adm2_zimstat_ocha_20180911.csv')
    df_adm = pd.read_csv(adm_csv_path)[['ADM1_PCODE', 'ADM2_PCODE']]

    # load enso data
    enso_filename = 'enso_' + today.strftime("%Y-%m") + '.csv'
    enso_file_path = os.path.join(data_in_path, enso_filename)
    df_enso = pd.read_csv(enso_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')
    df_data = df_adm.merge(df_enso, how='cross')

    logging.info('arrange_data: arranging ENSO and CHIRPS datasets for the model')

    # load relevant month(s) chirps and vci data, and append to the big dataframe
    if month == 11:
        month_data = 10
        this_year = today.year
        df_chirps = get_dataframe_from_remote('chirps', this_year, month_data, data_in_path)
        df_data = df_data.merge(df_chirps, on='ADM2_PCODE')
    if month == 12:
        months_data = (10, 11)
        years_data =  ((today.year, ) * len(months_data))
        # this_year = today.year
        for month_data, year_data in zip(months_data, years_data):
            df_chirps = get_dataframe_from_remote('chirps', year_data, month_data, data_in_path)
            df_data = df_data.merge(df_chirps, on='ADM2_PCODE')
    if month == 1:
        months_data = (10, 11, 12)
        years_data = ((today.year-1, ) * len(months_data))
        for month_data, year_data in zip(months_data, years_data):
            df_chirps = get_dataframe_from_remote('chirps', year_data, month_data, data_in_path)
            df_vci = get_dataframe_from_remote('vci', year_data, month_data, data_in_path)
            df_data = df_data.merge(df_chirps, on='ADM2_PCODE')
            df_data = df_data.merge(df_vci, on='ADM2_PCODE')
    if month == 2:
        months_data = (10, 11, 12, 1)
        years_data = (today.year-1, today.year-1, today.year-1, today.year)
        for month_data, year_data in zip(months_data, years_data):
            df_chirps = get_dataframe_from_remote('chirps', year_data, month_data, data_in_path)
            df_vci = get_dataframe_from_remote('vci', year_data, month_data, data_in_path)
            df_data = df_data.merge(df_chirps, on='ADM2_PCODE')
            df_data = df_data.merge(df_vci, on='ADM2_PCODE')
    if month == 3:
        months_data = (10, 11, 12, 1, 2)
        years_data = (today.year-1, today.year-1, today.year-1, today.year, today.year)
        for month_data, year_data in zip(months_data, years_data):
            df_chirps = get_dataframe_from_remote('chirps', year_data, month_data, data_in_path)
            df_vci = get_dataframe_from_remote('vci', year_data, month_data, data_in_path)
            df_data = df_data.merge(df_chirps, on='ADM2_PCODE')
            df_data = df_data.merge(df_vci, on='ADM2_PCODE')
    if month == 4:
        month_data = (10, 11, 12, 1, 2, 3)
        years_data = (today.year-1, today.year-1, today.year-1, today.year, today.year, today.year)
        for month_data, year_data in zip(months_data, years_data):
            df_chirps = get_dataframe_from_remote('chirps', year_data, month_data, data_in_path)
            df_vci = get_dataframe_from_remote('vci', year_data, month_data, data_in_path)
            df_data = df_data.merge(df_chirps, on='ADM2_PCODE')
            df_data = df_data.merge(df_vci, on='ADM2_PCODE')

    # load latest chirps data
    df_chirps = get_dataframe_from_remote('chirps', year, month, data_in_path)
    df_data = df_data.merge(df_chirps, on='ADM2_PCODE')#.drop(columns=['Unnamed: 0_x', 'Unnamed: 0_y'])
    if month in months_for_model3:
        df_vci = get_dataframe_from_remote('vci', year, month, data_in_path)
        df_data = df_data.merge(df_vci, on='ADM2_PCODE')#.drop(columns=['Unnamed: 0_x', 'Unnamed: 0_y'])

    # add cumulative chirps column, total dryspell, and averaged vci 
    if month == 11:
        df_data['p_cumul'] = df_data[['09_p_cumul', '10_p_cumul']].sum(axis=1)
        # df_data['dryspell'] = df_data[['09_dryspell', '10_dryspell']].sum(axis=1)
        subfoldername = 'enso+chirps'
    elif month == 12:
        df_data['p_cumul'] = df_data[['09_p_cumul', '10_p_cumul', \
            '11_p_cumul']].sum(axis=1)
        # df_data['dryspell'] = df_data[['09_dryspell', '10_dryspell', \
        #     '11_dryspell']].sum(axis=1)
        subfoldername = 'enso+chirps'
    elif month == 1:
        df_data['p_cumul'] = df_data[['09_p_cumul', '10_p_cumul', \
            '11_p_cumul', '12_p_cumul']].sum(axis=1)
        # df_data['dryspell'] = df_data[['09_dryspell', '10_dryspell', \
        #     '11_dryspell', '12_dryspell']].sum(axis=1)
        df_data['vci_avg'] = df_data[['09_vci', '10_vci', \
            '11_vci', '12_vci']].sum(axis=1)
        subfoldername = 'enso+chirps+vci'
    elif month == 2:
        df_data['p_cumul'] = df_data[['09_p_cumul', '10_p_cumul', \
            '11_p_cumul', '12_p_cumul', '01_p_cumul']].sum(axis=1)
        # df_data['dryspell'] = df_data[['09_dryspell', '10_dryspell', \
        #     '11_dryspell', '12_dryspell', '01_dryspell']].sum(axis=1)
        df_data['vci_avg'] = df_data[['09_vci', '10_vci', \
            '11_vci', '12_vci', '01_vci']].sum(axis=1)
        subfoldername = 'enso+chirps+vci'
    elif month == 3:
        df_data['p_cumul'] = df_data[['09_p_cumul', '10_p_cumul', \
            '11_p_cumul', '12_p_cumul', '01_p_cumul', '02_p_cumul']].sum(axis=1)
        # df_data['dryspell'] = df_data[['09_dryspell', '10_dryspell', \
        #     '11_dryspell', '12_dryspell', '01_dryspell', '02_dryspell']].sum(axis=1)
        df_data['vci_avg'] = df_data[['09_vci', '10_vci', \
            '11_vci', '12_vci', '01_vci', '02_vci']].sum(axis=1)
        subfoldername = 'enso+chirps+vci'
    elif month == 4:
        df_data['p_cumul'] = df_data[['09_p_cumul', '10_p_cumul', \
            '11_p_cumul', '12_p_cumul', '01_p_cumul', '02_p_cumul', '03_p_cumul']].sum(axis=1)
        # df_data['dryspell'] = df_data[['09_dryspell', '10_dryspell', \
        #     '11_dryspell', '12_dryspell', '01_dryspell', '02_dryspell', '03_dryspell']].sum(axis=1)
        df_data['vci_avg'] = df_data[['09_vci', '10_vci', \
            '11_vci', '12_vci', '01_vci', '02_vci', '03_vci']].sum(axis=1)
        subfoldername = 'enso+chirps+vci'

    # save data
    df_data = reorder_columns(df_data, cols_order)
    df_data.to_csv(input_file_path, index=False)
    blob_path = f'drought/Silver/zwe/{subfoldername}/{input_filename}'
    save_data_to_remote(input_file_path, blob_path, 'ibf')

    logging.info('arrange_data: done')
    # return df_data


# Hey there, cookie?


def forecast_model1():
    '''
    Function to load trained model 1 (ENSO) and run the forecast with new input data per province.
    An output csv contained PCODE and so-called forecast_severity will be saved in the datalake.
    
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
    df_enso = pd.read_csv(enso_file_path)#.drop(columns='Unnamed: 0')#, sep=' ')

    # forecast based on crop-yield
    logging.info('forecast_model1: forecasting with model 1 ENSO-only')
    df_pred_provinces = pd.DataFrame()

    for region in regions:
        df_pred = pd.DataFrame()
        
        # load model
        model_filename = 'zwe_m1_crop_' + region + '_' + str(leadtime) + '_model.json'
        blob_path = 'drought/Gold/zwe/model1/' + model_filename
        model_filepath = os.path.join(model_path, model_filename)
        download_data_from_remote('ibf', blob_path, model_filepath)

        model = XGBClassifier()
        model.load_model(model_filepath)

        # forecast
        pred = model.predict(df_enso)
        df_pred['forecast_severity'] = pred
        df_pred['region'] = region
        df_pred['leadtime'] = leadtime
        df_pred_provinces = df_pred_provinces.append(pd.DataFrame(data=df_pred, index=[0]))

    # save output locally
    predict_file_path = os.path.join(data_out_path, f'{year}-{month:02}_zwe_predict.csv')
    df_pred_provinces.to_csv(predict_file_path, index=False)

    # upload processed output
    blob_path = f'drought/Gold/zwe/{year}-{month:02}_zwe_predict.csv'
    save_data_to_remote(predict_file_path, blob_path, 'ibf')

    logging.info('forecast_model1: done')
    # forecast based on impact database: TBD


def forecast_model2():
    '''
    Function to load trained model 2 (ENSO+CHIRPS) and run the forecast with new input data per province.
    An output csv contained PCODE and so-called forecast_severity will be saved in the datalake.
    
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
    df_input = pd.read_csv(input_file_path).drop(columns=['ADM2_PCODE'])#, sep=' ')
    
    # forecast based on crop-yield
    logging.info('forecast_model2: forecasting with model 2 ENSO+CHIRPS')
    df_pred_provinces = pd.DataFrame()

    for region in regions:
        # df_pred = pd.DataFrame()
        
        # load model
        model_filename = 'zwe_m2_crop_' + str(leadtime) + '_model.json'
        blob_path = 'drought/Gold/zwe/model2/' + model_filename
        model_filepath = os.path.join(model_path, model_filename)
        download_data_from_remote('ibf', blob_path, model_filepath)

        model = XGBClassifier()
        model.load_model(model_filepath)
        # forecast
        df_input_region = df_input[df_input['ADM1_PCODE']==region].drop(columns='ADM1_PCODE')
        pred = model.predict(df_input_region)
        pred = max(list(pred))
        df_pred = {'forecast_severity': pred,
                   'region': region,
                   'leadtime': leadtime}
        df_pred_provinces = df_pred_provinces.append(pd.DataFrame(data=df_pred, index=[0]))

    # save output locally
    predict_file_path = os.path.join(data_out_path, f'{year}-{month:02}_zwe_predict.csv')
    df_pred_provinces.to_csv(predict_file_path, index=False)

    # upload processed output
    blob_path = f'drought/Gold/zwe/{year}-{month:02}_zwe_predict.csv'
    save_data_to_remote(predict_file_path, blob_path, 'ibf')

    logging.info('forecast_model2: done')
    # forecast based on impact database: TBD


def forecast_model3():
    '''
    Function to load trained model 3 (ENSO+CHIRPS+DrySpell+VCI) and run the forecast with new input data per province.
    An output csv contained PCODE and so-called forecast_severity will be saved in the datalake.
    
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
    df_input = pd.read_csv(input_file_path).drop(columns=['ADM2_PCODE'])#, sep=' ')
    
    # forecast based on crop-yield
    logging.info('forecast_model3: forecasting with model 3 ENSO+CHIRPS+DrySpell+VCI')
    df_pred_provinces = pd.DataFrame()

    for region in regions:
        
        # load model
        model_filename = 'zwe_m3_crop_' + str(leadtime) + '_model.json'
        blob_path = 'drought/Gold/zwe/model3/' + model_filename
        model_filepath = os.path.join(model_path, model_filename)
        download_data_from_remote('ibf', blob_path, model_filepath)

        model = XGBClassifier()
        model.load_model(model_filepath)

        # forecast
        df_input_region = df_input[df_input['ADM1_PCODE']==region].drop(columns='ADM1_PCODE')
        pred = model.predict(df_input_region)
        pred = round(np.median(list(pred)))
        df_pred = {'forecast_severity': pred,
                   'region': region,
                   'leadtime': leadtime}
        df_pred_provinces = df_pred_provinces.append(pd.DataFrame(data=df_pred, index=[0]))

    # save output locally
    predict_file_path = os.path.join(data_out_path, f'{year}-{month:02}_zwe_predict.csv')
    df_pred_provinces.to_csv(predict_file_path, index=False)

    # upload processed output
    blob_path = f'drought/Gold/zwe/{year}-{month:02}_zwe_predict.csv'
    save_data_to_remote(predict_file_path, blob_path, 'ibf')

    logging.info('forecast_model3: done')



def calculate_impact():
    '''
    Function to calculate impacts of drought per provinces.
    Drought areas are defined by the above function forecast() which is saved in the datalake.
    Impacts include affected population and affected ruminants.
    If a drought is forecasted in a province, entire population and ruminants of the province are considered to be impacted.
    
    '''

    logging.info('calculate_impact: calculating drought impact')

    data_out_path = "./data_out"

    # download to-be-uploaded data: forecast_severity
    if dummy_data:
        blob_path = 'drought/Gold/zwe/zwe_m1_crop_predict_dummy.csv'
        predict_filepath = os.path.join(data_out_path, 'zwe_m1_crop_predict_dummy.csv')
        download_data_from_remote('ibf', blob_path, predict_filepath)
        df_pred_provinces = pd.read_csv(predict_filepath)
    else:
        predict_file_path = os.path.join(data_out_path, f'{year}-{month:02}_zwe_predict.csv')
        df_pred_provinces = pd.read_csv(predict_file_path)
    df_pred_provinces = df_pred_provinces.rename(columns={'drought': 'forecast_severity'})
    df_pred_provinces['forecast_trigger'] = df_pred_provinces['forecast_severity'] # In this case forecast_trigger is the same as forecast_severity

    # load to-be-uploaded data: affected population
    blob_path = 'drought/Gold/zwe/zwe_population_adm1.csv'
    pop_filepath = os.path.join(data_out_path, 'zwe_population_adm1')
    download_data_from_remote('ibf', blob_path, pop_filepath)
    df_pop_provinces = pd.read_csv(pop_filepath)
    df_pred_provinces = df_pred_provinces.merge(df_pop_provinces, left_on='region', right_on='ADM1_PCODE')
    df_pred_provinces['population_affected'] = df_pred_provinces['forecast_severity'] * df_pred_provinces['total_pop']
    df_pred_provinces.drop(columns=['ADM1_EN', 'ADM1_PCODE', 'ADM0_EN', 'ADM0_PCODE', \
        'total_pop'], inplace=True)

    # load to-be-uploaded data: exposed ruminents
    blob_path = 'drought/Gold/zwe/zwe_ruminants_adm1.csv'
    rumi_filepath = os.path.join(data_out_path, 'zwe_ruminants_adm1.csv')
    download_data_from_remote('ibf', blob_path, rumi_filepath)
    df_pop_provinces = pd.read_csv(rumi_filepath)
    df_pred_provinces = df_pred_provinces.merge(df_pop_provinces, left_on='region', right_on='pcode')
    df_pred_provinces['small_ruminants_exposed'] = df_pred_provinces['forecast_severity'] * df_pred_provinces['small_reminant_lsu']
    df_pred_provinces.drop(columns=['admin1Name_en', 'pcode', 'season', 'small_reminant_lsu'], inplace=True)

    # load to-be-uploaded data: exposed cattle
    blob_path = 'drought/Gold/zwe/zwe_cattle_adm1.csv'
    catt_filepath = os.path.join(data_out_path, 'zwe_cattle_adm1.csv')
    download_data_from_remote('ibf', blob_path, catt_filepath)
    df_pop_provinces = pd.read_csv(catt_filepath)
    df_pred_provinces = df_pred_provinces.merge(df_pop_provinces, left_on='region', right_on='pcode')
    df_pred_provinces['cattle_exposed'] = df_pred_provinces['forecast_severity'] * df_pred_provinces['cattle_lsu']
    df_pred_provinces.drop(columns=['admin1Name_en', 'pcode', 'season', 'cattle_lsu'], inplace=True)

    logging.info('calculate_impact: done')

    return(df_pred_provinces)


def post_output(df_pred_provinces, upload_date):
    '''
    Function to post layers into IBF System.
    For every layer, the function calls IBF API and post the layer in the format of json.
    The layers are forecast_severity/forecast_trigger (drought or not drought per provinces), population_affected and ruminants_affected.

    '''

    logging.info('post_output: sending output to dashboard')

    # load credentials to IBF API
    ibf_credentials = get_secret_keyvault(api_info)
    ibf_credentials = json.loads(ibf_credentials)
    IBF_API_URL = ibf_credentials["IBF_API_URL"]
    ADMIN_LOGIN = ibf_credentials["ADMIN_LOGIN"]
    ADMIN_PASSWORD = ibf_credentials["ADMIN_PASSWORD"]

    # log in to IBF API
    login_response = requests.post(f'{IBF_API_URL}/api/user/login',
                                   data=[('email', ADMIN_LOGIN), ('password', ADMIN_PASSWORD)])
    token = login_response.json()['user']['token']

    # loop over layers to upload
    for layer in ['population_affected', 'small_ruminants_exposed', 'cattle_exposed', 'forecast_severity', 'forecast_trigger']:
        
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
        exposure_data["date"] = upload_date
        
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

    # process events (and send email if applicable)
    post_process_events(upload_date, IBF_API_URL, token)



def post_none_output(upload_date):
    '''
    Function to post non-trigger layers into IBF System during inactive months.
    For every layer, the function calls IBF API and post the layer in the format of json.
    The layers are forecast_severity/forecast_trigger (drought or not drought per provinces), population_affected and ruminants_affected.
    
    '''

    data_out_path = "./data_out"

    file_path_remote = 'drought/Gold/zwe/zwe_nontrigger.csv'
    predict_filepath = os.path.join(data_out_path, 'zwe_nontrigger.csv')
    download_data_from_remote('ibf', file_path_remote, predict_filepath)
    df_pred_provinces = pd.read_csv(predict_filepath)

    logging.info('post_none_output: sending non-trigger output to dashboard')

    # load credentials to IBF API
    ibf_credentials = get_secret_keyvault(api_info)
    ibf_credentials = json.loads(ibf_credentials)
    IBF_API_URL = ibf_credentials["IBF_API_URL"]
    ADMIN_LOGIN = ibf_credentials["ADMIN_LOGIN"]
    ADMIN_PASSWORD = ibf_credentials["ADMIN_PASSWORD"]

    # log in to IBF API
    login_response = requests.post(f'{IBF_API_URL}/api/user/login',
                                   data=[('email', ADMIN_LOGIN), ('password', ADMIN_PASSWORD)])
    token = login_response.json()['user']['token']

    # loop over layers to upload
    for layer in ['population_affected', 'small_ruminants_exposed', 'cattle_exposed', 'forecast_severity', 'forecast_trigger']:
        
        # prepare layer
        exposure_data = {'countryCodeISO3': 'ZWE'}
        exposure_place_codes = []
        for ix, row in df_pred_provinces.iterrows():
            exposure_entry = {'placeCode': row['region'],
                                                'amount': 0}
            exposure_place_codes.append(exposure_entry)
        exposure_data['exposurePlaceCodes'] = exposure_place_codes
        exposure_data["adminLevel"] = 1
        exposure_data["leadTime"] = leadtime_str
        exposure_data["dynamicIndicator"] = layer
        exposure_data["disasterType"] = 'drought'
        exposure_data["date"] = upload_date
        
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

    logging.info('post_none_output: sending output to dashboard')

    
    # process events (and send email if applicable)
    post_process_events(upload_date, IBF_API_URL, token)

def post_process_events(upload_date, IBF_API_URL, token):
    '''
    process events (and send email if applicable)
    
    '''
    
    if notify_email:
        api_path = 'events/process' #default for noNotifications=false
    else:
        api_path = 'events/process?noNotifications=true'
    process_events_response = requests.post(f'{IBF_API_URL}/api/{api_path}',
                                json={'countryCodeISO3': 'ZWE',
                                        'disasterType': 'drought',
                                        'date': upload_date},
                                headers={'Authorization': 'Bearer ' + token,
                                        'Content-Type': 'application/json',
                                        'Accept': 'application/json'})
    if process_events_response.status_code >= 400:
        # logging.error(f"PIPELINE ERROR AT EMAIL {process_events_response.status_code}: {process_events_response.text}")
        # print(r.text)
        raise ValueError()
        exit(0)

def list_week_number(year, month):
    '''
    List week number of a year in a month
    '''
    first_date_of_month = datetime.date(year, month, 1)
    date_of_next_month = first_date_of_month + datetime.timedelta(days=32) 
    last_date_of_month = date_of_next_month.replace(day=1)
    delta = last_date_of_month - first_date_of_month

    week_numbers = []
    for i in range(delta.days + 1):
        day = first_date_of_month + datetime.timedelta(days=i)
        week_number = day.isocalendar()[1]
        week_numbers.append(week_number)

    week_list = np.unique(week_numbers)

    return week_list


def wget_download(file_url, local_path, filename):
    '''
    Function to wget download file from url.
    If failed, try again for 5 time.
    Save downloaded file to local container and to datalake
    '''
    if not os.path.isfile(os.path.join(local_path, filename)):
        download_command = f"wget -nd -e robots=off -A {filename} {file_url}"
        downloaded = False
        attempts = 0
        while not downloaded and attempts < 5:
            try:
                subprocess.call(download_command, cwd=local_path, shell=True)
                logging.info(f'{filename} downloaded')
                downloaded = True
            except:
                logging.info(f'Attempt to download {filename} failed, retry {attempts + 1}')
                time.sleep(10)
                attempts += 1
        if not downloaded:
            logging.error(f'Failed to download {filename}')


def get_dataframe_from_remote(data, year, month, folder_local):
    '''
    Get past processed chirps data as dataframe from datalake
    '''
    filename = f'{data}_{year}-{month:02}.csv'
    file_path_remote = f'drought/Silver/zwe/{data}/'+ filename
    file_path_local = os.path.join(folder_local, filename)
    download_data_from_remote('ibf', file_path_remote, file_path_local)
    df = pd.read_csv(file_path_local)
    return df


def download_data_from_remote(container, file_path_remote, file_path_local):
    '''
    Download data from datalake
    '''
    with open(file_path_local, "wb") as download_file:
        blob_client = get_blob_service_client(file_path_remote, container)
        download_file.write(blob_client.download_blob().readall())
    # df = pd.read_csv(file_path_local)
    # return file_path_local # df


def save_data_to_remote(file_path_local, file_path_remote, container):
    '''
    Function to save data to datalake
    '''
    with open(file_path_local, "rb") as upload_file:
        blob_client = get_blob_service_client(file_path_remote, container)
        blob_client.upload_blob(upload_file, overwrite=True)


def cumulative_and_dryspell(df_precip, admin_column, month_data):
    '''
    Function to calculate:
    - monthly cumulative rainfall
    - number of dryspell event by definition below based on 14-day rolling
    cumulative sum of rainfall per district.
    Input is a dataframe of rainfall. Each row is a daily rainfall of a district.
    '''

    df_precip = df_precip.melt(id_vars=admin_column, var_name='date', value_name='rain')
    
    # calculate 14-day rolling cumulative rainfall per admin
    df_precip['rolling_cumul'] = df_precip.groupby(admin_column)['rain'].\
        rolling(14).sum().reset_index(0,drop=True)
    
    # dry spell if the cumulative rainfall is below 2mm
    df_precip['dryspell'] = np.where(df_precip['rolling_cumul'] <= 2, 1, 0)
    
    # count "dryspell" event and cumulative rainfall per month per admin
    precip_dryspell = df_precip.groupby([admin_column])['dryspell'].sum().\
        reset_index()
    precip_cumul = df_precip.groupby([admin_column])['rain'].sum().\
        reset_index()

    precip_processed = precip_dryspell.merge(precip_cumul, on=[admin_column])
    precip_processed = precip_processed.rename(
        columns={'rain': f'{month_data:02}_p_cumul', 
                'dryspell': f'{month_data:02}_dryspell'})

    return precip_processed


def reorder_columns(df, cols_order):
    '''
    Function to rearrange columns of a dataframe in a desired order.
    '''
    cols_df = list(df.columns)
    cols_df_in_order = [col for col in cols_order if col in cols_df] 
    df_reordered = df[cols_df_in_order]

    return df_reordered