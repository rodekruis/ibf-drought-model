import datetime


# settings for posting output
api_test = True # True/ False; True: to send output to the test server
notify_email = False # True/ False; False: to disable sending email notification
if api_test:
  api_info = 'ibf-credentials'
else:
  api_info = 'ibf-credentials-zwe'

# today
today = datetime.date.today()

# set dummy-mode. When the dummy-mode is on ("dummy = TRUE"), ENSO of August 2020 is extracted.
dummy_time = False # True/ False
dummy_data = False # True/ False

# define month of execution
if dummy_time:
  year = 2021
  month = 9 
else:
  year = datetime.datetime.now().year
  month = datetime.datetime.now().month

# define lead time corresponding to the month of execution
if month == 5:
  leadtime = 11
  leadtime_str = '11-month'
elif month == 6:
  leadtime = 10
  leadtime_str = '10-month'
elif month == 7:
  leadtime = 9
  leadtime_str = '9-month'
elif month == 8:
  leadtime = 8
  leadtime_str = '8-month'
elif month == 9:
  leadtime = 7
  leadtime_str = '7-month'
elif month == 10:
  leadtime = 6
  leadtime_str = '6-month'
elif month == 11:
  leadtime = 5
  leadtime_str = '5-month'
elif month == 12:
  leadtime = 4
  leadtime_str = '4-month'
elif month == 1:
  leadtime = 3
  leadtime_str = '3-month'
elif month == 2:
  leadtime = 2
  leadtime_str = '2-month'
elif month == 3:
  leadtime = 1
  leadtime_str = '1-month'
elif month == 4:
  leadtime = 0
  leadtime_str = '0-month'


# ENSO data URL
enso_url = 'https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt'
chirps_url = 'https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/'


# model selection
months_inactive = [5, 6, 7, 8]
months_for_model1 = [9]
months_for_model2 = [10, 11, 12, 1, 2, 3, 4]

