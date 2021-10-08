import datetime


# ENSO data URL
url = 'https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt'


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