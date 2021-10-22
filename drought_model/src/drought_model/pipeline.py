import datetime
import json
import os
from drought_model.utils import *
from drought_model.settings import *
from azure.storage.blob import BlobServiceClient, BlobClient
import logging
logging.root.handlers = []
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG, filename='ex.log')
# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)


def main():
    utc_timestamp = datetime.datetime.utcnow().isoformat()

    if month in months_inactive:
        continue_running = False
        pass
        logging.info('Python timer trigger function ran at %s. \
                        No output generated because of off-season', utc_timestamp)

    elif month in months_for_model1:
        try:
            get_new_enso()
        except Exception as e:
            logging.error(f'Error in get_new_enso(): {e}')
        try:
            forecast_model1()
        except Exception as e:
            logging.error(f'Error in forecast_model1(): {e}')
        continue_running = True

    elif month in months_for_model2:
        try:
            get_new_enso()
        except Exception as e:
            logging.error(f'Error in get_new_enso(): {e}')
        try:
            get_new_chirps()
        except Exception as e:
            logging.error(f'Error in get_chirps(): {e}')
        try:
            arrange_data()
        except Exception as e:
            logging.error(f'Error in arrange_data(): {e}')
        try:
            forecast_model2()
        except Exception as e:
            logging.error(f'Error in forecast_model2(): {e}')
        continue_running = True
    
    if continue_running:
        try:
            df_prediction = calculate_impact()
        except Exception as e:
            logging.error(f'Error in calculate_impact(): {e}')
        try:
            post_output(df_prediction)
        except Exception as e:
            logging.error(f'Error in post_output(): {e}')

        logging.info('Python timer trigger function ran at %s', utc_timestamp)


if __name__ == "__main__":
    main()
