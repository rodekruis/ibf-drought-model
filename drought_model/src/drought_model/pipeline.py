import datetime
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

    try:
        basic_data()
    except Exception as e:
        logging.error(f'Error in basic_data(): {e}')
    try:
        get_new_enso()
    except Exception as e:
        logging.error(f'Error in get_new_enso(): {e}')
    try:
        get_new_chirps()
    except Exception as e:
        logging.error(f'Error in get_new_chirps(): {e}')
    try:
        get_new_vci()
    except Exception as e:
        logging.error(f'Error in get_new_vci(): {e}')
    logging.info(f'Python timer trigger function ran at {utc_timestamp}. \
        Downloaded new ENSO, CHIRPS and VCI of the month.')

    upload_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]
    if month in months_inactive:
        continue_calculation = False
        try:
            post_none_output(upload_date)
            logging.info(f'Done post_output()')
        except Exception as e:
            logging.error(f'Error in post_output(): {e}')
        logging.info(f'Python timer trigger function ran at {utc_timestamp}. \
            Non-trigger generated because of off-season')

    elif month in months_for_model1:
        try:
            forecast_model1()
        except Exception as e:
            logging.error(f'Error in forecast_model1(): {e}')
        continue_calculation = True

    elif month in months_for_model2:
        try:
            arrange_data()
        except Exception as e:
            logging.error(f'Error in arrange_data() for model 2: {e}')
        try:
            forecast_model2()
        except Exception as e:
            logging.error(f'Error in forecast_model2(): {e}')
        continue_calculation = True

    elif month in months_for_model3:
        try:
            arrange_data()
        except Exception as e:
            logging.error(f'Error in arrange_data() for model 3: {e}')
        try:
            forecast_model3()
        except Exception as e:
            logging.error(f'Error in forecast_model3(): {e}')
        continue_calculation = True
    
    if continue_calculation:
        try:
            df_prediction = calculate_impact()
        except Exception as e:
            logging.error(f'Error in calculate_impact(): {e}')
        try:
            post_output(df_prediction, upload_date)
        except Exception as e:
            logging.error(f'Error in post_output(): {e}')

        logging.info(f'Python timer trigger function ran at {utc_timestamp}.')


if __name__ == "__main__":
    main()
