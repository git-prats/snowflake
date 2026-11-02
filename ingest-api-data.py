import os
import sys
import json
import logging
from datetime import datetime
import pytz
import requests
from snowflake.snowpark import Session

# Initiate logging
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Set the IST time zone
ist_timezone = pytz.timezone('Asia/Kolkata')
current_time_ist = datetime.now(ist_timezone)

# Format timestamps
timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')
file_name = f'air_quality_data_{timestamp}.json'
today_string = current_time_ist.strftime('%Y_%m_%d')


def snowpark_basic_auth() -> Session:
    connection_parameters = {
        "ACCOUNT": "YZZZCYO-DR72872",
        "USER": "prathmesh.haware@gmail.com",
        "AUTHENTICATOR": "externalbrowser",
        "ROLE": "SYSADMIN",
        "DATABASE": "DEV_DB",
        "SCHEMA": "STAGE_SCH",
        "WAREHOUSE": "LOAD_WH"
    }
    return Session.builder.configs(connection_parameters).create()


def get_air_quality_data(api_key: str, limit: int):
    api_url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'
    
    params = {
        'api-key': api_key,
        'format': 'json',
        'limit': limit
    }
    headers = {
        'accept': 'application/json'
    }

    try:
        logging.info("Requesting data from data.gov.in...")
        response = requests.get(api_url, params=params, headers=headers, timeout=60)

        if response.status_code != 200:
            logging.error(f"API Error {response.status_code}: {response.text}")
            sys.exit(1)

        logging.info("Data fetched successfully. Parsing JSON...")
        json_data = response.json()

        # Write to local disk
        logging.info(f"Writing to local file: {file_name}")
        with open(file_name, 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, indent=2)

        # Stage location in Snowflake
        stg_location = f"@DEV_DB.STAGE_SCH.RAW_STG/india/{today_string}/"
        
        logging.info("Connecting to Snowflake (browser authentication will open if prompted)...")
        sf_session = snowpark_basic_auth()

        # Build cross-platform safe absolute URI for PUT
        abs_path = os.path.abspath(file_name).replace('\\', '/')
        file_uri = f"file://{abs_path}"

        logging.info(f"Uploading file {file_uri} to stage {stg_location}...")
        sf_session.file.put(file_uri, stg_location, auto_compress=True, overwrite=True)
        logging.info("File placed successfully in Snowflake stage.")

        # Verify uploaded file
        lst_query = f"LIST {stg_location}{file_name}.gz"
        logging.info(f"Checking stage contents: {lst_query}")
        result_lst = sf_session.sql(lst_query).collect()
        logging.info(f"Stage verification output: {result_lst}")

        logging.info("Job completed successfully.")
        return json_data

    except Exception as e:
        logging.error(f"Execution failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    api_key = '579b464db66ec23bdd000001f3011488fa684db1651b8c7af41fa4e7'
    limit_value = 4000
    get_air_quality_data(api_key, limit_value)
