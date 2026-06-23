# Apex wealth management stock pipeline
# ETL script to extract stock data from an API, transform it, and load it into a database

# Import necessary libraries
import requests
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine, engine
from dotenv import load_dotenv
import os
import logging

# set up logging
# 4 leveles: DEBUG, INFO, WARNING, ERROR
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# DB configuration
load_dotenv()
API_KEY = os.getenv('API_KEY')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

symbols = ["MSFT", "AAPL", "GOOGL", "AMZN"]

def extract(symbols):
    all_records = []
    for symbol in symbols:
        try:
            url = f'https://api.twelvedata.com/time_series?symbol={symbol}&interval=1min&apikey={API_KEY}'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if "values" not in data:
                logger.warning(
                    f"No data returned for {symbol}. Response: {data}"
                )
                continue

            for record in data["values"]:
                record['symbol'] = symbol

            all_records.extend(data["values"])
            logger.info(f"Extracted data for {symbol}: {len(data['values'])} rows")

        except requests.exceptions.RequestException as e:
            logger.error(
                f"API request failed for {symbol}: {e}",
                exc_info=True
            )

        except Exception as e:
            logger.error(
            f"Unexpected error extracting {symbol}: {e}",
            exc_info=True
            )  

    logger.info(f"Total records extracted: {len(all_records)}")
    return all_records

# Transform the data
def transform_data(records):
    try:
        if not records:
            raise ValueError("No records received for transformation.")

        # convert to dataframe
        df = pd.DataFrame(records)

        # convert columns to correct data types
        df['datetime'] = pd.to_datetime(df['datetime'])

        # convert everything else
        df = df.astype({
            'open': 'float',
            'high': 'float',
            'low': 'float',
            'close': 'float',
            'volume': 'int'
        })

        logger.info(f"transformed done: {len(df)} rows")
        return df
    
    except KeyError as e:
        logger.error(
            f"Missing required column: {e}",
            exc_info=True
        )
        raise

    except ValueError as e:
        logger.error(
            f"Data type conversion error: {e}",
            exc_info=True
        )
        raise

    except Exception as e:
        logger.error(
            f"Unexpected transformation error: {e}",
            exc_info=True
        )
        raise



# Loading
def load(df):
    try:
        db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

        # create sqlalchemy engine
        engine = create_engine(db_url)

        # load dataframe to sql table
        df.to_sql('stock_prices_data', engine, if_exists='append', index=False)

        logger.info(f"data loaded to database: {len(df)} rows")

    except Exception as e:
        logger.error(
            f"Database load failed: {e}",
            exc_info=True
        )
        raise

def run_pipeline():
    logger.info("pipeline started")
    records = extract(symbols)
    df = transform_data(records)
    load(df)

if __name__ == "__main__":
    run_pipeline()

            