# Apex wealth data pipeline
# ETL script for extracting stock prices from Twelve Data API,
# transforming them, and loading into a PostgreSQL database.

import requests
import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
from sqlalchemy import create_engine, text
import logging

# -----------------------------
# Logging configuration
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()
API_KEY = os.getenv('API_KEY')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT')

symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']


# -----------------------------
# Step 1 — Extract
# -----------------------------
def extract(symbols: list) -> list:
    all_records = []

    for symbol in symbols:
        url = f'https://api.twelvedata.com/time_series?symbol={symbol}&interval=1min&apikey={API_KEY}&outputsize=4'

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data['status'] != 'ok':
                raise ValueError(f"Error with {symbol}: {data.get('message', 'unknown error')}")

            for record in data["values"]:
                record['symbol'] = symbol

            all_records.extend(data["values"])
            logger.info(f"Extraction done for {symbol}: extracted {len(data['values'])} rows")

        except requests.exceptions.RequestException as e:
            logger.error(f"Extract failed for {symbol}: request failed - {e}")
            raise

    logger.info(f"Extract done: extracted {len(all_records)} rows total")
    return all_records


# -----------------------------
# Step 2 — Transform
# -----------------------------
def transform(records: list) -> pd.DataFrame:
    try:
        df = pd.DataFrame(records)

        df['datetime'] = pd.to_datetime(df['datetime'])

        df = df.astype({
            'open': 'float',
            'high': 'float',
            'low': 'float',
            'close': 'float',
            'volume': 'int'
        })

        logger.info(f"Transform done - {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"Transform failed - {e}")
        raise


# -----------------------------
# Step 3 — Load
# -----------------------------
def load(df: pd.DataFrame) -> None:
    try:
        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)

        df.to_sql('stockprices_data', engine, if_exists='append', index=False)

        logger.info(f"Data loaded - {len(df)} rows loaded into the database.")

    except Exception as e:
        logger.error(f"Loading failed - {e}")
        raise


# -----------------------------
# Step 4 — Validation
# -----------------------------
def validate(expected_count: int):
    logger.info("Validating database load...")

    try:
        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)

        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM stockprices_data"))
            actual_count = result.scalar()

        logger.info(f"Database now contains {actual_count} rows")

        if actual_count < expected_count:
            raise ValueError(
                f"Validation failed: expected at least {expected_count} new rows, "
                f"but database has only {actual_count}."
            )

        logger.info("Database load validation passed.")

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise


# -----------------------------
# ETL-only runner
# -----------------------------
def run_etl():
    logger.info("ETL started")

    records = extract(symbols)
    df = transform(records)
    load(df)

    logger.info("ETL completed successfully")
    return df  # return df so validation can use it


# -----------------------------
# Validation-only runner
# -----------------------------
def run_validation():
    # You can choose what expected_count means:
    # Option 1: Validate at least 1 row exists
    # Option 2: Validate based on last ETL run (recommended)
    logger.info("Running validation only...")

    # Example: validate at least 1 row exists
    validate(1)


# -----------------------------
# Full pipeline runner
# -----------------------------
def run_pipeline():
    df = run_etl()
    validate(len(df))
    logger.info("Pipeline completed successfully")


# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    run_pipeline()   # Change to run_etl() or run_validation() if needed
