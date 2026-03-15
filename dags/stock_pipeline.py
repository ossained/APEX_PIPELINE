#Apex wealth data pipeline
# ETL script for extracting stock prices from Twelve data API,transforms,loads to postgres database


# import libraries
import requests
import pandas as pd 
from dotenv import load_dotenv
import os
import psycopg2
from sqlalchemy import create_engine
import logging


# set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# configurations
load_dotenv()
API_KEY = os.getenv('API_KEY')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT')

symbols =['AAPL','MSFT','GOOGL','AMZN']


# step 1 -Extract
def extract(Symbols:list) -> list:
    
    all_records = []
    
    for symbol in symbols:
        url = f'https://api.twelvedata.com/time_series?symbol={symbol}&interval=1min&apikey={API_KEY}&outputsize=4'
     
        try:
            response = requests.get(url,timeout=10)
            response.raise_for_status() # raise an error for bad responses
            data = response.json()
            
            #check if the api response is ok
            if data['status'] != 'ok':
                raise ValueError(f"error with {symbol}: {data.get('message','unkwnown error')}")
            
            
            for record in data["values"]:
                record['symbol'] = symbol
                
            all_records.extend(data["values"])
            logger.info(f"Extraction done for {symbol}:extracted {len(data["values"])} rows")
            
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Extract failed for {symbol}: request failed - {e}")
            raise
            
    logger.info(f"Extract done for {symbol}:extracted {len(all_records)} rows")
    return all_records


#step 2 - Transform
# tranfer data to a dataframe
def transform(records:list) -> pd.DataFrame:
    try:
    
        #convert to dataframe
        df = pd.DataFrame(records)
        
        # convert columns to appropriate data type
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        df = df.astype({
            'open':'float',
            'high':'float',
            'low':'float',
            'close':'float',
            'volume':'int'
        })
        
        logger.info(f"Transform done - {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Transform failed - {e}")
        raise
    
    
    
# Step 3 - Load

def load(df:pd.DataFrame) -> None:
    
    
    try:
       #create connection string with url
        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)

        # load data to data base 
        df.to_sql('stockprices_data',engine,if_exists='append',index=False)

        logger.info(f'Data loaded - {len(df)}rows loaded into the database.')
    except Exception as e:
        logger.error(f"loading failed -{e}")
        raise
    
    
    
    
def run_pipeline():
    logger.info("pipeline started")
    
    records = extract(symbols)
    df = transform(records)
    load(df)
    
    
if __name__ == "__main__":
    run_pipeline()
     

    
         
         
     