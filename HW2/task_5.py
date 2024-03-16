import pandas as pd
from clickhouse_driver import Client
import argparse
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)  

QUERY = """CREATE TABLE IF NOT EXISTS mharalovic.l2_dataset
         (
         eventDate String,
         eventName String,
         userPseudoId String,
         platform LowCardinality(String),
         status LowCardinality(String),
         geoCountry LowCardinality(String),
         id Nullable(Int64)
         )
         ENGINE = MergeTree()
         ORDER BY eventDate
         SETTINGS index_granularity = 8192;
         """

def _parse_arguments():
   
   parser = argparse.ArgumentParser()
   parser.add_argument('--csv_file_path',required=True, help='Local path which will be uploaded')
   parser.add_argument('--clickhouse_database',required=True, help='Clickhouse database in which csv will be inserted')
   parser.add_argument('--host', required= True, help='Remote host')
   parser.add_argument('--port', required= True, help='Remote port')
   parser.add_argument('--username', required= True, help='Username to connect to remote server')
   parser.add_argument('--password', required= True, help='Password to connect to remote server')
   
   args = parser.parse_args()
   return args.csv_file_path,args.clickhouse_database, args.host, args.port, args.username, args.password

def insert_df_chunks(csv_file_path,client,chunk_size=100000):
   df = pd.read_csv(csv_file_path,                               
                    chunksize=chunk_size,
                    on_bad_lines='skip',
                    encoding='utf-8',
                    low_memory=False
                    )
   
   for chunk in tqdm(df, desc="Inserting chunks"):
      _ = client.insert_dataframe('INSERT INTO mharalovic.l2_dataset(eventDate,eventName,userPseudoId,platform, status,geoCountry,id) VALUES', 
                                           chunk, 
                                           settings=dict(use_numpy=True)
                                           )
      
def upload_csv_to_clickhouse(csv_file_path,clickhouse_database,host, port, username, password, max_tries = 3):
   tries = 0
   logging.info(f"Connecting to host: {host}, port: {port}, database: {clickhouse_database}")
   while tries < max_tries:
    try:
        client = Client(host=host,
                        port=port,
                        user=username,
                        password=password,
                        database=clickhouse_database)
        break  
    except ConnectionError as e: 
        attempts += 1
        logging.error(f"There was an error connecting to host: {host}, port: {port}, database: {clickhouse_database} with your username and password. Attempt {tries}/{max_tries}. Error: {e}")
        if tries==max_tries:
            logging.error("Maximum connection attempts reached. Exiting.")
            break
   
   client.execute(QUERY)
   
   try:
      insert_df_chunks(csv_file_path,client)
   except Exception as e:
      logging.error("There was an error inserting data chunks into database")
   
def main():
   csv_file_path,clickhouse_database,host, port, username, password = _parse_arguments()
   upload_csv_to_clickhouse(csv_file_path,clickhouse_database,host, port, username, password)
   
   
if __name__ == '__main__':
   main()
   
   #EXAMPLE USAGE: 
   # python task_5.py --csv_file_path './raw_data/processed_data.csv --clickhouse_database mharalovic --host clickhouse.sofascore.ai --port 9000 --username nsurname --password yourPassword