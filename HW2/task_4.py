import pandas
import os
import argparse
import logging
import pandas as pd
from tqdm import tqdm 
import re
import numpy as np
import time

def camel_case(s):
    #taken from https://www.w3resource.com/python-exercises/string/python-data-type-string-exercise-96.php
    s = re.sub(r"(_|-)+", " ", s).title().replace(" ", "")
    return ''.join([s[0].lower(), s[1:]])
 
def get_camel_column_names():
   """" Returns a list of column names in camel case +  list of column names to select from csv files"""
   column_names = ['event_date', 'event_timestamp', 'event_name', 'user_pseudo_id', 'geo_country', 
                   'app_info_version', 'platform', 'firebase_experiments', 'id', 'item_name', 
                   'previous_first_open_count', 'name', 'event_id', 'status' 
                   ]
   camel_case_columns = [camel_case(column) for column in column_names]
   columns_to_select = ["event_date", "event_name", "user_pseudo_id", "platform", "status", "geo_country", "id"]
   camel_case_columns_to_select = [camel_case(column) for column in columns_to_select]
   
   return camel_case_columns, camel_case_columns_to_select

def _parse_arguments():
   parser = argparse.ArgumentParser()
   parser.add_argument('--csv_save_path',required=True, help='Putanja gdje se file lokalno sprema')
   parser.add_argument('--data_folder', required= True, help='Putanja do mape u kojoj se pregledavaju csv zapisi')
   args = parser.parse_args()
   return args.csv_save_path, args.data_folder

def search_dir(directory_path):
    file_paths = []
    if os.path.exists(directory_path):
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.csv'):
                    full_path = os.path.join(root, file).replace('\\', '/')
                    file_paths.append(full_path)
    return file_paths
 
def merge_data(file_paths,csv_file_path,max_file_size = 1000000000,chunk_size=100000, max_tries=3):
   
   camel_case_columns, camel_case_columns_to_select = get_camel_column_names()
   
   processed_data = pd.DataFrame(columns=camel_case_columns)
   processed_data.to_csv(csv_file_path,index=False)
   
   # chunks = []
   for path in tqdm(file_paths,desc="Processing csvs"):
      tries = 0
      while tries < max_tries:
         try:
            if os.stat(path).st_size < max_file_size:
               logging.info(f" Checked {path}")
               csv_data = pd.read_csv(path,
                                 header=None,
                                 names=camel_case_columns,
                                 usecols=camel_case_columns_to_select,
                                 on_bad_lines='skip',
                                 #quoting = csv.QUOTE_NONE,#https://stackoverflow.com/questions/18016037/pandas-parsererror-eof-character-when-reading-multiple-csv-files-to-hdf5 
                                 encoding='utf-8',
                                 low_memory=False)

               csv_data.drop_duplicates(keep='first',inplace=True)
               csv_data['eventDate'] = pd.to_datetime(csv_data['eventDate'], format='%Y%m%d', errors='coerce')
               csv_data = csv_data[csv_data['eventDate'] <= pd.Timestamp('2023-03-15')]
               csv_data.to_csv(csv_file_path,mode='a',index=False,header=False)
               # chunks.append(csv_data)
            else:
               df = pd.read_csv(path,
                                 header=None,
                                 names=camel_case_columns,
                                 usecols=camel_case_columns_to_select,
                                 chunksize=chunk_size,
                                 on_bad_lines='skip',
                                 # quoting=csv.QUOTE_NONE, #https://stackoverflow.com/questions/18016037/pandas-parsererror-eof-character-when-reading-multiple-csv-files-to-hdf5 
                                 encoding='utf-8')
               for chunk in df:
                  chunk.drop_duplicates(keep='first',inplace=True)
                  chunk['eventDate'] = pd.to_datetime(chunk['eventDate'], format='%Y%m%d', errors='coerce')
                  chunk = chunk.loc[chunk['eventDate'] <= '2023-03-15']
                  chunk.to_csv(csv_file_path,mode='a',index=False)
                  # chunks.append(chunk)
            break
         except pd.errors.ParserError as e:
            logging.exception(f'Error reading CSV file on path : {path}')
            tries+=1
            time.sleep(5)
            if tries == max_tries:
                    logging.error(f"Reached maximum tries ({max_tries}) for file: {path}")
                    
   #comment :  option to append each csv file and csv chunk was due to memory issue (my working memory coudnt handle the size of array crated by appending chunks)
   # processed_data = pd.concat(chunks, ignore_index=True)
   # processed_data.to_csv(csv_file_path,index=False)
   
def main():
   csv_file_path,data_folder = _parse_arguments()
   file_paths = search_dir(directory_path=data_folder)
   merge_data(file_paths,csv_file_path)
   return 

if __name__ == "__main__":
   main()
   # EXAMPLE USAGE : python task_4.py --csv_save_path './raw_data/processed_data.csv' --data_folder './raw_data'

