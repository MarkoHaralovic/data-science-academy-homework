import pandas
import os
import argparse
import logging
import sys
import pandas as pd
import csv
from tqdm import tqdm 

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
                    full_path = os.path.join(root, file)
                    file_paths.append(full_path)
                    #print(full_path)
    return file_paths
def merge_data(file_paths,csv_file_path):
   max_file_size = 1000000000 # size in bytes, if csv larger than this, load it in chunks
   chunk_size=100000
   column_names = ['event_date', 'event_timestamp', 'event_name', 'user_pseudo_id', 'geo_country', 
                   'app_info_version', 'platform', 'firebase_experiments', 'id', 'item_name', 
                   'previous_first_open_count', 'name', 'event_id', 'status' 
                   ]
   columns_to_select = ["event_date", "event_name", "user_pseudo_id", "platform", "status", "geo_country", "id"]
   processed_data = pd.DataFrame(columns=columns_to_select)
   for path in tqdm(file_paths,desc="Processing csvs"):
      if os.stat(path).st_size < max_file_size:
         csv_data = pd.read_csv(path,
                             header=None,
                             names=column_names,
                             usecols=columns_to_select,
                             on_bad_lines='skip',
                             quoting = csv.QUOTE_NONE,#https://stackoverflow.com/questions/18016037/pandas-parsererror-eof-character-when-reading-multiple-csv-files-to-hdf5 
                             encoding='utf-8',
                             low_memory=False)
         processed_data = pd.concat([processed_data,csv_data],ignore_index=True)
      else:
         try:
            df = pd.read_csv(path,
                             names=column_names,
                             usecols=columns_to_select,
                             chunksize=chunk_size,
                             on_bad_lines='skip',
                             quoting=csv.QUOTE_NONE, #https://stackoverflow.com/questions/18016037/pandas-parsererror-eof-character-when-reading-multiple-csv-files-to-hdf5 
                             encoding='utf-8')
            for chunk in df:
               processed_data.append(chunk)
         except pd.errors.ParserError as e:
            logging.exception(f'Error reading CSV file on path : {path}')
         
   processed_data.to_csv(csv_file_path)
def main():
   csv_file_path,data_folder = _parse_arguments()
   file_paths = search_dir(directory_path=data_folder)
   merge_data(file_paths,csv_file_path)
   return 

if __name__ == "__main__":
   main()
   # EXAMPLE USAGE : python task_4.py --csv_save_path './raw_data/processed_data.csv' --data_folder './raw_data'
