import pandas
import os
import argparse
import logging
import pandas as pd
from tqdm import tqdm 
import re
import numpy as np
import time
import sys

from helper_functions  import search_dir, camel_case
 
def get_camel_column_names():
   """" Returns a list of column names in camel case +  list of column names to select from csv files"""
   #these column names have been taken from the database documentation
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
   parser.add_argument('--csv_save_path',required=True, help='Local path where csv file will be saved')
   parser.add_argument('--data_folder', required= True, help='Path to directory in which we search for csvs')
   args = parser.parse_args()
   return args.csv_save_path, args.data_folder


def merge_data(file_paths,csv_file_path,max_file_size = 1000000000,chunk_size=100000, max_tries=3,keep_existing=False):
   """
   Argument desc:
        file_paths: paths to the CSV files to be merged.
        csv_file_path: path where the mergd CSV file will be saved.
        max_file_size: max size in bytes of a CSV file to be read in one try,where files larger than this wll be read in chunks.
        chunk_size : number of rows to be read at once while reading large files in chunks. 
        max_tries : number of attempts to try reading a file.
        keep_existing: If True, the function checks if a CSV file already exists at `csv_file_path`. 
                                        If such a file exists, i raise a wrning and quit the program. 
                                        If False, an existing file at the path is overwritten. Defaults to False.
    """
   
   camel_case_columns, camel_case_columns_to_select = get_camel_column_names()
   
   
   processed_data = pd.DataFrame(columns=camel_case_columns)
   
   if os.path.isfile(csv_file_path):
      if not keep_existing:
         logging.warning("File already exists, and will be overwritten.")
      else:
         logging.error("File already exists, exiting.")
         sys.exit(1)
      
      
   processed_data.to_csv(csv_file_path,index=False)
   
   # chunks = []
   for path in tqdm(file_paths,desc="Processing csvs"):
      logging.info(f" Checked {path}")
      tries = 0
      while tries < max_tries:
         try:
            if os.stat(path).st_size < max_file_size:
               csv_data = pd.read_csv(path,
                                 header=None,
                                 names=camel_case_columns,
                                 usecols=camel_case_columns_to_select,
                                 on_bad_lines='skip',
                                 #quoting = csv.QUOTE_NONE,#https://stackoverflow.com/questions/18016037/pandas-parsererror-eof-character-when-reading-multiple-csv-files-to-hdf5 
                                 encoding='utf-8',
                                 low_memory=False)
                                 #added low meemory since i got warning for column types. I didn't send dtypes here due to inability to read id as int + eventDate would be string later turned to datetime
                                 #on link https://towardsdatascience.com/%EF%B8%8F-load-the-same-csv-file-10x-times-faster-and-with-10x-less-memory-%EF%B8%8F-e93b485086c7
                                 # there was suggestion :"by defining correct dtypes we can reduce memory usage significantly."
                                 # I did and still got memory error described below.If converted after reading, script time prolongs.

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
                                 encoding='utf-8',
                                 low_memory=False)
               for chunk in df:
                  chunk.drop_duplicates(keep='first',inplace=True)
                  chunk['eventDate'] = pd.to_datetime(chunk['eventDate'], format='%Y%m%d', errors='coerce')
                  chunk = chunk.loc[chunk['eventDate'] <= '2023-03-15']
                  chunk.to_csv(csv_file_path,mode='a',index=False,header=False)
                  # chunks.append(chunk)
            break
         except pd.errors.ParserError as e:
            logging.exception(f'Error reading CSV file on path : {path}')
            tries+=1
            time.sleep(5)
            if tries == max_tries:
                    logging.error(f"Reached maximum tries ({max_tries}) for file: {path}")
                    
   #APPROACH COMMENTARY:  option to append each csv file and csv chunk was dropped  due to memory issue (my working memory couldn't handle the size of array crated by appending chunks)
   # got error : cant allocate 5.2GB of memory for array of shape [shape]
   # this method of appending chunks and individual csv to csv file is slower, but works without errors
   # processed_data = pd.concat(chunks, ignore_index=True)
   # processed_data.to_csv(csv_file_path,index=False)
   
def main():
   #get paths from cmd line
   csv_file_path,data_folder = _parse_arguments()
   #search for csv files in a specified folder
   file_paths = search_dir(directory_path=data_folder, extension=".csv")
   merge_data(file_paths,csv_file_path,keep_existing=True)
   return 

if __name__ == "__main__":
   main()
   # EXAMPLE USAGE : python task_4.py --csv_save_path './raw_data/processed_data.csv' --data_folder './raw_data'

