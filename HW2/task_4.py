import pandas
import os
import argparse
import logging
import sys
import pandas as pd

def parse_arguments():
   parser = argparse.ArgumentParser()
   parser.add_argument('--file_save_path',required=True, help='Putanja gdje se file lokalno sprema')
   parser.add_argument('--data_folder', required= True, help='Putanja do mape u kojoj se pregledavaju csv zapisi')
   args = parser.parse_args()
   return args.file_save_path, args.remote_file_path

def search_dir(directory_path):
    file_paths = []
    if os.path.exists(directory_path):
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.csv'):
                    full_path = os.path.join(root, file)
                    file_paths.append(full_path)
                    print(full_path)
    return file_paths
def merge_data(file_paths):
   column_names = ['event_date', 'event_timestamp', 'event_name', 'user_pseudo_id', 'geo_country', 
                   'app_info_version', 'platform', 'firebase_experiments', 'id', 'item_name', 
                   'previous_first_open_count', 'name', 'event_id', 'status' 
                   ]
   columns_to_select = ["event_date", "event_name", "user_pseudo_id", "platform", "status", "geo_country", "id"]
   processed_data = 1
   for path in file_paths:
      csv_file = pd.read_csv(path,header=None,names=column_names,usecols=columns_to_select)
      
   
def main():
   file_paths = search_dir(directory_path="C:\\SofascoreAcademy2024\\data-science-academy-homework\\HW2\\raw_data")
   merge_data(file_paths)
   return 

if __name__ == "__main__":
   main()
   # EXAMPLE USAGE : python task_4.py --file_save_path './raw_data/processed_data.csv' --data_folder './raw_data'
