import paramiko
from zipfile import ZipFile, BadZipFile
import os
import logging
import argparse
import time
import sys

from helper_functions import create_dir,parse_arguments,unzip_file,ssh_download_data,ssh_download_data_from_dir

logging.basicConfig(level=logging.INFO)  
         
def main():
   #get paths from cmd line
   save_path, remote_file_path,keep_zip_file = parse_arguments()
   #create dir where data will be saved
   create_dir(save_path)
    # open ssh session and download data
   ssh_download_data_from_dir(remote_file_path,save_path,keep_zip=keep_zip_file)
   
if __name__ == "__main__":
   main()
   # EXAMPLE USAGE : python task_3.py --file_save_path './raw_data/march' --remote_file_path '/home/sofascore_academy/l2_dataset/march'
