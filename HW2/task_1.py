import paramiko
from zipfile import ZipFile, BadZipFile
import os
import logging
import argparse
import time
import sys

from helper_functions import printTotals,parse_arguments,create_dir,unzip_file,ssh_download_data 

logging.basicConfig(level=logging.INFO)  

def main():
   #get paths from cmd line
   save_path, remote_file_path,keep_zip_file = parse_arguments()
   #create directory based on provided path (where the person wish to save the data)
   create_dir(os.path.dirname(save_path))
   # open ssh session and download data
   ssh_download_data(remote_file_path, save_path)
   #unzip the downloaded file
   unzip_file(save_path,os.path.dirname(save_path),keep_zip=keep_zip_file)

if __name__ == "__main__":
   main()
   # EXAMPLE USAGE : python task_1.py --file_save_path './raw_data/january.csv.zip' --remote_file_path '/home/sofascore_academy/l2_dataset/january.csv.zip'
