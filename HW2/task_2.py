import paramiko
from zipfile import ZipFile, BadZipFile
import tarfile
import os
import logging
import argparse
import time
import sys

from helper_functions import printTotals,parse_arguments,create_dir,open_tar_file,ssh_download_data 
      
def main():
   logging.basicConfig(level=logging.INFO)
   #get paths from cmd line
   save_path, remote_file_path = parse_arguments()
   #create directory based on provided path (where the person wish to save the data)
   create_dir(os.path.dirname(save_path))
   # open ssh session and download data
   ssh_download_data(remote_file_path, save_path)
   #extract data from tarfile
   print(save_path)
   print(os.path.dirname(save_path))
   open_tar_file(save_path,os.path.dirname(save_path))

if __name__ == "__main__":
   main()
   # EXAMPLE USAGE : python task_1.py --file_save_path './raw_data/february.tar.gz' --remote_file_path '/home/sofascore_academy/l2_dataset/february.tar.gz'
