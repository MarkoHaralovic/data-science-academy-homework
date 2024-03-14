import paramiko
from zipfile import ZipFile, BadZipFile
import os
import logging
import argparse
import time
import sys

from helper_functions import create_dir,parse_arguments,unzip_file,ssh_download_data 

def ssh_download_data_from_dir(remote_folder,local_folder):
   with paramiko.SSHClient() as ssh:
      ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
      ssh.connect('130.61.238.15', username='sofascore_academy', password='H69qByfVGwkLgezF')
      command = f'ls -l {remote_folder}'
      _, stdout, _ = ssh.exec_command(command)
      for line in stdout:
         print(line)
         file = line.strip()
         file = file.split(" ")[-1]
         print(file)
         if file.endswith('.zip'):
            remote_file_path = os.path.join(remote_folder, file).replace('\\', '/')
            local_file_path = os.path.join(local_folder, file)
            ssh_download_data(remote_file_path, local_file_path, max_tries=3)
            unzip_file(local_file_path, local_folder)
         
def main():
   logging.basicConfig(level=logging.INFO)
   #get paths from cmd line
   save_path, remote_file_path = parse_arguments()
   #create dir where data will be saved
   create_dir(save_path)
    # open ssh session and download data
   ssh_download_data_from_dir(remote_file_path,save_path)
   
if __name__ == "__main__":
   main()
   # EXAMPLE USAGE : python task_3.py --file_save_path './raw_data/march' --remote_file_path '/home/sofascore_academy/l2_dataset/march'
