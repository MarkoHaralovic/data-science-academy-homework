import paramiko
from zipfile import ZipFile, BadZipFile
import tarfile
import os
import logging
import argparse
import time
import sys

def printTotals(transferred, toBeTransferred):
    print(f"Transferred: {transferred}\tOut of: {toBeTransferred}")

def parse_arguments():
   parser = argparse.ArgumentParser()
   parser.add_argument('--file_save_path',required=True, help='Putanja gdje se file lokalno sprema')
   parser.add_argument('--remote_file_path', required= True, help='Putanja do remote datoteke koja se skida')
   args = parser.parse_args()
   return args.file_save_path, args.remote_file_path

def create_dir(file_save_dir):
   if not os.path.exists(file_save_dir):
      os.makedirs(file_save_dir)
      
def open_tar_file(local_file_path, save_path):
    if tarfile.is_tarfile(local_file_path):
        try:
            with tarfile.open(local_file_path, 'r:gz') as tar:
                tar.extractall(path=save_path)
        except Exception as e: 
            logging.error(f"There was a problem extracting tar file from location {local_file_path}: {e}")
    else:
        logging.error(f"{local_file_path} is not a valid tar file.")

      
def ssh_download_data(remote_file_path, local_file_path, max_tries=3):
   tries = 0
   while tries < max_tries:
      try:
         with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect('130.61.238.15', username='sofascore_academy', password='H69qByfVGwkLgezF')
            with ssh.open_sftp() as sftp:
               sftp.get(remote_file_path, local_file_path,callback = printTotals)
            return
      except FileNotFoundError as e:
         logging.error(f"File does not exist  on remote  path : {remote_file_path}, try {tries +1}")
         tries+=1
         time.sleep(5)
      except SSHException as e:
         logging.error(f"Error connecting to host,try {tries +1}")
         tries+=1
         time.sleep(5)
   logging.error(f"Reached maximum tries ({max_tries})")
   sys.exit(1)
      
def main():
   logging.basicConfig(level=logging.INFO)
   #get paths from cmd line
   save_path, remote_file_path = parse_arguments()
   # #create directory based on provided path (where the person wish to save the data)
   # create_dir(os.path.dirname(save_path))
   # # open ssh session and download data
   # ssh_download_data(remote_file_path, save_path)
   # #extract data from tarfile
   print(save_path)
   print(os.path.dirname(save_path))
   open_tar_file(save_path,os.path.dirname(save_path))

if __name__ == "__main__":
   main()
   # EXAMPLE USAGE : python task_1.py --file_save_path './raw_data/february.tar.gz' --remote_file_path '/home/sofascore_academy/l2_dataset/february.tar.gz'