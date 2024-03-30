import paramiko
from zipfile import ZipFile, BadZipFile
import tarfile
import os
import logging
import argparse
import time
import sys
import re

def printTotals(transferred, toBeTransferred):
    print(f"Transferred: {transferred}\tOut of: {toBeTransferred}")

def parse_arguments():
   parser = argparse.ArgumentParser()
   parser.add_argument('--file_save_path',required=True, help='Putanja gdje se file lokalno sprema')
   parser.add_argument('--remote_file_path', required= True, help='Putanja do remote datoteke koja se skida')
   args = parser.parse_args()
   return args.file_save_path, args.remote_file_path

def search_dir(directory_path,extension):
    file_paths = []
    if os.path.exists(directory_path):
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith(extension):
                    full_path = os.path.join(root, file).replace('\\', '/')
                    file_paths.append(full_path)
    return file_paths

def create_dir(dir):
   if not os.path.exists(dir):
      os.makedirs(dir)
      
def unzip_file(local_file_path,save_path):
   try:
      with ZipFile(local_file_path,'r') as zip_file:
         zip_file.extractall(path=save_path)  
   except BadZipFile as e:
      logging.error(f"Error unzipping file on path {local_file_path}")

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
      
def ssh_download_data_from_dir(remote_folder,local_folder,max_tries=3):
   tries = 0
   while tries < max_tries:
      try:
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
            
def camel_case(s):
    #taken from https://www.w3resource.com/python-exercises/string/python-data-type-string-exercise-96.php
    s = re.sub(r"(_|-)+", " ", s).title().replace(" ", "")
    return ''.join([s[0].lower(), s[1:]])