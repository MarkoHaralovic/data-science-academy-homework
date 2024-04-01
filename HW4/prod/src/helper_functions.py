import paramiko
from paramiko.ssh_exception import BadHostKeyException, AuthenticationException, SSHException
from zipfile import ZipFile, BadZipFile
import tarfile
import os
import logging
import argparse
import time
import sys
import re
from dotenv import load_dotenv
import json

load_dotenv()
REMOTE_HOST_ADDRESS = os.getenv('REMOTE_HOST_ADDRESS')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

def printTotals(transferred, toBeTransferred):
    print(f"Transferred: {transferred}\tOut of: {toBeTransferred}")

def load_config_from_file(file_path, task_name):
    with open(file_path, 'r') as file:
        config = json.load(file)
    merged_config = {**config.get("common", {}), **config.get(task_name, {})}
    return merged_config

def parse_arguments():
   basic_parser = argparse.ArgumentParser(add_help=False)
   basic_parser.add_argument('--config_file', help='Path to the configuration file', default=None)
   basic_args, _ = basic_parser.parse_known_args()

   parser = argparse.ArgumentParser()
   
   if basic_args.config_file:
        required = False
   else:
        required = True
  
   required_arguments = parser.add_argument_group('Required arguments')
   required_arguments .add_argument('--file_save_path',required=required, help='Local path to save file to')
   required_arguments .add_argument('--remote_file_path', required=required, help='Path to remote file to save.')
   
   parser._optionals.title = 'Optional arguments'
   parser.add_argument('--config_file', help='Path to the configuration file', default=None)
   parser.add_argument('--task', help='Task to execute (task_1, task_2, task_3)')
   parser.add_argument('--keep_zip_tar_file', type=bool, choices=[True, False], default=False, help='Store/keep the zip/tar file downloaded from remote host')
   parser.add_argument('--max_tries', type=int, default=3,help='Maximum number of tries to download the data')
   parser.add_argument('--retry_delay', type=int,default=5,help='Sleep time between retries')
   parser.add_argument('--log-level', default='ERROR', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set the logging level')
   parser.add_argument('--overwrite',type=bool,choices=[True,False],default=False,help='When creating dir, overwrite existing dir if on the location where user wishes to crate dir.')
   
   args = parser.parse_args()
   return args

def search_dir(directory_path,extension):
    file_paths = []
    if os.path.exists(directory_path):
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith(extension):
                    full_path = os.path.join(root, file).replace('\\', '/')
                    file_paths.append(full_path)
    return file_paths

def create_dir(dir,overwrite=True):
   if not os.path.exists(dir):
      os.makedirs(dir)
   elif os.path.exists(dir) and not overwrite:
      logging.error('Directory already exists: %s, define different directory to save data to' % dir)
      sys.exit(1)
   else:
      logging.info('Overwriting directory %s' % dir)
           
      
def unzip_file(local_zip_file_path,save_path,keep_zip=True):
   try:
      logging.info('Extracting data to %s' % save_path)
      with ZipFile(local_zip_file_path,'r') as zip_file:
         zip_file.extractall(path=save_path)  
      os.remove(local_zip_file_path) if not keep_zip else None
   except BadZipFile as e:
      logging.error(f"Error unzipping file on path {local_zip_file_path}")

def open_tar_file(local_file_path, save_path,keep_tar_file=True):
    if tarfile.is_tarfile(local_file_path):
        try:
            with tarfile.open(local_file_path, 'r:gz') as tar:
                tar.extractall(path=save_path)
            os.remove(local_file_path) if not keep_tar_file else None
        except Exception as e: 
            logging.error(f"There was a problem extracting tar file from location {local_file_path}: {e}")
    else:
        logging.error(f"{local_file_path} is not a valid tar file.")
      
def ssh_download_data(remote_file_path, local_file_path, max_tries,retry_delay):
   tries = 0
   while tries < max_tries:
      try:
         with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(REMOTE_HOST_ADDRESS, username=USERNAME, password=PASSWORD)
            with ssh.open_sftp() as sftp:
               sftp.get(remote_file_path, local_file_path,callback = printTotals)
            return
      except FileNotFoundError as e:
         logging.error(f"File does not exist  on remote  path : {remote_file_path}, try {tries +1}")
         tries+=1
         time.sleep(retry_delay)
      except BadHostKeyException as e:
         logging.error(f"Bad host key: {e}")
         break
      except AuthenticationException as e:
         logging.error(f"Authentication failed: {e}")
         break
      except SSHException as e:
         logging.error(f"Error connecting to host,try {tries +1}")
         tries+=1
         time.sleep(retry_delay)
   logging.error(f"Reached maximum tries ({max_tries})")
   sys.exit(1)
      
def ssh_download_data_from_dir(remote_folder,local_folder,keep_zip,max_tries,retry_delay):
   tries = 0
   while tries < max_tries:
      try:
         with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(REMOTE_HOST_ADDRESS, username=USERNAME, password=PASSWORD)
            command = f'ls -l {remote_folder}'
            _, stdout, _ = ssh.exec_command(command)
            for line in stdout:
               file = line.strip()
               file = file.split(" ")[-1]
                              
               if file.endswith('.zip'):
                  remote_file_path = os.path.join(remote_folder, file).replace('\\', '/')
                  local_file_path = os.path.join(local_folder, file)
                  ssh_download_data(remote_file_path, local_file_path, max_tries=max_tries,retry_delay=retry_delay)
                  unzip_file(local_file_path, local_folder,keep_zip=keep_zip)
               elif file.endswith('.csv'):
                  logging.info(file)
                  remote_file_path = os.path.join(remote_folder, file).replace('\\', '/')
                  local_file_path = os.path.join(local_folder, file)
                  ssh_download_data(remote_file_path, local_file_path,max_tries=max_tries,retry_delay=retry_delay)                  
         return
      except FileNotFoundError as e:
         logging.error(f"File does not exist  on remote  path : {remote_file_path}, try {tries +1}")
         tries+=1
         time.sleep(retry_delay)
      except BadHostKeyException as e:
         logging.error(f"Bad host key: {e}")
         break
      except AuthenticationException as e:
         logging.error(f"Authentication failed: {e}")
         break
      except SSHException as e:
         logging.error(f"Error connecting to host,try {tries +1}")
         tries+=1
         time.sleep(retry_delay)
   logging.error(f"Reached maximum tries ({max_tries})")
   sys.exit(1)
            
def camel_case(s):
    #taken from https://www.w3resource.com/python-exercises/string/python-data-type-string-exercise-96.php
    s = re.sub(r"(_|-)+", " ", s).title().replace(" ", "")
    return ''.join([s[0].lower(), s[1:]])