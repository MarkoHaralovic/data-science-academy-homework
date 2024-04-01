import logging
from helper_functions import create_dir,parse_arguments,ssh_download_data_from_dir,load_config_from_file
  
def main():
   args = parse_arguments()

   if args.config_file:
      config = load_config_from_file(args.config_file,args.task)
      save_path = config['file_save_path']
      remote_file_path = config['remote_file_path']
      keep_zip_file = config['keep_zip_tar_file']
      max_tries = config['max_tries']
      retry_delay = config['retry_delay']
      log_level = config['log-level']
      overwrite = config['overwrite']
   else:
      save_path = args.file_save_path
      remote_file_path = args.remote_file_path
      keep_zip_file = args.keep_zip_tar_file
      max_tries = args.max_tries
      retry_delay = args.retry_delay
      log_level = args.log_level
      overwrite = args.overwrite
   
   logging.basicConfig(level=getattr(logging,log_level))
   
   #create dir where data will be saved
   create_dir(save_path,overwrite=overwrite)
    # open ssh session and download data
   ssh_download_data_from_dir(remote_file_path,save_path,keep_zip=keep_zip_file,max_tries=max_tries,retry_delay=retry_delay)
   
if __name__ == "__main__":
   main()
   # EXAMPLE USAGE : python task_3.py --file_save_path './raw_data/march' --remote_file_path '/home/sofascore_academy/l2_dataset/march'
