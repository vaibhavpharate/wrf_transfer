import pandas as pd
import numpy as np
import os
import paramiko
from datetime import datetime, timedelta
import shutil

from paths import *
from db_functions import get_connection, get_wrf_transferred
import re

file_logs_schema = 'files_map_logs'




## check for latest date
## get the ssh connection
def get_ssh():
    ssh = paramiko.SSHClient() ## Create the SSH object
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) # no known_hosts error
    try:
        ssh.connect(source_ip, username='ubuntu', key_filename=source_key)
    except Exception as e:
        print("There was an error")
        print(e)
    else:
        print("Connected Securely to the Source Server")
    return ssh

## choose the latest date folder available in source 
def choose_latest_date(ssh_client,source_path=source_path,folder_format=date_folder_format):
    stdin, stdout, stderr = ssh_client.exec_command(f'ls {source_path}')
    date_folder_list = stdout.readlines()
    date_folder_list = [str(x)[:-1] for x in date_folder_list]
    # date_folder_list.remove("EXIM")
    date_folder_dates  = [datetime.strptime(x,folder_format) for x in date_folder_list]
    date_folder_dates.sort()
    latest_date = date_folder_dates[-1]

    choosing_latest =latest_date.strftime(folder_format)
    return choosing_latest



def get_wrf_file(ssh_client,latest_date,source_path=source_path):
    stdin, stdout, stderr = ssh_client.exec_command(f'ls {source_path}/{latest_date}')
    date_files = stdout.readlines()
    
    date_files = list(map(lambda x: str(x)[:-1] , date_files))
    pattern = r"^\d{4}-\d{2}-\d{2}\.nc$"
    valid_files = [filename for filename in date_files if re.match(pattern, filename)]
    return valid_files[0]




def transfer_wrf_file(ssh_client,latest_date,wrf_file_name,db_connection):
    try:
        sftp_client = ssh_client.open_sftp()
    except Exception as e:
        print("CANNOT GET SSH CONNECTION")
        ssh_client = get_ssh()
        sftp_client = ssh_client.open_sftp()
    if os.path.exists(os.path.join(destination_path,latest_date))==False:
        os.makedirs(os.path.join(destination_path,latest_date))
        print(f"CREATED DATE Folder {latest_date}")
        
    file_path = os.path.join(destination_path,latest_date,wrf_file_name)
    source_file_path = os.path.join(source_path,latest_date,wrf_file_name)
    if os.path.exists(file_path) == False:
        sftp_client.get(source_file_path,file_path)
        df_log = pd.DataFrame({    'status':['transferred'],
                                   'log_ts':[datetime.now()],'file':[wrf_file_name], ## NEED TO UPDATE HERE
                                   'read_status':[0],
                                   'source_timestamp':[latest_date]})
        df_log.to_sql(schema=file_logs_schema,
                          name='transfer_wrf_logs',
                          if_exists='append',
                          con=db_connection,
                          index=False)
        return True
    else:
        print("File Already Exists")
        return False



ssh_client = get_ssh()
db_connection = get_connection(host=data_configs_map['host'],
                               passord=data_configs_map['password'],
                               user=data_configs_map['user'],
                               database=data_configs_map['database'],
                               port=data_configs_map['port'])

latest_date = choose_latest_date(ssh_client=ssh_client)
wrf_file_name = get_wrf_file(ssh_client=ssh_client,latest_date=latest_date)

transfer_wrf_file(ssh_client=ssh_client,
                  latest_date=latest_date,
                  wrf_file_name=wrf_file_name,
                  db_connection=db_connection)
