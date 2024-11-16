import os
source_path = "/home/ubuntu/WRF_SOLAR/WRF_OUT/"
source_ip = "34.170.124.51"
home_path = "/home/vicktor/vaib/wrf_transfer"

source_key = os.path.join(home_path,'chabis','allkey.pem')
destination_path = os.path.join(home_path,'WRF_OUT')

date_folder_format = '%Y-%m-%d'
timestamp_format = "%Y-%m-%d %H:%M:%S"


data_configs_map = {'ENGINE': 'django.db.backends.postgresql',
            'database': 'postgres',
            'user': 'admin123',
            'password': 'tensor123',
            'host': '35.184.116.149',
            'port': '5432',}

data_send = {'database': 'postgres',
            'user': 'weather_data_user',
            'password': 'bronzed1234',
            'host': '34.172.251.28',
            'port': '5432'}


