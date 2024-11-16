import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import shutil
import xarray as xr
from sqlalchemy import text

from paths import *
from db_functions import get_connection, get_wrf_transferred


file_logs_schema = 'files_map_logs'

# today = datetime.now() # + timedelta(hours=5,minutes=30)


def get_start_and_end_of_day(input_date=None):
    """
    Returns the start and end timestamps for a given date.
    If no date is provided, the current date is used.
    
    Parameters:
        input_date (datetime, optional): The date for which to calculate start and end timestamps.
    
    Returns:
        tuple: (start_of_day, end_of_day) as datetime objects
    """
    # Use the provided date or default to today NEED TO CHANGE THIS
    if input_date is None:
        input_date = datetime.now()  # + timedelta(hours=5,minutes=30)
    
    # Start of the day
    start_of_day = datetime(input_date.year, input_date.month, input_date.day)
    
    # End of the day
    end_of_day = start_of_day + timedelta(days=1) - timedelta(microseconds=1)
    
    return start_of_day

def parse_timestamp(value):
    date_part = int(value)
    fractional_part = value - date_part
    
    # Convert to date and time
    date = datetime.strptime(str(date_part), "%Y%m%d")
    time = timedelta(days=fractional_part)
    final_timestamp = date + time
    
    minutes_to_add = (15 - (final_timestamp.minute % 15)) % 15

    # If minutes_to_add is 0 but there are seconds/microseconds, round to the next quarter hour
    if minutes_to_add == 0 and (final_timestamp.second > 0 or final_timestamp.microsecond > 0):
        minutes_to_add = 15
    
    # Add the necessary minutes and truncate seconds/microseconds
    rounded_timestamp = (final_timestamp + timedelta(minutes=minutes_to_add)).replace(second=0, microsecond=0)
    return rounded_timestamp


def read_wrf(file_name,db_connection,data_connection,today_str,yesterday_str,tomorrow_str):
    today = datetime.strptime(today_str,timestamp_format)
    tomorrow = datetime.strptime(tomorrow_str,timestamp_format)
    yesterday = datetime.strptime(yesterday_str,timestamp_format)
    latest_date = today.strftime("%Y-%m-%d")
    file_path = os.path.join(destination_path,latest_date,file_name)
    cols_select = ['timestamp','temp','lat','lon']
    
    if os.path.exists(file_path):
        data = xr.open_dataset(file_path)
        df = data.to_dataframe().reset_index()
        df['Times']=df['Times'].apply(parse_timestamp)
        df['Times'] = df['Times'] + timedelta(hours=5,minutes=30)
        df['T2'] = df['T2'] - 273.15
        df.rename(columns={'Times':'timestamp','XLAT':'lat',"XLONG":"lon","T2":'temp'},inplace=True)
        df['st'] = 'n'
        df = df.loc[:,cols_select]
        #thsi si asd
        if len(df) > 1000:
            with data_connection.connect() as conn:
                try:
                    conn.execute(text(f"UPDATE data_forecast.wrf_temp SET st='o' WHERE timestamp < '{today_str}' and timestamp >= '{yesterday_str}' and st='n'"))
                    conn.commit()
                    print("UPDATING OLD TIMESTAMPS")
                    conn.execute(text(f"UPDATE data_forecast.wrf_temp SET st='u' WHERE timestamp >= '{today_str}' and st='n'"))
                    conn.commit()
                    print("UPDATING DELETE TIMESTAMPS")
                except Exception as e:
                    print(e)
                    print("Error Updating")
            try:
                print("Uploading Latest DATA")
                resp = df.to_sql(schema='data_forecast',
                  name='wrf_temp',
                  index=False,
                  if_exists='append',
                  con=data_connection,
                  method='multi',
                  chunksize=100000)
                if resp:
                    with db_connection.connect() as conn_db:
                        conn_db.execute(f"UPDATE {file_logs_schema}.transfer_wrf_logs SET read_status = 1 WHERE file='{file_name}' and read_status=0")
                        conn_db.commit()
                    with data_connection.connect() as conn_data:
                        try:
                            conn_data.execute(text(f"DELETE FROM data_forecast.wrf_temp WHERE st='u'"))
                            conn_data.commit()
                        except Exception as e:
                            print("Error Deleting Data")
                        
                else:
                    print("Cannot upload data to database")
            except Exception as e:
                print("ERROR IN LATEST DATA TRANSFER")
                print(e)
        else:
            print("Not much data available")
        # df.to_csv('sample.csv')
        
    

db_connection = get_connection(host=data_configs_map['host'],
                               passord=data_configs_map['password'],
                               user=data_configs_map['user'],
                               database=data_configs_map['database'],
                               port=data_configs_map['port'])

data_connection = get_connection(host=data_send['host'],
                               passord=data_send['password'],
                               user=data_send['user'],
                               database=data_send['database'],
                               port=data_send['port'])

today = get_start_and_end_of_day()
yesterday = today - timedelta(days=1)
tomorrow = today + timedelta(days=1)

today_str = today.strftime(timestamp_format)
tomorrow_str = tomorrow.strftime(timestamp_format)
yesterday_str = yesterday.strftime(timestamp_format)
df_transf = get_wrf_transferred(db_connection=db_connection)

df_transf.sort_values('file',ascending=False,inplace=True)
df_transf = df_transf.head(1)
for index,row in df_transf.iterrows():
    read_wrf(file_name=row['file'],
             data_connection=data_connection,
             db_connection=db_connection,
             today_str=today_str,
             yesterday_str=yesterday_str,
             tomorrow_str=tomorrow_str)
    

