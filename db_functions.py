from sqlalchemy import create_engine, text
from paths import *
import pandas as pd


def get_connection(host,port,user,passord,database):
    connection_string = f"postgresql://{user}:{passord}@{host}/{database}"
    db_connect = create_engine(connection_string)
    try:
        with db_connect.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("\n\n---------------------Connection Successful")
        return db_connect
    except Exception as e:
        print("\n\n---------------------Connection Failed")
        print(f"{e}")
        
        
def get_wrf_transferred(db_connection):
    df = pd.read_sql_query("SELECT * FROM files_map_logs.transfer_wrf_logs WHERE status= 'transferred' and read_status=0",con=db_connection)
    return df