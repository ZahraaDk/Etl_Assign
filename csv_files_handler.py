import os #operating system 
import pandas as pd
from lookups import Error 
from logging_handler import error_message 

def list_csv_files(folder):
    if not os.path.exists(folder):
        raise Exception("Doesn't Exist!")
    try:
        df_list = []
        files = os.listdir(folder)
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(folder, file)
                df = pd.read_csv(file_path)
                df_list.append(df)
        return df_list
    except Exception as csv_e:
        error_prefix = Error.CSV_Error
        error_suffix = str(csv_e)
        error_message(error_prefix, error_suffix)
    finally:
        return df_list 

