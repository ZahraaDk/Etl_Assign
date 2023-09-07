import psycopg2
import pandas as pd 
import time
from lookups import Error, Input_Types, Pandas_Transformations
from logging_handler import error_message

db_name = "DvdRental"
db_user = "postgres"
db_pass = "admin"
db_host = "localhost"
db_port = 5432

def create_connection():
    db_session = None 
    try:
        db_session = psycopg2.connect(
            database = db_name, 
            user = db_user, 
            password = db_pass, 
            host = db_host, 
            port = db_port
        )
    except Exception as create_e:
        error_prefix = Error.DB_Connecting_Error.value
        error_suffix = str(create_e)
        error_message(error_prefix, error_suffix)
    else:
        print(f"Connection is Successful")
    finally:
        return db_session 

def close_connection(db_session):
    db_session.close()

def refresh_connection(db_session):
    db_session.close()
    time.sleep(5)
    db_session = create_connection()
    return db_session 

def return_query(db_session, query):
    query_result = []
    try:
        with db_session.cursor() as cursor:
            cursor.execute(query)
            query_result = cursor.fetchall()
        db_session.commit()
    except Exception as return_e:
        error_prefix = Error.DB_ReturnQuery_Error.value
        error_suffix = str(return_e)
        error_message(error_prefix, error_suffix)
    finally:
        return query_result

def return_input_as_df(input_data, input_type, db_session = None):
    df = None
    try:
        if input_type == Input_Types.CSV:
            df = pd.read_csv(input_data)
        elif input_type == Input_Types.EXCEL:
            df = pd.read_excel(input_data)
        elif input_type == Input_Types.SQL:
            df = pd.read_sql_query(sql = input_data, con = db_session)
        else:
            raise Exception ("Undefined input file type, try another time! ")
    except Exception as returndf_e:
        error_suffix = str(returndf_e)
        if input_type == Input_Types.CSV:
            error_prefix = Error.Return_From_CSV_Error.value
        elif input_type == Input_Types.EXCEL:
            error_prefix = Error.Return_From_Excel_Error.value
        elif input_type == Input_Types.SQL:
            error_prefix = Error.Return_From_SQL_Error.value       
        error_message(error_prefix, error_suffix)
    finally:
        return df

def execute_query(db_session, query):
    try:
        with db_session.cursor() as cursor:
            cursor.execute(query)
        db_session.commit()
    except Exception as execute_e:
        error_prefix = Error.DB_Executing_Error.value
        error_suffix = str(execute_e)
        error_message(error_prefix, error_suffix)

def schema_information(db_session, query):
    schema_info = None
    try:
        with db_session.cursor() as cursor:
            cursor.execute(query)
            schema_info = cursor.fetchall()
        return schema_info
    except Exception as schema_e:
        error_prefix = Error.DB_Getting_SchemaInfo_Error.value
        error_suffix = str(schema_e)
        error_message(error_prefix, error_suffix)
    finally:
        return schema_info 
 