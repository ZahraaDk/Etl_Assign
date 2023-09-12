import os 
from database_handler import execute_query, return_input_as_df, return_create_statement_from_df, create_connection, close_connection
from lookups import Error, PreHookSteps, SQLTablesToReplicate, Input_Types, SourceName
from logging_handler import error_message
import pandas as pd 

def execute_sql_folder(db_session, sql_directory_path):
    # [] for creating a new list, directory path: path to a directory where sql commands are stored.
    sql_files = [sqlfile for sqlfile in os.listdir(sql_directory_path) if sqlfile.endswith('.sql')]
    # for the execution to be ordered
    sorted_sql_files = sorted(sql_files)
    for sql_file in sorted_sql_files:
        # specifying the whole path to the file, and the file is opened for reading 
        with open(os.path.join(sql_directory_path, sql_file), 'r') as file:
            sql_query = file.read()
            return_val = execute_query(db_session=db_session, query=sql_query)
            if not return_val == Error.NO_ERROR:
                raise Exception(f"{PreHookSteps.EXECUTE_SQL_QUERY.value} = SQL FILE Error on SQL File = " + str(sql_file))

def return_tables_by_schema(schema_name):
    schame_tables = list()
    tables = [table.value for table in SQLTablesToReplicate]
    for table in tables:
        if table.split('.')[0] == schema_name:
            schame_tables.append(table.split('.')[1])
    print("Schema tables: " + schame_tables)
    return schame_tables

def create_sql_staging_tables(db_session, source_name):
    tables = return_tables_by_schema(source_name)
    for table in tables:
        staging_query = f"""
                SELECT * FROM {source_name}.{table} LIMIT 1
        """
        # we had the limit 1 bc we are only interested in the column names, not to waste time with the full data
        staging_df = return_input_as_df(db_session= db_session, input_type= Input_Types.SQL, input_data = staging_query)
        dst_table = f"stg_{source_name}_{table}"
        create_statement = return_create_statement_from_df(staging_df, 'dw_reporting', dst_table)
        execute_query(db_session=db_session, query= create_statement)

def execute_csv(db_session, csv_directory_path):
    csv_files = [csv_file for csv_file in os.listdir(csv_directory_path) if csv_file.endswith('.csv')]
    for csv_file in csv_files:
        with open(os.path.join(csv_directory_path, csv_file), 'r') as csv_path:
            csv_df = pd.read_csv(csv_path)
            table = os.path.splitext(csv_file)[0]
            staging_df = f"stg_{public_schema}_{table}"
            create_stmt = return_create_statement_from_df(staging_df, 'dw_reporting', dst_table)
            execute_query(db_session=db_session, query= create_stmt)

def execute_prehook(sql_command_directory_path = './SQL_Commands'):
    try:
        db_session = create_connection()
        # Step 1:
        execute_sql_folder(db_session, sql_command_directory_path) 
        # Step 2 getting dvd rental staging:
        create_sql_staging_tables(db_session,SourceName.DVDRENTAL.value)
        execute_csv(db_session, csv_directory)
        # Step 3 getting college staging:
        # create_sql_staging_tables(db_session,SourceName.COLLEGE)
        close_connection(db_session)
    except Exception as error:
        suffix = str(error)
        error_prefix = Error.PREHOOK_SQL_ERROR
        error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")