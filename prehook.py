import os 
from database_handler import execute_query, return_input_as_df, return_create_statement_from_df, create_connection, close_connection
from lookups import Error, PreHookSteps, SQLTablesToReplicate, Input_Types, SourceName
from logging_handler import error_message
import pandas as pd 

def execute_query_folder(db_session, query_directory_path):
    query_files = os.listdir(query_directory_path)

    sql_files = [file for file in query_files if file.endswith('.sql')]
    csv_files = [file for file in query_files if file.endswith('.csv')]
    sorted_sql_files = sorted(sql_files)
    sorted_csv_files = sorted(csv_files)

    for sql_file in sorted_sql_files:
        with open(os.path.join(query_directory_path, sql_file), 'r') as file:
            sql_query = file.read()
            return_val = execute_query(db_session=db_session, query=sql_query)
            if not return_val == Error.NO_ERROR:
                raise Exception(f"{PreHookSteps.EXECUTE_SQL_QUERY.value} - SQL FILE Error on SQL File = " + str(sql_file))
            
    for csv_file in sorted_csv_files:
        csv_path = os.path.join(query_directory_path, csv_file)
        staging_df = pd.read_csv(csv_path, nrows=1) #Get the first row only to get column names
        staging_df.columns = staging_df.columns.str.replace(' ', '_').str.replace('-', '_')
        table_name = f"stg_{os.path.splitext(csv_file)[0]}"
        table_name = table_name.replace(' ', '_').replace('-', '_')
        create_statement = return_create_statement_from_df(staging_df, 'dw_reporting', table_name)
        execute_query(db_session=db_session, query=create_statement)

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


def execute_prehook(query_directory_path = './Csv_Files'):
    try:
        db_session = create_connection()
        # Step 1:
        execute_query_folder(db_session, query_directory_path) 
        # Step 2 getting dvd rental staging:
        create_sql_staging_tables(db_session,SourceName.DVDRENTAL.value)
        close_connection(db_session)
    except Exception as error:
        suffix = str(error)
        error_prefix = Error.PREHOOK_SQL_ERROR
        error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")