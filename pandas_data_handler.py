import pandas as pd
from database_handler import return_query
def return_create_statement_from_df(dataframe, schema_name, table_name):
    type_mapping = {
        'int64' : 'INT', 
        'float64' : 'FLOAT', 
        'datetime64[ns]' : 'TIMESTAMP', 
        'bool' : 'BOOLEAN', 
        'object' : 'TEXT'
    }
    fields = []
    for column, dtype in dataframe.dtypes.items():
        sql_type = type_mapping.get(str(dtype), 'TEXT')
        fields.append(f"{column} {sql_type}")

    create_table_statement = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n"
    create_table_statement += ", \n".join(fields) 
    create_table_statement += "\n);"
    return create_table_statement

def return_insert_into_sql_statement_from_df(dataframe, schema_name, table_name):
    columns = ', '.join(dataframe.columns)
    values_list = []
    for _, row in dataframe.iterrows():
        value_strs = []
        for val in row.values:
            if pd.isna(val):
                value_strs.append("NULL")
            elif isinstance(val, str):
                # Escape single quotes in the string
                val_escaped = val.replace("'", "''")
                value_strs.append(f"'{val_escaped}'")
            else:
                value_strs.append(str(val))
        values = ', '.join(value_strs)
        values_list.append(f"({values})")
    values_str = ',\n'.join(values_list)
    insert_statement = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES\n{values_str};"
    return insert_statement

def check_if_table_exists(db_session, schema_name, table_name):
    return_val = False
    query = f"""
    SELECT 
        COUNT(1)
    FROM information_schema.tables
    WHERE table_schema = '{schema_name}'
    AND table_name = '{table_name}'
    """
    return_data = return_query(db_session= db_session, query= query)
    if return_data[0][0] > 0:
        return_val = True
    return return_val