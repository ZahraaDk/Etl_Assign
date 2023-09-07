from enum import Enum

# Enumerations, way to create named constants in programming. 
class Error(Enum):
    DB_Connecting_Error = "Error Connecting To Database!"
    DB_ReturnQuery_Error = "Error Returning Query"
    DB_Return_Df_Error = "Error returning this file type" 
    DB_Executing_Error = "Error executing the query"
    DB_Getting_SchemaInfo_Error = "Error getting the schema information"

class Input_Types(Enum):
    CSV = 'CSV'
    EXCEL = 'Excel'
    SQL = 'SQL' 
