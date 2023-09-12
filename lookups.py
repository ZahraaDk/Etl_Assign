from enum import Enum

# Enumerations, way to create named constants in programming. 
class Error(Enum):
    DB_Connecting_Error = "Error Connecting To Database!"
    DB_ReturnQuery_Error = "Error Returning Query"
    DB_Return_Df_Error = "Error returning this file type" 
    Return_From_CSV_Error = "Error returning this CSV file"
    Return_From_Excel_Error = "Error returning this Excel file"
    Return_From_SQL_Error = "Error returning this SQL file"
    Undefined_File_Type = "This file type is not defined"
    DB_Executing_Error = "Error executing the query"
    DB_Getting_SchemaInfo_Error = "Error getting the schema information"
    Removing_Duplicates_Error = "Error removing duplicates"
    Removing__Nulls_Error = "Error removing nulls"
    Getting_Blanks_Error = "Error getting the rows with null values"
    Getting_Shape_Error = "Error finding the shape of the dataframe"
    Getting_Length_Error = "Error getting the length of the dataframe"
    Not_Defined_Function_Error = "This function is not defined"
    CSV_Error = "Error returning a list of csv files"
    NO_ERROR = "No error!"


class Pandas_Transformations(Enum):
    Remove_Duplicates = "Remove Duplicates From Df"
    Remove_Nulls = "Remove null values from Df"
    Get_Blanks = "Get blanks from Df"
    Get_Shape = "Find the shape of the Df"
    Get_Length = "Get the total length of the Df"


class Input_Types(Enum):
    CSV = 'CSV'
    EXCEL = 'Excel'
    SQL = 'SQL' 

class PreHookSteps(Enum):
    EXECUTE_SQL_QUERY = "execute_sql_folder"

class SQLTablesToReplicate(Enum):
    Rental = "rental"
    Actor = "actor"

class SourceName(Enum):
    DVDRENTAL = "DvdRental"
