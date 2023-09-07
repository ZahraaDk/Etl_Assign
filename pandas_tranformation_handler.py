from lookups import Pandas_Transformations, Error
from logging_handler import error_message 

def pandas_transformation_functions(input_df, function):
    output_df = None 
    try:
        if function == Pandas_Transformations.Remove_Duplicate:
            output_df = input_df.drop_duplicates()
        elif function == Pandas_Transformations.Remove_Nulls:
            output_df = input_df.dropna()
        elif function == Pandas_Transformations.Get_Blanks:
            output_df = input_df.isna()
        elif function == Pandas_Transformations.Get_Shape:
            output_df = input_df.shape()
        elif function == Pandas_Transformations.Get_Length:
            output_df = len(input_df)
        else:
            raise Exception("Undefined function")
    except Exception as pandas_e:
        if function == Pandas_Transformations.Remove_Duplicate:
            error_prefix = Error.Removing_Duplicates_Error.value
        elif function == Pandas_Transformations.Remove_Nulls:
            error_prefix = Error.Removing__Nulls_Error.value
        elif function == Pandas_Transformations.Get_Blanks:
            error_prefix = Error.Getting_Blanks_Error.value
        elif function == Pandas_Transformations.Get_Shape:
            error_prefix = Error.Getting_Shape_Error.value
        elif function == Pandas_Transformations.Get_Length:
            error_prefix = Error.Getting_Length_Error.value
        else:
            error_prefix = Error.Not_Defined_Function_Error.value
        error_suffix = str(pandas_e)
        error_message(error_prefix, error_suffix)
    finally:
        return output_df 

