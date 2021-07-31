from flask import current_app
from inspect import currentframe
from .functions import checkData, get_badrows
import pandas as pd

def watershed(all_dfs):
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # This data type should only have tbl_watershed
    watershed = all_dfs['tbl_watershed']

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    args = {
        "dataframe": watershed,
        "tablename": 'tbl_watershed',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    # (1) 

    lu_insitusoil = pd.read_sql("SELECT * FROM lu_insitusoil", current_app.eng).insitusoil.to_list() 
    df_badrows = watershed[~watershed['insitusoil'].isnull()][watershed['insitusoil'].isin([
        x for x in watershed['insitusoil'] if x not in lu_insitusoil
    ])]
    args.update({
        "dataframe": watershed, 
        "tablename": "tbl_watershed",
        "badrows": get_badrows(df_badrows),
        "badcolumn": "insitusoil",
        "error_type" : "Lookup Fail",
        "error_message" : "Entry is not in lookup list."
    })
    errs = [*errs, checkData(**args)]



    return {'errors': errs, 'warnings': warnings}