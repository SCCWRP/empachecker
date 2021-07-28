from inspect import currentframe
from flask import current_app
from .functions import checkData, get_badrows

def datalogger(all_dfs):
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
    
    # This data type should only have tbl_data_logger_raw
    # example = all_dfs['tbl_data_logger_raw']
    datalogger = all_dfs['tbl_data_logger_raw']

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    args = {
        "dataframe": datalogger,
        "tablename": 'tbl_data_logger_raw',
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



    args.update({
        "badrows": get_badrows(datalogger[datalogger.intensity != 5]),
        "badcolumn": "intensity",
        "error_type" : "Not 5",
        "error_message" : "This is a helpful useful message for the user"
    })
    warnings = [*warnings, checkData(**args)]

    args.update({
        "badrows": get_badrows(datalogger[datalogger.temperature != 5]),
        "badcolumn": "temperature",
        "error_type" : "Not 5",
        "error_message" : "The temperature should be 5"
    })
    warnings = [*warnings, checkData(**args)]
        
    
    return {'errors': errs, 'warnings': warnings}