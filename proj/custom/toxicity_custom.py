# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, get_primary_key, mismatch
import pandas as pd

def toxicity(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # define errors and warnings list
    errs = []
    warnings = []


    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # This data type should only have tbl_example
    # example = all_dfs['tbl_example']

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    # args = {
    #     "dataframe": example,
    #     "tablename": 'tbl_example',
    #     "badrows": [],
    #     "badcolumn": "",
    #     "error_type": "",
    #     "is_core_error": False,
    #     "error_message": ""
    # }

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": df[df.temperature != 'asdf'].index.tolist(),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    # return {'errors': errs, 'warnings': warnings}

    toxicitybatch = all_dfs['tbl_toxicitybatch'].assign(tmp_row = all_dfs['tbl_toxicitybatch'].index)
    toxicityresults = all_dfs['tbl_toxicityresults'].assign(tmp_row = all_dfs['tbl_toxicityresults'].index)
    toxicitysummary = all_dfs['tbl_toxicitysummary'].assign(tmp_row = all_dfs['tbl_toxicitysummary'].index)

    grabevent_details = pd.read_sql("SELECT * FROM tbl_grabevent_details", g.eng)

    toxicitybatch_args = {
        "dataframe": toxicitybatch,
        "tablename": 'tbl_toxicitybatch',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    toxicityresults_args = {
        "dataframe": toxicityresults,
        "tablename": 'tbl_toxicityresults',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    toxicitysummary_args = {
        "dataframe": toxicitysummary,
        "tablename": 'tbl_toxicitysummary',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- Toxicity Logic Checks ---------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    print("# CHECK - 1")
    # Description: Each toxicitysummary record must have correspond record in the grabeventdetails in database
    # Created Coder: Caspian Thackeray
    # Created Date: 09/05/23
    # Last Edited Date: 09/08/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (09/08/23): Wrote new code to checker standards

    toxsum_pkey = get_primary_key('tbl_toxicitysummary', g.eng)
    grabdeets_pkey = get_primary_key('tbl_grabevent_details', g.eng)

    toxsum_grabdeets_shared_pkey = list(set(toxsum_pkey).intersection(set(grabdeets_pkey)))

    toxicitysummary_args.update({
        "dataframe": toxicitysummary,
        "tablename": 'tbl_toxicitysummary',
        "badrows": mismatch(toxicitysummary, grabevent_details, toxsum_grabdeets_shared_pkey),
        "badcolumn": ','.join(toxsum_grabdeets_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each toxicitysummary record must have corresponding record in the grabevent_details table in database"
    })
    errs = [*errs, checkData(**toxicitysummary_args)]
    print("# END of CHECK - 1")

    print("# CHECK - 2")
    # Description: Each toxicitybatch record must have corresponding record in the grabevent_details table in database
    # Created Coder: Caspian Thackeray
    # Created Date: 09/05/23
    # Last Edited Date: 09/08/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (09/08/23): Wrote new code to checker standards
    
    toxbatch_pkey = get_primary_key('tbl_toxicitybatch', g.eng)
    grabdeets_pkey = get_primary_key('tbl_grabevent_details', g.eng)

    toxbatch_grabdeets_shared_pkey = list(set(toxbatch_pkey).intersection(set(grabdeets_pkey)))

    print('variables done')

    toxicitybatch_args.update({
        "dataframe": toxbatch_pkey,
        "tablename": 'tbl_toxicitybatch',
        "badrows": mismatch(toxicitybatch, grabevent_details, toxbatch_grabdeets_shared_pkey),
        "badcolumn": ','.join(toxbatch_grabdeets_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each toxicitybatch record must have corresponding record in the grabevent_details table in database"
    })
    print('update done')
    errs = [*errs, checkData(**toxicitybatch_args)]

    print("# END of CHECK - 2")

    print("# CHECK - 3")
    # Description: Each toxicityresults record must have corresponding record in the grabevent_details table in database
    # Created Coder: Caspian Thackeray
    # Created Date: 09/05/23
    # Last Edited Date: 09/08/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (09/08/23): Wrote new code to checker standards
    
    toxres_pkey = get_primary_key('tbl_toxicityresults', g.eng)
    grabdeets_pkey = get_primary_key('tbl_grabevent_details', g.eng)

    toxres_grabdeets_shared_pkey = list(set(toxres_pkey).intersection(set(grabdeets_pkey)))

    toxicityresults_args.update({
        "dataframe": toxbatch_pkey,
        "tablename": 'tbl_toxicityresults',
        "badrows": mismatch(toxicityresults, grabevent_details, toxres_grabdeets_shared_pkey),
        "badcolumn": ','.join(toxres_grabdeets_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each toxicitybatch record must have corresponding record in the grabevent_details table in database"
    })
    errs = [*errs, checkData(**toxicityresults_args)]

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------END of Toxicity Logic Checks ------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################  

    return {'errors': errs, 'warnings': warnings}
