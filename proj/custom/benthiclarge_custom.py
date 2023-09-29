# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, get_primary_key, mismatch
import pandas as pd

def benthiclarge(all_dfs):
    
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

    benthiclargemeta = all_dfs['tbl_benthiclarge_metadata']
    benthiclargeabundance= all_dfs['tbl_benthiclarge_abundance']

    benthiclargemeta['tmp_row'] = benthiclargemeta.index
    benthiclargeabundance['tmp_row'] = benthiclargeabundance.index

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

    args = {
        "dataframe": pd.DataFrame({}),
        "tablename": '',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ benthiclarge Logic Checks --------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    benthiclargemeta_pkey = get_primary_key('tbl_benthiclarge_metadata', g.eng)
    benthiclargeabundance_pkey = get_primary_key('tbl_benthiclarge_abundance', g.eng)


    benthiclargemeta_benthiclargeabundance_shared_pkey = [x for x in benthiclargemeta_pkey if x in benthiclargeabundance_pkey]

    print("# CHECK - 1")
    # Description: Each record in benthiclarge_metadata must include corresponding record in benthiclarge_abundance (🛑 ERROR 🛑)
    # Created Coder: Duy
    # Created Date: 9/28/2023
    # Last Edited Date: 
    # Last Edited Coder:
    # NOTE (9/28/2023): Duy created the check, has not QA'ed yet. 
    args.update({
        "dataframe": benthiclargemeta,
        "tablename": "tbl_benthiclarge_metadata",
        "badrows": mismatch(benthiclargemeta, benthiclargeabundance, benthiclargemeta_benthiclargeabundance_shared_pkey), 
        "badcolumn": ','.join(benthiclargemeta_benthiclargeabundance_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in benthiclarge_metadata must include corresponding record in benthiclarge_abundance"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: Each record in benthiclarge_abundance must include corresponding record in benthiclarge_metadata (🛑 ERROR 🛑)
    # Created Coder: Duy
    # Created Date: 9/28/2023
    # Last Edited Date: 
    # Last Edited Coder:
    # NOTE (9/28/2023): Duy created the check, has not QA'ed yet. 
    args.update({
        "dataframe": benthiclargeabundance,
        "tablename": "tbl_benthiclarge_abundance",
        "badrows": mismatch(benthiclargeabundance, benthiclargemeta, benthiclargemeta_benthiclargeabundance_shared_pkey), 
        "badcolumn": ','.join(benthiclargemeta_benthiclargeabundance_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in benthiclarge_abundance must include corresponding record in benthiclarge_metadata"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 2")
    
    
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------end of benthiclarge Logic Checks --------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################



    return {'errors': errs, 'warnings': warnings}