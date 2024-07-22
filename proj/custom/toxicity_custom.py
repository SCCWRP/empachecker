# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, get_primary_key, mismatch, check_multiple_dates_within_site
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

    toxicitysummary = all_dfs['tbl_toxicitysummary'].assign(tmp_row = all_dfs['tbl_toxicitysummary'].index)
    toxicitybatch = all_dfs['tbl_toxicitybatch'].assign(tmp_row = all_dfs['tbl_toxicitybatch'].index)
    toxicityresults = all_dfs['tbl_toxicityresults'].assign(tmp_row = all_dfs['tbl_toxicityresults'].index)

    grabevent_details = pd.read_sql("SELECT * FROM tbl_grabevent_details WHERE sampletype = 'toxicity'", g.eng)

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
    # --------------------------------------------- Toxicity Logic Checks ---------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    toxicitybatch_pkey = get_primary_key('tbl_toxicitybatch', g.eng)
    toxicitysummary_pkey = get_primary_key('tbl_toxicitysummary', g.eng)
    grabeventdetails_pkey = get_primary_key('tbl_grabevent_details', g.eng)
    
    toxicitysummary_grabeventdetails_shared_pkey = [x for x in toxicitysummary_pkey if x in grabeventdetails_pkey]
    toxicitysummary_toxicitybatch_shared_pkey = [x for x in toxicitysummary_pkey if x in toxicitybatch_pkey]

    if 'samplecollectiondate' in toxicitysummary.columns:
        toxicitysummary['samplecollectiondate'] = pd.to_datetime(toxicitysummary['samplecollectiondate'])
    if 'samplecollectiondate' in grabevent_details.columns:
        grabevent_details['samplecollectiondate'] = pd.to_datetime(grabevent_details['samplecollectiondate'])

    print("# CHECK - 1")
    # Description: Each toxicitysummary record must have correspond record in the grabeventdetails in database
    # Created Coder: Caspian
    # Created Date: 09/01/23
    # Last Edited Date: 10/05/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (9/18/23): Duy rewrites the logic check
    # NOTE (10/05/2023): Aria revised the error message

    args.update({
        "dataframe": toxicitysummary,
        "tablename": "tbl_toxicitysummary",
        "badrows": mismatch(toxicitysummary, grabevent_details, toxicitysummary_grabeventdetails_shared_pkey), 
        "badcolumn": ','.join(toxicitysummary_grabeventdetails_shared_pkey),
        "error_type": "Logic Error",
        "error_message": 
            "Each record in tbl_toxicitysummary must have a corresponding field record. You must submit the field data to the checker first. The Field template can be downloaded on empa.sccwrp.org (Field Grab table).  Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(toxicitysummary_grabeventdetails_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    
    print("# END of CHECK - 1")

    print("# CHECK - 2")
    # Description: Each toxicitysummary record must have corresponding record in toxicitybatch
    # Created Coder: Caspian
    # Created Date: 09/01/23
    # Last Edited Date: 10/05/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (9/18/23): Duy rewrites the logic check
    # NOTE (10/05/2023): Aria revised the error message

    args.update({
        "dataframe": toxicitysummary,
        "tablename": "tbl_toxicitysummary",
        "badrows": mismatch(toxicitysummary, toxicitybatch, toxicitysummary_toxicitybatch_shared_pkey), 
        "badcolumn": ','.join(toxicitysummary_toxicitybatch_shared_pkey),
        "error_type": "Logic Error",
        "error_message": 
            "Each record in tbl_toxicitysummary must have a corresponding record in tbl_toxicitybatch.  Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(toxicitysummary_toxicitybatch_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]

    print("# END of CHECK - 2")


    print("# CHECK - 3")
    # Description: Each toxicitybatch record must have corresponding record in toxicitysummary
    # Created Coder: Caspian
    # Created Date: 09/01/23
    # Last Edited Date: 10/05/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (9/18/23): Duy rewrites the logic check
    # NOTE (10/05/2023): Aria revised the error message
    args.update({
        "dataframe": toxicitybatch,
        "tablename": "tbl_toxicitybatch",
        "badrows": mismatch(toxicitybatch, toxicitysummary, toxicitysummary_toxicitybatch_shared_pkey), 
        "badcolumn": ','.join(toxicitysummary_toxicitybatch_shared_pkey),
        "error_type": "Logic Error",
        "error_message": 
            "Each record in tbl_toxicitybatch must have a corresponding record in tbl_toxicitysummary.  Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(toxicitysummary_toxicitybatch_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]

    print("# END of CHECK - 3")


    print("# CHECK - 4")
    # Description: Each toxicityresults record must have corresponding record in the toxicitybatch
    # Created Coder: Caspian
    # Created Date: 09/01/23
    # Last Edited Date: 10/05/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (9/18/23): Duy rewrites the logic check
    # NOTE (10/05/2023): Aria revised the error message

    args.update({
        "dataframe": toxicitybatch,
        "tablename": "tbl_toxicitybatch",
        "badrows": mismatch(toxicitybatch, toxicitysummary, toxicitysummary_toxicitybatch_shared_pkey), 
        "badcolumn": ','.join(toxicitysummary_toxicitybatch_shared_pkey),
        "error_type": "Logic Error",
        "error_message": 
            "Each record in tbl_toxicitybatch must have a corresponding record in tbl_toxicitysummary.  Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(toxicitysummary_toxicitybatch_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]

    print("# END of CHECK - 4")



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------ END of Toxicity Logic Checks ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # -------------------------------------------- Toxicity Custom Checks ---------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 4")
    # Description: Within toxicity data, return a warning if a submission contains multiple dates within a single site
    # Created Coder: Caspian Thackeray
    # Created Date: 09/13/23
    # Last Edited Date: 09/13/23
    # Last Edited Coder: Caspian Thackeray

    multiple_dates_within_site_summary = check_multiple_dates_within_site(toxicitysummary)
    multiple_dates_within_site_results = check_multiple_dates_within_site(toxicityresults)

    args.update({
        "dataframe": toxicitysummary,
        "tablename": "tbl_toxicitysummary",
        "badrows": multiple_dates_within_site_summary[0], 
        "badcolumn": 'siteid,samplecollectiondate',
        "error_type": "Logic Error",
        "error_message": "Multiple dates are submitted within a single site"
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": toxicityresults,
        "tablename": "tbl_toxicityresults",
        "badrows": multiple_dates_within_site_results[0], 
        "badcolumn": 'siteid,samplecollectiondate',
        "error_type": "Logic Error",
        "error_message": "Multiple dates are submitted within a single site"
    })
    errs = [*errs, checkData(**args)]


    print("# END of CHECK - 4")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------ END of Toxicity Custom Checks ----------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    return {'errors': errs, 'warnings': warnings}
