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
        "error_message": "Each toxicity summary record must have corresponding record in the grabevent_details table in database"
    })
    errs = [*errs, checkData(**toxicitysummary_args)]
    print("# END of CHECK - 1")

    print("# CHECK - 2")
    # Description: Each toxicitybatch record must have corresponding record in the grabevent_details table in database
    # Created Coder: Caspian Thackeray
    # Created Date: 09/05/23
    # Last Edited Date: 09/13/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (09/12/23): Still working on this one
    # NOTE (09/13/23):  The problem with this check is that there are no matching primary keys between grabevent_details and tox batch.
    #                   Also, there aren't any useful matching primary keys between tox summary and batch to use a work around
    #                   I'll come back to this check later
    
    toxbatch_pkey = get_primary_key('tbl_toxicitybatch', g.eng)
    toxsum_pkey = get_primary_key('tbl_toxicitysummary', g.eng)
    grabdeets_pkey = get_primary_key('tbl_grabevent_details', g.eng)

    toxbatch_toxsum_shared_pkey = list(set(toxbatch_pkey).intersection(set(toxsum_pkey)))
    toxsum_grabdeets_shared_pkey = list(set(toxsum_pkey).intersection(set(grabdeets_pkey)))

    summ_bad_rows = mismatch(toxicitysummary, grabevent_details, toxsum_grabdeets_shared_pkey)
    summ_batch_mismatching_rows = mismatch(toxicitysummary, toxicitybatch, toxbatch_toxsum_shared_pkey)

    batch_grab_intersect = list(set(summ_bad_rows).intersection(set(summ_batch_mismatching_rows)))
    """
    toxicitybatch_args.update({
        "dataframe": toxicitybatch,
        "tablename": 'tbl_toxicitybatch',
        "badrows": "",
        "badcolumn": ','.join(toxbatch_toxsum_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each toxicitybatch record must have corresponding record in the grabevent_details table in database"
    })
    print('update done')
    errs = [*errs, checkData(**toxicitybatch_args)]
    """

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
        "dataframe": toxicityresults,
        "tablename": 'tbl_toxicityresults',
        "badrows": mismatch(toxicityresults, grabevent_details, toxres_grabdeets_shared_pkey),
        "badcolumn": ','.join(toxres_grabdeets_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each toxicity results record must have corresponding record in the grabevent_details table in database"
    })
    errs = [*errs, checkData(**toxicityresults_args)]

    print("# END of CHECK - 3")

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

    toxicitysummary_args.update({
        "dataframe": toxicitysummary,
        "tablename": 'tbl_toxicitysummary',
        "badrows": toxicitysummary[(toxicitysummary['samplecollectiondate'].duplicated()) & (toxicitysummary['siteid'].duplicated())].tmp_row.tolist(),
        "badcolumn": 'siteid, samplecollectiondate',
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": f'Warning! You are submitting toxicity data with multiple dates for the same site. {multiple_dates_within_site_summary[1]} unique sample dates were submitted. Is this correct?'
    })
    warnings = [*warnings, checkData(**toxicitysummary_args)]
    
    toxicityresults_args.update({
        "dataframe": toxicityresults,
        "tablename": 'tbl_toxicityresults',
        "badrows": toxicityresults[(toxicityresults['samplecollectiondate'].duplicated()) & (toxicityresults['siteid'].duplicated())].tmp_row.tolist(),
        "badcolumn": 'siteid, samplecollectiondate',
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": f'Warning! You are submitting toxicity data with multiple dates for the same site. {multiple_dates_within_site_results[1]} unique sample dates were submitted. Is this correct?'
    })
    warnings = [*warnings, checkData(**toxicityresults_args)]

    print("# END of CHECK - 4")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------ END of Toxicity Custom Checks ----------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    return {'errors': errs, 'warnings': warnings}
