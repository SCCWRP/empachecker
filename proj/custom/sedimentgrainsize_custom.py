from inspect import currentframe
from flask import current_app, g
from pandas import DataFrame
import pandas as pd
from .functions import checkData, checkLogic, mismatch,get_primary_key, check_consecutiveness




def sedimentgrainsize_lab(all_dfs):
    
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
    
    # This data type should only have tbl_example
    # example = all_dfs['tbl_example']
    
    sed_data = all_dfs['tbl_sedgrainsize_data']
    sed_labbatch = all_dfs['tbl_sedgrainsize_labbatch_data']
    grabeventdetails = pd.read_sql("SELECT * FROM tbl_grabevent_details WHERE sampletype = 'grainsize'", g.eng)

    sed_data['tmp_row'] = sed_data.index
    sed_labbatch['tmp_row'] = sed_labbatch.index
    grabeventdetails['tmp_row'] = grabeventdetails.index

    sed_data_pkey = get_primary_key('tbl_sedgrainsize_data', g.eng)
    sed_labbatch_pkey = get_primary_key('tbl_sedgrainsize_labbatch_data', g.eng)
    grabeventdetails_pkey = get_primary_key('tbl_grabevent_details', g.eng)

    sed_labbatch_grabevntdetails_shared_key = [x for x in sed_labbatch_pkey if x in grabeventdetails_pkey]
    sed_data_sed_labbatch_shared_pkey = [x for x in sed_data_pkey if x in sed_labbatch_pkey]


    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    args = {
        "dataframe": sed_data,
        "tablename": 'tbl_sedgrainsize_data',
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

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ SedGrainSize Logic Checks --------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    print("# CHECK - 1")
    # Description: Each labbatch data must correspond to grabeventdetails in database
    # Created Coder: Ayah
    # Created Date: 09/15/2021
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Ayah created logic check, has not tested yet
    # NOTE (10/05/2023): Aria revised the error message

    if 'samplecollectiondate' in sed_labbatch.columns:
        sed_labbatch['samplecollectiondate'] = pd.to_datetime(sed_labbatch['samplecollectiondate'])
    if 'samplecollectiondate' in grabeventdetails.columns:
        grabeventdetails['samplecollectiondate'] = pd.to_datetime(grabeventdetails['samplecollectiondate'])

    args.update({
        "dataframe": sed_labbatch,
        "tablename": "tbl_sedgrainsize_labbatch_data",
        "badrows": mismatch(sed_labbatch, grabeventdetails, sed_labbatch_grabevntdetails_shared_key), 
        "badcolumn": ','.join(sed_labbatch_grabevntdetails_shared_key),
        "error_type": "Logic Error",
        "error_message": 
            "Each record in sedgrainsize_labbatch_data must have a corresponding field record. You must submit the field data to the checker first. "+\
            "The Field template can be downloaded on "+\
            "<a href='/checker/templater?datatype=grab_field' target='_blank'>Field Template</a>. "
            "Records are matched based on these columns: {}".format(','.join(sed_labbatch_grabevntdetails_shared_key))
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: Each labbatch data must include corresponding data within session submission
    # Created Coder: Ayah
    # Created Date: 09/15/2021
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Ayah created logic check, has not tested yet
    # NOTE (10/05/2023): Aria revised the error message
    args.update({
        "dataframe": sed_labbatch,
        "tablename": "tbl_sedgrainsize_labbatch_data",
        "badrows": mismatch(sed_labbatch, sed_data, sed_data_sed_labbatch_shared_pkey), 
        "badcolumn": ','.join(sed_data_sed_labbatch_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in sedgrainsize_labbatch_data must have a corresponding record in sedgrainsize_data. "+\
            "Records are matched based on these columns: {}".format(','.join(sed_data_sed_labbatch_shared_pkey))
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")


    print("# CHECK - 3")
    # Description: Each data must include corresponding labbatch data within session submission
    # Created Coder: Ayah
    # Created Date: 09/15/2021
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Ayah created logic check, has not tested yet
    # NOTE (10/05/2023): Aria revised the error message
    args.update({
        "dataframe": sed_data,
        "tablename": "tbl_sedgrainsize_data",
        "badrows": mismatch(sed_data, sed_labbatch, sed_data_sed_labbatch_shared_pkey), 
        "badcolumn": ','.join(sed_data_sed_labbatch_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in sedgrainsize_data must have a corresponding record in sedgrainsize_labbatch_data. "+\
            "Records are matched based on these columns: {}".format(','.join(sed_data_sed_labbatch_shared_pkey))
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")    


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF SedGrainSize Logic Checks -------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################






    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Sedgrainsize LabBatch Checks ------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 4")
    # Description: Labreplicate must be consecutive within primary keys 
    # Created Coder: Aria Askarar
    # Created Date: 09/28/2023
    # Last Edited Date: 09/28/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/28/2023): Aria wrote the replicate check, it has not been tested.
    groupby_cols =  [x for x in sed_labbatch_pkey if x != 'labreplicate']
    args.update({
        "dataframe": sed_labbatch,
        "tablename": "tbl_sedgrainsize_labbatch_data",
        "badrows" : check_consecutiveness(sed_labbatch, groupby_cols, 'labreplicate'),
        "badcolumn": "labreplicate",
        "error_type": "Replicate Error",
        "error_message": f"labreplicate values must be consecutive. Records are grouped by {','.join(groupby_cols)}"
    })
    errs = [*errs, checkData(**args)]
    
    print("# END OF CHECK - 4")


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Sedgrainsize LabBatch Checks ----------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################








    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Sedgrainsize Data Checks ---------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################


    print("# CHECK - 5")
    # Description: Labreplicate must be consecutive within primary keys 
    # Created Coder: Aria Askarar
    # Created Date: 09/28/2023
    # Last Edited Date: 09/28/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/28/2023): Aria wrote the replicate check, it has not been tested. 
    groupby_cols = [x for x in sed_data_pkey if x != 'labreplicate']
    args.update({
        "dataframe": sed_data,
        "tablename": "tbl_sedgrainsize_data",
        "badrows" : check_consecutiveness(sed_data, groupby_cols, 'labreplicate'),
        "badcolumn": "labreplicate",
        "error_type": "Replicate Error",
        "error_message": f"labreplicate values must be consecutive. Records are grouped by {','.join(groupby_cols)}"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 5")


    print("# CHECK - 7")
    # Description: phi needs to be in the lookup list lu_sedgrainsize_phi
    # Created Coder: Duy Nguyen
    # Created Date: 7/11/24
    # Last Edited Date: 
    # NOTE (7/11/24): created check
    lu_sedgrainsize_phi = pd.read_sql("SELECT phi, wentworth_class FROM lu_sedgrainsize_phi",g.eng)
    args.update({
        "dataframe": sed_data,
        "tablename": "tbl_sedgrainsize_data",
        "badrows" : sed_data[~sed_data['phi'].isin(set(lu_sedgrainsize_phi['phi']))].tmp_row.tolist(),
        "badcolumn": "phi",
        "error_type": "Value Error",
        "error_message": 'The phi value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_sedgrainsize_phi target="_blank">lu_sedgrainsize_phi</a>'
    })
    errs = [*errs, checkData(**args)]
    
    print("# END OF CHECK - 7")

    print("# CHECK - 8")
    # Description: wentworth_class needs to be in the lookup list lu_sedgrainsize_phi. If the analyticalmethod is SM 2560 D, then it is best that you enter the value of phi and let the checker fills wentworth_class
    # Created Coder: Duy Nguyen
    # Created Date: 7/11/24
    # Last Edited Date: 
    # NOTE (7/11/24): created check
    args.update({
        "dataframe": sed_data,
        "tablename": "tbl_sedgrainsize_data",
        "badrows" : sed_data[~sed_data['wentworth_class'].isin(set(lu_sedgrainsize_phi['wentworth_class']))].tmp_row.tolist(),
        "badcolumn": "wentworth_class",
        "error_type": "Value Error",
        "error_message": 'The wentworth_class value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_sedgrainsize_phi target="_blank">lu_sedgrainsize_phi</a>\n.'
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 8")

    print("# CHECK - 9")
    # Description: The value of wentworth_class does not match the lookup list for the given phi
    # Created Coder: Duy Nguyen
    # Created Date: 7/11/24
    # Last Edited Date: 
    # NOTE (7/11/24): created check
    # Merging sed_data with lu_sedgrainsize_phi on 'phi'
    merged_data = sed_data.merge(lu_sedgrainsize_phi, how='left', on='phi', suffixes=('', '_lookup'))

    # Identifying rows with unmatched wentworth_class
    unmatched_rows = merged_data[
        (merged_data['phi'] != -88) &
        (merged_data['wentworth_class'] != merged_data['wentworth_class_lookup'])
    ]

    # Updating args with the results of the check
    args.update({
        "dataframe": sed_data,
        "tablename": "tbl_sedgrainsize_data",
        "badrows": unmatched_rows.tmp_row.tolist(),
        "badcolumn": "wentworth_class",
        "error_type": "Value Error",
        "error_message": 'The value of wentworth_class does not match the lookup list for the given phi. Refer to <a href=scraper?action=help&layer=lu_sedgrainsize_phi target="_blank">lu_sedgrainsize_phi</a>. If the analyticalmethod is SM 2560 D, then it is best that you put the value of phi and let the checker fills wentworth_class'
    })

    # Adding the check result to the list of errors
    errs = [*errs, checkData(**args)]


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Sedgrainsize Data Checks --------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    
    return {'errors': errs, 'warnings': warnings}