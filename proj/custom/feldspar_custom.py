# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import datetime as dt
import pandas as pd
import numpy as np
from datetime import date
from .functions import checkData, mismatch, multicol_lookup_check, get_primary_key
def feldspar(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    lu_list_script_root = current_app.script_root
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

    felddata = all_dfs['tbl_feldspar_data']
    feldmeta = all_dfs['tbl_feldspar_metadata']

    felddata['tmp_row'] = felddata.index
    feldmeta['tmp_row'] = feldmeta.index

    felddata_pkey = get_primary_key('tbl_feldspar_data',g.eng)
    feldmeta_pkey = get_primary_key('tbl_feldspar_metadata',g.eng)
    felddata_feldmeta_shared_pkey = [x for x in felddata_pkey if x in feldmeta_pkey]

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    
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
    # ------------------------------------------------ Logic Checks ---------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 1")
    # Description: if plug_extracted = yes then corresponding data
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/05/2023 
    # Last Edited Coder: Aria Askaryar
    # NOTE (10/05/2023): Aria revised the error message
    feldmeta_filter = feldmeta[feldmeta['plug_extracted'] == 'Yes']
    
    args = {
        "dataframe": feldmeta,
        "tablename": 'tbl_feldspar_metadata',
        "badrows": mismatch(feldmeta_filter, felddata, felddata_feldmeta_shared_pkey),
        "badcolumn": ','.join(felddata_feldmeta_shared_pkey),
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "Since plug_extracted = yes, metadata must have corresponding records in Feldspar Data.  Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(felddata_feldmeta_shared_pkey)
        )
    }
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: Each record in feldspar_data must have a corresponding record in feldspar_metadata
    # Created Coder: Aria 
    # Created Date: NA
    # Last Edited Date: 10/05/2023 
    # Last Edited Coder: Aria Askaryar
    # NOTE (9/28/2023): Check was changed so the code now matched the updated check
    # NOTE (10/05/2023): Aria revised the error message
    args.update({
        "dataframe": felddata,
        "tablename": "tbl_feldspar_data",
        "badrows": mismatch(felddata,feldmeta,felddata_feldmeta_shared_pkey), 
        "badcolumn": ','.join(felddata_feldmeta_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each Feldspar data  must have corresponding records in Feldspar Metadata.  Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(felddata_feldmeta_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------END OF Logic Checks ---------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################






    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Feldspar Meta Checks -------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
 
    


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Feldspar Meta Checks ------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################






    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Feldspar Data Checks -------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 3")
    # Description: average field needed to be the average of sideone,sidetwo,sidethree,sidefour excluding -88 values
    # Created Coder: Duy Nguyen
    # Created Date: 10/02/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (10/02/2023): Check created by Duy. QA'ed
    badrows = felddata[
        felddata.apply(
            lambda row: row['average'] != np.nanmean(
                [
                    row[side_no]
                    for side_no in ['sideone','sidetwo','sidethree','sidefour']
                    if row[side_no] != -88
                ]
            ),
            axis=1
        )
    ].tmp_row.tolist()
    args.update({
        "dataframe": felddata,
        "tablename": "tbl_feldspar_data",
        "badrows": badrows, 
        "badcolumn": 'average',
        "error_type": "Value Error",
        "error_message": "average field needed to be the average of sideone,sidetwo,sidethree,sidefour excluding -88 values"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Feldspar Data Checks ------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    return {'errors': errs, 'warnings': warnings}