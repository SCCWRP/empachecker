# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import datetime as dt
import pandas as pd
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
    felddata_feldmeta_shared_pkey = list(set(felddata_pkey).intersection(set(feldmeta_pkey)))

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
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    feldmeta_filter = feldmeta[feldmeta['plug_extracted'] == 'Yes']
    
    args = {
        "dataframe": feldmeta,
        "tablename": 'tbl_feldspar_metadata',
        "badrows": mismatch(feldmeta_filter, felddata, felddata_feldmeta_shared_pkey),
        "badcolumn": ','.join(felddata_feldmeta_shared_pkey),
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "Since plug_extracted = yes, metadata must have corresponding records in Feldspar Data. Records are matched based on the columns listed in the Column(s) box."
        
    }
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: Each record in feldspar_data must have a corresponding record in feldspar_metadata
    # Created Coder: Aria 
    # Created Date: NA
    # Last Edited Date: 9/28/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (9/28/2023): Check was changed so the code now matched the updated check
    print(f"these are the share pkeys {','.join(felddata_feldmeta_shared_pkey)}")
    args.update({
        "dataframe": felddata,
        "tablename": "tbl_feldspar_data",
        "badrows": mismatch(felddata,feldmeta,felddata_feldmeta_shared_pkey), 
        "badcolumn": ','.join(felddata_feldmeta_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each Feldspar data  must have corresponding records in Feldspar Metadata. Records are matched based on the columns listed in the Column(s) box."
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





    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Feldspar Data Checks ------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    '''
    ############################### --Start of Logic Checks -- #############################################################
    print("Begin fieldspar Logic Checks...")

    #check 1: Each data must include corresponding sample metadata
    print('Start logic check 1')
    eng = g.eng
    sql = eng.execute("SELECT *  FROM tbl_feldspar_metadata ")
    db_feldmeta = pd.DataFrame(sql.fetchall())
    db_feldmeta.columns = sql.keys()
    groupcols = ['siteid','estuaryname', 'samplecollectiondate','stationno','projectid']
    args = {
        "dataframe": felddata,
        "tablename": 'tbl_feldspar_data',
        "badrows": mismatch(felddata,db_feldmeta,groupcols),
        "badcolumn": "siteid, estuaryname, samplecollectiondate, stationno,projectid ",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Records in tbl_feldspar_metadata must have corresponding records in tnl_feldspar_data."
    }
    errs = [*errs, checkData(**args)]
    print('check ')
    print('End of logic check 1')
    print("END fieldspar Logic Checks...")
    ############################### --End of Logic Checks -- #############################################################


    ############################### --Start of Custom Checks -- #############################################################
    print("Begin fieldspar custom Checks...")
    #Check 2: Siteid/Estuaryname  pair must match lookup list (multicolumn check)
    print('check 2 begin feldspar:')
    lookup_sql = f"SELECT * FROM lu_siteid;"
    lu_siteid = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['siteid','estuaryname']
    lookup_cols = ['siteid','estuary']
    args = {
        "dataframe": felddata,
        "tablename": 'tbl_feldspar_data',
        "badrows": multicol_lookup_check(felddata,lu_siteid,check_cols,lookup_cols),
        "badcolumn": "siteid, estuaryname, samplecollectiondate, stationno,projectid ",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": f'The siteid/estuaryname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_siteid" '
                        'target="_blank">lu_siteid</a>'
    }

    #check 3: if plug_extracted = yes then corresponding data
    print('Start check 3')
    feldmeta_filter = feldmeta[feldmeta['plug_extracted'] == 'yes']

    groupcols = ['siteid','estuaryname', 'samplecollectiondate','stationno','projectid']
    args = {
        "dataframe": feldmeta,
        "tablename": 'tbl_feldspar_metadata',
        "badrows": mismatch(feldmeta_filter, felddata, groupcols),
        "badcolumn": "plug_extracted ",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "if plug_extracted = yes then corresponding data"
    }
    errs = [*errs, checkData(**args)]
    print('End of check 3 ')

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

     # likely deleting this custom check warning
    args.update({
        "dataframe": felddata,
        "tablename": "tbl_feldspar_data",
        "badrows":felddata['samplecollectiondate'] == felddata['samplecollectiondate'].dt.date.index.tolist(),
        "badcolumn": "samplecollectiondate",
        "error_type" : "Date Value out of range",
        "error_message" : "Your Date format is not correct, must be YYYY-MM-DD."
    })
    errs = [*warnings, checkData(**args)]
    
    args.update({
        "dataframe": feldmeta,
        "tablename": "tbl_feldspar_metadata",
        "badrows":feldmeta['samplecollectiondate'] == feldmeta['samplecollectiondate'].dt.date.index.tolist(),
        "badcolumn": "samplecollectiondate",
        "error_type" : "Date Value out of range",
        "error_message" : "Your Date format is not correct, must be YYYY-MM-DD."
    })
    errs = [*warnings, checkData(**args)]

    print("END fieldspar Logic Checks...")
    ############################### --End of Logic Checks -- #############################################################
    '''
    return {'errors': errs, 'warnings': warnings}