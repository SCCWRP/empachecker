from inspect import currentframe
from flask import current_app, g
from pandas import DataFrame
import pandas as pd
from .functions import checkData, checkLogic, mismatch,get_primary_key
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
    grabeventdetails = pd.read_sql("SELECT * FROM tbl_grabevent_details", g.eng)

    sed_data_pkey = get_primary_key('tbl_sedgrainsize_data', g.eng)
    sed_labbatch_pkey = get_primary_key('tbl_sedgrainsize_labbatch_data', g.eng)
    grabeventdetails_pkey = get_primary_key('tbl_grabevent_details', g.eng)

    sed_labbatch_grabevntdetails_shared_pkey = list(set(sed_labbatch_pkey).intersection(set(grabeventdetails_pkey)))
    sed_labbatch_grabevntdetails_shared_key = list(set(sed_data_pkey).intersection(set(grabeventdetails_pkey)))
    sed_data_sed_labbatch_shared_pkey = list(set(sed_data_pkey).intersection(set(sed_labbatch_pkey)))


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
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/15/2021): Ayah wrote logic check
    args.update({
        "dataframe": sed_labbatch,
        "tablename": "tbl_sedgrainsize_labbatch_data",
        "badrows": mismatch(sed_labbatch, grabeventdetails, sed_labbatch_grabevntdetails_shared_key), 
        "badcolumn": ','.join(sed_labbatch_grabevntdetails_shared_key),
        "error_type": "Logic Error",
        "error_message": "Each labbatch data must have corresponding records in the grabeventdetails table based on the columns: {}".format(
            ','.join(sed_labbatch_grabevntdetails_shared_key)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: Each labbatch data must include corresponding data within session submission
    # Created Coder: Ayah
    # Created Date: 09/15/2021
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/15/2021): Ayah wrote logic check
    args.update({
        "dataframe": sed_labbatch,
        "tablename": "tbl_sedgrainsize_data",
        "badrows": mismatch(sed_labbatch, sed_data, sed_data_sed_labbatch_shared_pkey), 
        "badcolumn": ','.join(sed_data_sed_labbatch_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each labbatch data must have corresponding records in the data table based on the columns: {}".format(
            ','.join(sed_data_sed_labbatch_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")


    print("# CHECK - 3")
    # Description: Each data must include corresponding labbatch data within session submission
    # Created Coder: Ayah
    # Created Date: 09/15/2021
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/15/2021): Ayah wrote logic check
    args.update({
        "dataframe": sed_data,
        "tablename": "tbl_sedgrainsize_data",
        "badrows": mismatch(sed_data, sed_labbatch, sed_data_sed_labbatch_shared_pkey), 
        "badcolumn": ','.join(sed_data_sed_labbatch_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each  data must have corresponding records in the labbatch data table based on the columns: {}".format(
            ','.join(sed_data_sed_labbatch_shared_pkey)
        )
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

    #print("# CHECK - 4")
    # Description: Labreplicate must be consecutive within primary keys 
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 4")


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


    #print("# CHECK - 5")
    # Description: Labreplicate must be consecutive within primary keys 
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - ")




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Sedgrainsize Data Checks --------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################



    '''
    # Logic Checks
    eng = g.eng
    sql = eng.execute("SELECT * FROM tbl_sedgrainsize_metadata")
    sql_df = DataFrame(sql.fetchall())
    sql_df.columns = sql.keys()
    sedmeta = sql_df
    del sql_df

    ############################### --Start of Logic Checks -- #############################################################
    print("Begin Sediment Grain Size Lab Logic Checks...")

    # Logic Check 1: Siteid/estuaryname pair must match lookup list -- sedimentgrainsize_metadata (db) & sediment_labbatch_data (submission), sedimentgrainsize_metadata records do not exist in database
    print("begin check 1")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'matrix', 'samplelocation','projectid']
    args = {
        "dataframe": sed_labbatch,
        "tablename": 'tbl_sedgrainsize_labbatch_data',
        "badrows": mismatch(sed_labbatch, sedmeta, groupcols),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, matrix, samplelocation",
        "error_type": "Logic Error",
        "error_message": "Field submission for sediment grain size labbatch data is missing. Please verify that the sediment grain size field data has been previously submitted."
    }
    errs = [*errs, checkData(**args)]
    print("check ran - logic - sediment grain size metadata records do not exist in database for sediment grain size labbatch data submission")
    print("end check 1")

    # Logic Check 2: sedgrainsize_labbatch_data & sedgrainsize_data must have corresponding records within session submission
    # Logic Check 2a: sedgrainsize_data missing records provided by sedgrainsize_labbatch_data
    print("begin check 2a")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplelocation', 'preparationbatchid', 'labreplicate', 'matrix', 'projectid']
    args.update({
        "dataframe": sed_labbatch,
        "tablename": "tbl_sedgrainsize_labbatch_data",
        "badrows": mismatch(sed_labbatch, sed_data, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplelocation, preparationbatchid, labreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in sedimentgrainsize_labbatch_data must have corresponding records in sedgrainsize_data. Missing records in sedgrainsize_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - missing sedgrainsize_data records")
    print("end check 2a")

    # Logic Check 2b: sedgrainsize_labbatch_data missing records provided by sedgrainsize_data
    print("begin check 2b")
    tmp = sed_data.merge(
        sed_labbatch.assign(present = 'yes'), 
        on = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplelocation', 'preparationbatchid', 'matrix', 'labreplicate', 'projectid'],
        how = 'left'
    )
    # badrows = tmp[pd.isnull(tmp.present)].tmp_row.tolist()
    args.update({
        "dataframe": sed_data,
        "tablename": "tbl_sedgrainsize_data",
        "badrows": tmp[pd.isnull(tmp.present)].tmp_row.tolist(),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplelocation, preparationbatchid, matrix, labreplicate, 'projectid",
        "error_type": "Logic Error",
        "error_message": "Records in sedgrainsize_data must have corresponding records in sedgrainsize_labbatch_data. Missing records in sedgrainsize_labbatch_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - missing sedgrainsize_labbatch_data records")
    print("begin check 2b")
    print("End Sediment Grain Size Lab Logic Checks...")
    '''
    
    return {'errors': errs, 'warnings': warnings}