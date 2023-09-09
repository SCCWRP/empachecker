# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from datetime import date
from .functions import checkData,get_primary_key, mismatch, multicol_lookup_check

def edna_lab(all_dfs):
    
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

    # These are the dataframes that got submitted for edna

    ednased = all_dfs['tbl_edna_sed_labbatch_data']
    ednawater = all_dfs['tbl_edna_water_labbatch_data']
    ednadata= all_dfs['tbl_edna_data']

    
    ednased['tmp_row'] = ednased.index
    ednawater['tmp_row'] = ednawater.index
    ednadata['tmp_row'] = ednadata.index

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename":'',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- EDNA Logic Checks ---------------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    print("# CHECK - 1")
    #Description: Each labbatch data must correspond to grabeventdetails in database
    # Created Coder:Ayah H
    # Created Date: 09/08/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/08-2023): Ayah wrote check
    eng = g.eng
    sql = eng.execute("SELECT * FROM tbl_grabevent_details")
    sql_df = DataFrame(sql.fetchall())
    sql_df.columns = sql.keys()
    grabevent  = sql_df
    del sql_df
    

    #print("# END OF CHECK - 1")
    
    
    
    #print("# CHECK - 2")
    #Description: Each data must correspond to grabeventdetails in database
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 2")
    
    
    
    #print("# CHECK - 3")
    #Description: Each labbatch data must correspond to data within submission
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 3")
    
    
    
    #print("# CHECK - 4")
    #Description: Each data must correspond to labbatch data within submission
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 4")



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END OF EDNA Logic Checks --------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- Sed Labbatch Data Checks --------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 5")
    #Description: Samplecollectiondate should be before preparationdate
    #Created Coder:
    #Created Date:
    #Last Edited Date: 
    #Last Edited Coder: 
    #NOTE (Date):
    #print("# END OF CHECK - 5")
    
    
    #print("# CHECK - 6")
    #Description: Preparationtime should be before samplecollectiontime
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 6")
    
    
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END OF Sed Labbatch Data Checks -------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    
    
    
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- Water Labbatch Data Checks ------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 7")
    # Description: Preparationtime should be before samplecollectiontime
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 7")

    #print("# CHECK - 8")
    # Description: Preparationdate should be before samplecollectiondate
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 8")

    
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END OF Water Labbatch Data Checks -----------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    
    
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- Edna Data Checks ----------------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 9")
    #Description: labreplicate must be consecutive within primary keys
    # Created Coder: 
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 9")




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END OF Edna Data Checks ---------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

























    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]
    '''
    ############################### --Start of sed labbatch data checks -- #############################################################
    print("Begin eDNA lab checks...")
    #check 1: Samplecollectiondate should be before preparationdate
    print('begin edna-custom-check 1')
    args.update({
        "dataframe": ednased,
        "tablename": "tbl_edna_sed_labbatch_data",
        "badrows": ednased[ednased.preparationdate.apply(pd.Timestamp) > ednased.samplecollectiondate.apply(pd.Timestamp)].tmp_row.tolist(),
        "badcolumn": "preparationdate,samplecollectiondate",
        "error_type" : "Value out of range",
        "error_message" : "Your Collection date should be before your preparation date."
    })
    errs = [*warnings, checkData(**args)]
    print('end labbatch-custom-check 1')

    
    #check 2: Preparationtime should be before samplecollectiontime
    print("begin edna-custom-check 2")
    args.update({
        "dataframe": ednased,
        "tablename": "tbl_edna_sed_labbatch_data",
        "badrows": ednased[ednased.preparationtime.apply(pd.Timestamp) > ednased.samplecollectiontime.apply(pd.Timestamp)].tmp_row.tolist(),
        "badcolumn": "preparationtime, collectiontime",
        "error_type" : "Value out of range",
        "error_message" : "Your preparation time should be before collection time."
    })
    errs = [*errs, checkData(**args)]
    print("end edna-custom-check 2")

    #check 3: Preparationtime should be before samplecollectiontime
    print("begin edna-custom-check 3")
    args.update({
        "dataframe": ednawater,
        "tablename": "tbl_edna_water_labbatch_data",
        "badrows": ednawater[ednawater.preparationtime.apply(pd.Timestamp) > ednawater.samplecollectiontime.apply(pd.Timestamp)].tmp_row.tolist(),
        "badcolumn": "preparationtime,samplecollectiontime",
        "error_type" : "Value out of range",
        "error_message" : "Your preparation time should be before your collection time"
    })
    errs = [*errs,checkData(**args)]
    print("end edna-custom-check 3")

    #check 4: Preparationdate should be before samplecollectiondate
    print("begin edna-custom-check 4")
    args.update({
        "dataframe": ednawater,
        "tablename": "tbl_edna_water_labbatch_data",
        "badrows": ednawater[ednawater.preparationdate.apply(pd.Timestamp) > ednawater.samplecollectiondate.apply(pd.Timestamp)].tmp_row.tolist(),
        "badcolumn": "preparationdate,samplecollectiondate",
        "error_type" : "Value out of range",
        "error_message" : "Your preparation date should be before your collection date."
    })
    errs = [*warnings, checkData(**args)]
    print("end edna-custom-check 4")
    '''
    return {'errors': errs, 'warnings': warnings}
