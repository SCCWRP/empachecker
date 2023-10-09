# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from pandas import DataFrame
from .functions import checkData, checkLogic, mismatch, get_primary_key, check_consecutiveness

def sedchem_lab(all_dfs):
    
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

    sedlabbatch = all_dfs['tbl_sedchem_labbatch_data']
    seddata = all_dfs['tbl_sedchem_data']
    grabeventdetails = pd.read_sql("SELECT * FROM tbl_grabevent_details", g.eng)

    sedlabbatch['tmp_row'] = sedlabbatch.index
    seddata['tmp_row'] = seddata.index
    grabeventdetails['tmp_row'] = grabeventdetails.index

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
    # ------------------------------------------------ SedChemLab Logic Checks ----------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    sedlabbatch_pkey = get_primary_key('tbl_sedchem_labbatch_data', g.eng)
    seddata_pkey = get_primary_key('tbl_sedchem_data', g.eng)
    grabeventdetails_pkey = get_primary_key('tbl_grabevent_details', g.eng)

    sedlabbatch_seddata_shared_pkey = [x for x in sedlabbatch_pkey if x in seddata_pkey]
    sedlabbatch_grabeventdetails_shared_pkey = [x for x in sedlabbatch_pkey if x in grabeventdetails_pkey]

    
    print("# CHECK - 1")
    # Description: Each labbatch data must correspond to grabeventdetails in database based on their shared pkeys (🛑 ERROR 🛑)
    # Created Coder: Ayah 
    # Created Date: 09/12/2023
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Ayah created logic check, has not tested yet
    # NOTE (10/05/2023): Aria revised the error message

    args.update({
        "dataframe": sedlabbatch,
        "tablename": "tbl_sedchem_labbatch_data",
        "badrows": mismatch(sedlabbatch, grabeventdetails, sedlabbatch_grabeventdetails_shared_pkey), 
        "badcolumn": ','.join(sedlabbatch_grabeventdetails_shared_pkey),
        "error_type": "Logic Error",
        "error_message": 
            "Each record in sedchem_labbatch_data must have a corresponding field record. You must submit the field data to the checker first. "+\
            "The Field template can be downloaded on "+\
            "<a href='/checker/templater?datatype=grab_field' target='_blank'>Field Template</a>. "
            "Records are matched based on these columns: {}".format(','.join(sedlabbatch_grabeventdetails_shared_pkey))
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 1")
    
    
    
    
    print("# CHECK - 2")
    # Description: Each labbatch data must include corresponding data within session submission (🛑 ERROR 🛑)
    # Created Coder: Ayah 
    # Created Date: 09/12/2023
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Ayah created logic check, has not tested yet
    # NOTE (10/05/2023): Aria revised the error message 

    args.update({
        "dataframe": sedlabbatch,
        "tablename": "tbl_sedchem_labbatch_data",
        "badrows": mismatch(sedlabbatch, seddata, sedlabbatch_seddata_shared_pkey), 
        "badcolumn": ','.join(sedlabbatch_seddata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in sedchem_labbatch_data must have a corresponding record in sedchem_data. "+\
            "Records are matched based on these columns: {}".format(','.join(sedlabbatch_seddata_shared_pkey))
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 2")

    print("# CHECK - 3")
    # Description: Each data must include corresponding labbatch data within session submission (🛑 ERROR 🛑)
    # Created Coder: Ayah
    # Created Date: 09/12/2023
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Ayah created logic check, has not tested yet
    # NOTE (10/05/2023): Aria revised the error message

    args.update({
        "dataframe": seddata,
        "tablename": "tbl_sedchem_data",
        "badrows": mismatch(seddata,sedlabbatch,sedlabbatch_seddata_shared_pkey), 
        "badcolumn": ','.join(sedlabbatch_seddata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in sedchem_data must have a corresponding record in sedchem_labbatch_data. "+\
            "Please submit the metadata for these records first based on these columns: {}".format(','.join(sedlabbatch_seddata_shared_pkey))
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 3")






    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF SedChemLab Logic Checks ---------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################




    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------ SedChemLabBatch Checks ---------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
    print("# CHECK - 4")
    # Description: samplereplicate must be consecutive within primary keys (🛑 ERROR 🛑)
    # Created Coder: Ayah
    # Created Date: 09/12/2023
    # Last Edited Date: 09/18/23
    # Last Edited Coder: Duy
    # NOTE (09/12/2023): I made the variable "seddata_pkey_norepcol" because the error message must output all the pkey columns except for replicate ones
    # NOTE (09/18/23): Duy made a function to check for consecutive values
    groupby_cols = [x for x in sedlabbatch_pkey if x != 'samplereplicate']
    args.update({
        "dataframe": sedlabbatch,
        "tablename": "tbl_sedchem_labbatch_data",
        "badrows" : check_consecutiveness(sedlabbatch, groupby_cols, 'samplereplicate'),
        "badcolumn": "samplereplicate",
        "error_type": "Replicate Error",
        "error_message": f"samplereplicate values must be consecutive. Records are grouped by {','.join(groupby_cols)}"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 4") 
    
    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ---------------------------------------------------END OF SedChemLabBatch Checks --------------------------------------------------#
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
    

    
    
    
    
    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------ SedChemData Checks -------------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
    print("# CHECK - 5")
    # Description: If there is a value in result column, mdl cannot be empty (🛑 ERROR 🛑)
    # Created Coder: Ayah
    # Created Date: 09/12/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/12/2023): Ayah wrote the check, it has not been tested yet
    
    args.update({
        "dataframe": seddata,
        "tablename": "tbl_sedchem_data",
        "badrows" : seddata[
            (seddata['results'].notna()) & 
            (seddata['mdl'].isna())
        ].tmp_row.tolist(),
        "badcolumn": "mdl",
        "error_type": "Empty Value Error",
        "error_message": f"If there is a value in result column, mdl cannot be empty"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 5")       


    #print("# CHECK - 6")
    # Description: labreplicate must be consecutive within primary keys (🛑 ERROR 🛑)
    # Created Coder: Ayah
    # Created Date: 09/12/2023
    # Last Edited Date: 10/09/23
    # Last Edited Coder: Duy
    # NOTE (09/12/2023): Ayah wrote the replicate check, it has not been tested. 
    # NOTE (09/12/2023): I made the variable "seddata_pkey_norepcol" because the error message must output all the pkeys except for any rep columns
    # NOTE (09/18/23): Duy made a function to check for consecutive values
    # NOTE (10/09/23): Commented this out. See Accuracy Checklist for reason why.
    # groupby_cols = [
    #     'projectid','preparationbatchid','siteid','estuaryname','stationno','samplecollectiondate','matrix','sampletype',
    #     'samplereplicate','sampleid'
    # ]
    # args.update({
    #     "dataframe": seddata,
    #     "tablename": "tbl_sedchem_data",
    #     "badrows" : check_consecutiveness(seddata, groupby_cols, 'labreplicate'),
    #     "badcolumn": "labreplicate",
    #     "error_type": "Replicate Error",
    #     "error_message": f"labreplicate values must be consecutive. Records are grouped by {','.join(groupby_cols)}"
    # })
    # errs = [*errs, checkData(**args)]
    #print("# END OF CHECK - 6")       
    
    print("# CHECK - 7")
    # Description: samplereplicate must be consecutive within primary keys (🛑 ERROR 🛑)
    # Created Coder: Ayah
    # Created Date: 09/12/2023
    # Last Edited Date: 09/18/23
    # Last Edited Coder: Duy
    # NOTE (09/12/2023): Ayah wrote the replicate check, it has not been tested. 
    # NOTE (09/12/2023): I made the variable "seddata_pkey_norepcol" because the error message must output all the pkey columns except for replicate ones
    # NOTE (09/18/23): Duy made a function to check for consecutive values
    groupby_cols = [x for x in seddata_pkey if x != 'samplereplicate']
    args.update({
        "dataframe": seddata,
        "tablename": "tbl_sedchem_data",
        "badrows" : check_consecutiveness(seddata, groupby_cols, 'samplereplicate'),
        "badcolumn": "samplereplicate",
        "error_type": "Replicate Error",
        "error_message": f"samplereplicate values must be consecutive. Records are grouped by {','.join(groupby_cols)}"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 7")
       

    
    
    
    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ---------------------------------------------------END OF SedChemData Checks -------------------------------------------------------#
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################


    return {'errors': errs, 'warnings': warnings}