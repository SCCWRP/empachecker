

from inspect import currentframe
import pandas as pd
from pandas import DataFrame
from flask import current_app, g
from .functions import checkData, checkLogic, mismatch, get_primary_key
import re
import time

#define new function called 'bruvlab' for lab data dataset - 
#change to bruvmeta, need to make changes to the visual map check
def bruv_field(all_dfs):
    
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
    
    # These are the dataframes that got submitted for bruv
    print("define tables")
    bruvmeta = all_dfs['tbl_bruv_metadata']
    bruvmeta['tmp_row'] = bruvmeta.index

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    # Im just initializing the args dictionary
    
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
    # ------------------------------------------------ BRUV Meta Checks ------------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 4")
    # Description: Depth_m must be -88 or greater than or equal to 0
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 09/14/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Aria Askaryar adjusts the format so it follows the coding standard

    args.update({
        "dataframe": bruvmeta,
        "tablename": 'tbl_bruv_metadata',
        "badrows": bruvmeta[(bruvmeta['depth_m'] < 0) & (bruvmeta['depth_m'] != -88)].tmp_row.tolist(),
        "badcolumn": "depth_m",
        "error_type" : "Value out of range",
        "error_message" : "Depth measurement should not be a negative number, must be greater than 0."
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 4")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF BRUV Meta Checks ----------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    return {'errors': errs, 'warnings': warnings}




def bruv_lab(all_dfs):
    
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
    bruvdata = all_dfs['tbl_bruv_data']
    bruvvideo = all_dfs['tbl_bruv_videolog']
    bruvmeta = pd.read_sql("SELECT * FROM tbl_bruv_metadata;",g.eng)
    errs = []
    warnings = []
    
    bruvdata['tmp_row'] = bruvdata.index
    bruvvideo['tmp_row'] = bruvvideo.index
    bruvmeta['tmp_row'] = bruvmeta.index

    # read in samplecollectiondate as pandas datetime so merging bruv dfs (incl metadata) without dtype conflict
    bruvvideo['samplecollectiondate'] = pd.to_datetime(bruvvideo['samplecollectiondate'])
    bruvdata['samplecollectiondate'] = pd.to_datetime(bruvdata['samplecollectiondate'])
    bruvmeta['samplecollectiondate'] = pd.to_datetime(bruvmeta['samplecollectiondate'])

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

    bruvmeta_pkey = get_primary_key('tbl_bruv_metadata', g.eng)
    bruvdata_pkey = get_primary_key('tbl_bruv_data', g.eng)
    bruvvideo_pkey = get_primary_key('tbl_bruv_videolog', g.eng)

    bruvdata_bruvvideo_shared_pkey = [x for x in bruvdata_pkey if x in bruvvideo_pkey]
    bruvmeta_bruvdata_shared_pkey =  [x for x in bruvmeta_pkey if x in bruvdata_pkey]

    print("# CHECK - 1")
    # Description: Each data must include corresponding metadata record in the database
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 10/05/2023 
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard
    # NOTE (10/05/2023): Aria revised the error message

    args.update({
        "dataframe": bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": mismatch(bruvdata, bruvmeta, bruvmeta_bruvdata_shared_pkey), 
        "badcolumn": ','.join(bruvmeta_bruvdata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Records in tbl_bruv_data should have corresponding field records. "+\
            "The field data template can be found on <a href='/checker/templater?datatype=bruv_field' target='_blank'>BRUV Field Template</a>. "+\
            "Records are matched based on these columns: {}".format(','.join(bruvmeta_bruvdata_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: Each data must include corresponding videolog data
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 10/05/2023 
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard
    # NOTE (10/05/2023): Aria revised the error message

    args.update({
        "dataframe": bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": mismatch(bruvdata, bruvvideo, bruvdata_bruvvideo_shared_pkey), 
        "badcolumn": ','.join(bruvdata_bruvvideo_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each data (bruv_data) should have a corresponding record in videolog based on these fields {}".format(
            ','.join(bruvdata_bruvvideo_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")

    print("# CHECK - 3")
    # Description: Each videolog data must include corresponding data if fish is yes and bait is visible
    # Created Coder: Ayah Halabi
    # Created Date: NA
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Ayah wrote check 
    # NOTE (09/27/2023): Duy lowercased fish and bait column and compare against the lower case values 
    # NOTE (10/05/2023): Aria revised the error message

    bruvideo_filtered = bruvvideo[ 
        (bruvvideo['fish'].str.lower() == 'yes') &
        (bruvvideo['bait'].str.lower() == 'visible')
    ]
    print(bruvideo_filtered)
    args.update({
        "dataframe": bruvvideo,
        "tablename": "tbl_bruv_videolog",
        "badrows": mismatch(bruvideo_filtered, bruvdata, bruvdata_bruvvideo_shared_pkey), 
        "badcolumn": ','.join(bruvdata_bruvvideo_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Since Fish is 'Yes' and bait is 'visible', these records in bruv_videolog should have corresponding records in bruvdata on the following columns {}".format(
            ','.join(bruvdata_bruvvideo_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Logic Checks --------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################






    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ BRUV Data Checks ------------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 5")
    # Description: MaxNs must be a positive integer or left blank
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 09/27/2023 
    # Last Edited Coder: Duy Nguyen
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard
    # NOTE (09/27/2023): Duy added ~bruvdata['maxns'].isna() to the code
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[
            (~bruvdata['maxns'].isna()) &
            (bruvdata['maxns'] < 0)
        ].tmp_row.tolist(),
        "badcolumn": "maxns",
        "error_type": "Value Error",
        "error_message": "MaxNs must be a positive integer or left blank."
    }
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 5")

    print("# CHECK - 6")
    # Description: If in_out is "out" then both MaxN/MaxNs must be empty
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 09/12/2023 
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[
            (bruvdata['in_out'] == 'out') & 
            (
                (~bruvdata['maxn'].isna()) | (~bruvdata['maxns'].isna())
            )
        ].tmp_row.tolist(),
        "badcolumn": "in_out, maxn, maxns",
        "error_type": "Value Error",
        "error_message": "Invalid entry for MaxN/MaxNs column. Since in_out column is 'out', then both MaxN/MaxNs must be empty."
    }
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 6")

    print("# CHECK - 7")
    # Description: If in_out is "in" then both MaxN/MaxNs must have an integer value and cannot be left empty
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 09/12/2023 
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[
            (bruvdata['in_out'] == 'in') & 
            (
                (bruvdata['maxn'].isna()) | (bruvdata['maxns'].isna())
            )
        ].tmp_row.tolist(),
        "badcolumn": "in_out, maxn, maxns",
        "error_type": "Value Error",
        "error_message": "Invalid entry for MaxN/MaxNs column. Since in_out column is 'in', then both MaxN/MaxNs must have an integer value and cannot be left empty."
    }
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 7")

    print("# CHECK - 8")
    # Description: If in_out is "in" then MaxN is the sum of MaxNs (MaxN is the sum of all/each species counts (MaxNs) within a given FOV frame)
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 09/19/2023 
    # Last Edited Coder: Robert Butler
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard

    # NOTE (09/19/2023): The tmp_row column should never be dropped from the dataframe. 
    #                    If one needs to do more complicated operations with the data they need to make a copy of it
    #                    This is so we can correctly mark the original row numbers of the dataframe
    
    # grouped_cols = ['siteid','estuaryname','stationno','samplecollectiondate','camerareplicate','foventeredtime','fovlefttime','in_out'] 
    cols = ['siteid','estuaryname','stationno','samplecollectiondate','camerareplicate','videoorder','foventeredtime','fovlefttime','in_out'] 
    
    #subsetting for 'in_out' == 'in' so that there are fewer keys to loop through
    grouped_df = bruvdata[bruvdata['in_out'] == 'in'].groupby(cols) 
    gb = grouped_df.groups
    key_list_from_gb = gb.keys()
    badrows = [] #initialized to append
    for key, values in gb.items():
        if key in key_list_from_gb: #this is by unique group
            tmp = bruvdata.loc[values]
            brows = tmp[(tmp['maxn'] != tmp['maxns'].sum())].tmp_row.tolist()
            badrows.extend(brows) #this will be populated to the badrows key in the args dict
    

    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": badrows,
        "badcolumn": "maxns, maxn",
        "error_type": "Value Error",
        "error_message": "MaxN is the sum of all/each species counts (MaxNs) within a given frame. Each record with the same FOV frame should have the same value for MaxN."
    }
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 8")


    print("# CHECK - 9")
    # Description: If in_out is "in" then certainty cannot be left empty
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 09/27/2023 
    # Last Edited Coder: Duy Nguyen
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard
    # NOTE (09/12/2023): Aria - i am not sure wh the badcolumns are checking all those keys? Shouldnt it jus be in_out and certainty
    # NOTE (09/27/2023 ): Duy commented out since certainty is a required field.

    # args = {
    #     "dataframe": bruvdata,
    #     "tablename": 'tbl_bruv_data',
    #     "badrows": bruvdata[(bruvdata['in_out'] == 'in') & (pd.isnull(bruvdata['certainty']))].tmp_row.tolist(),
    #     "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate",
    #     "error_type": "Value Error",
    #     "error_message": "Invalid entry for certainty column. Since in_out column is 'in', then certainty must have a nonempty value."
    # }
    # errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 9")

    print("# CHECK - 10")
    # Description: maxn needed to be in [0,1000]
    # Created Coder: Duy
    # Created Date: 9/27/23
    # Last Edited Date:  
    # Last Edited Coder: 
    # NOTE (9/27/23): Check created by Duy 

    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[
            (bruvdata['maxn'] <= 0) | 
            (bruvdata['maxn'] >= 1000)    
        ].tmp_row.tolist(),
        "badcolumn": "maxn",
        "error_type": "Value Error",
        "error_message": "values in maxn needed to be in [0,1000]"
    }
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 10")




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF BRUV Data Checks ----------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################






    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ BRUV Videolog Checks -------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF BRUV Videolog Checks ------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    return {'errors': errs, 'warnings': warnings}


