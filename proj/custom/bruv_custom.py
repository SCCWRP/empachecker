

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

    print("did it break here?")
    protocol = all_dfs['tbl_protocol_metadata']
    bruvdata = all_dfs['tbl_bruv_data']
    bruvvideo = all_dfs['tbl_bruv_videolog']
    errs = []
    warnings = []
    
    protocol['tmp_row'] = protocol.index
    bruvdata['tmp_row'] = bruvdata.index
    bruvvideo['tmp_row'] = bruvvideo.index

    # read in samplecollectiondate as pandas datetime so merging bruv dfs (incl metadata) without dtype conflict
    bruvvideo['samplecollectiondate'] = pd.to_datetime(bruvvideo['samplecollectiondate'])
    bruvdata['samplecollectiondate'] = pd.to_datetime(bruvdata['samplecollectiondate'])

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
    print("Potential error here 1:")
    protocol_pkey = get_primary_key('tbl_protocol_metadata', g.eng)
    bruvdata_pkey = get_primary_key('tbl_bruv_data', g.eng)
    bruvvideo_pkey = get_primary_key('tbl_bruv_videolog', g.eng)
    print("Potential error here 2:")
    bruvdata_protocol_shared_pkey = list(set(bruvdata_pkey).intersection(set(protocol_pkey)))
    bruvdata_bruvvideo_shared_pkey = list(set(bruvdata_pkey).intersection(set(bruvvideo_pkey)))
    bruvvideo_bruvdata_shared_pkey = list(set(bruvvideo_pkey).intersection(set(bruvdata_pkey)))

    print("# CHECK - 1")
    # Description: Each data must include corresponding metadata record in the database
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 09/12/2023 
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard

    args.update({
        "dataframe": bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": mismatch(bruvdata, protocol, bruvdata_protocol_shared_pkey), 
        "badcolumn":  ','.join(bruvdata_protocol_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Records in tbl_bruv_data should have corresponding records in bruv_metadata in the database. Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(bruvdata_protocol_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: Each data must include corresponding videolog data
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 09/12/2023 
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard

    args.update({
        "dataframe": bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": mismatch(bruvdata, bruvvideo, bruvdata_bruvvideo_shared_pkey), 
        "badcolumn":  ','.join(bruvdata_bruvvideo_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each data (bruv_data) should have a corresponding record in videolog based on these fields {}".format(
            ','.join(bruvdata_bruvvideo_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")

    print("# CHECK - 3")
    # Description: Each videolog data must include corresponding data
    # Created Coder: Aria Askaryar
    # Created Date: NA
    # Last Edited Date: 09/12/2023 
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard

    args.update({
        "dataframe": bruvvideo,
        "tablename": "tbl_bruv_videolog",
        "badrows": mismatch( bruvvideo, bruvdata, bruvvideo_bruvdata_shared_pkey), 
        "badcolumn":  ','.join(bruvvideo_bruvdata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Records in bruv_videolog should have corresponding records in bruv_metadata. Please submit the metadata for these records first {}".format(
            ','.join(bruvvideo_bruvdata_shared_pkey)
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
    # Last Edited Date: 09/12/2023 
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[bruvdata['maxns'] < 0].tmp_row.tolist(),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate",
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
            (~bruvdata['scientificname'].isin(['unidentifiable fish','unknown juvenile fish','unknown crab'])) &
            (bruvdata['in_out'] == 'out') & 
            ((~bruvdata['maxn'].isna()) | (~bruvdata['maxns'].isna()))].tmp_row.tolist(),
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
            (~bruvdata['scientificname'].isin(['unidentifiable fish','unknown juvenile fish','unknown crab'])) &
            (bruvdata['in_out'] == 'in') & 
            ((bruvdata['maxn'].isna()) | (bruvdata['maxns'].isna()))
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
    # Last Edited Date: 09/12/2023 
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Aria adjusts the format so it follows the coding standard
    # NOTE (09/12/2023): Aria - i am not sure wh the badcolumns are checking all those keys? Shouldnt it jus be in_out and certainty

    print("bruvdata")
    print(bruvdata)
    
    print("""bruvdata[(bruvdata['in_out'] == 'in') & (pd.isnull(bruvdata['certainty']))]""")
    print(bruvdata[(bruvdata['in_out'] == 'in') & (pd.isnull(bruvdata['certainty']))])

    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[(bruvdata['in_out'] == 'in') & (pd.isnull(bruvdata['certainty']))].tmp_row.tolist(),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate",
        "error_type": "Value Error",
        "error_message": "Invalid entry for certainty column. Since in_out column is 'in', then certainty must have a nonempty value."
    }
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 9")




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















    '''
    ##################################### CUSTOM CHECKS ######################################################
    # Custom Checks
    # Check 1: bruv_videolog MaxNs col must be a postive integer 
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[bruvdata['maxns'] < 0].tmp_row.tolist(),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate",
        "error_type": "Value Error",
        "error_message": "MaxNs must be a positive integer or left blank."
    }
    errs = [*errs, checkData(**args)]
    print("check ran - logic - bruv metadata records do not exist in database for bruv lab submission") #tested

    # Check 4: if in_out = 'out', maxn & maxns MUST be NULL
    print('begin check 4')
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[
            (~bruvdata['scientificname'].isin(['unknown fish','unknown juvenile fish','unknown crab'])) &
            (bruvdata['in_out'] == 'out') & 
            ((~bruvdata['maxn'].isna()) | (~bruvdata['maxns'].isna()))].tmp_row.tolist(),
        "badcolumn": "in_out, maxn, maxns",
        "error_type": "Value Error",
        "error_message": "Invalid entry for MaxN/MaxNs column. Since in_out column is 'out', then both MaxN/MaxNs must be empty."
    }
    errs = [*errs, checkData(**args)]
    print("check 4 ran - value - bruv_data - invalid maxn/maxns value (out)") #tested #working

    # Check 5: if in_out = 'in', maxn & maxns MUST be INTEGER values
    print('begin check 5')
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[
            (~bruvdata['scientificname'].isin(['unknown fish','unknown juvenile fish','unknown crab'])) &
            (bruvdata['in_out'] == 'in') & 
            ((bruvdata['maxn'].isna()) | (bruvdata['maxns'].isna()))
            ].tmp_row.tolist(),
        "badcolumn": "in_out, maxn, maxns",
        "error_type": "Value Error",
        "error_message": "Invalid entry for MaxN/MaxNs column. Since in_out column is 'in', then both MaxN/MaxNs must have an integer value and cannot be left empty."
    }
    errs = [*errs, checkData(**args)]
    print("check 5 ran - value - bruv_data - invalid maxn/maxns value (in)") #tested #working

    # Check 6: MaxN = sum(MaxNs) for i = 0, 1,2,.., where i is the rows per grouped_df on grouped_cols
    print('begin check 6')
    # grouped_cols = ['siteid','estuaryname','stationno','samplecollectiondate','camerareplicate','foventeredtime','fovlefttime','in_out'] 
    cols = ['siteid','estuaryname','stationno','samplecollectiondate','camerareplicate','videoorder','foventeredtime','fovlefttime','in_out'] 
    # keep origial indices for marking file
    bruvdata['tmp_row'] = bruvdata.index
    #subsetting for 'in_out' == 'in' so that there are fewer keys to loop through
    grouped_df = bruvdata[bruvdata['in_out'] == 'in'].groupby(cols) 
    gb = grouped_df.groups
    key_list_from_gb = gb.keys()
    badrows = [] #initialized to append
    for key, values in gb.items():
        if key in key_list_from_gb: #this is by unique group
            tmp = bruvdata.loc[values]
            #print("=============== printing tmp for maxn maxns check ==================")
            #print(tmp)
            #print("=============== tmp printed ==================")
            brows = tmp[(tmp['maxn'] != tmp['maxns'].sum())].tmp_row.tolist()
            #extend adds second list elts to first list
            badrows.extend(brows) #this will be populated to the badrows key in the args dict
    bruvdata = bruvdata.drop(columns=['tmp_row'])
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": badrows,
        "badcolumn": "maxns, maxn",
        "error_type": "Value Error",
        "error_message": "MaxN is the sum of all/each species counts (MaxNs) within a given frame. Each record with the same FOV frame should have the same value for MaxN."
    }
    errs = [*errs, checkData(**args)]
    print("check 6 ran - value - bruv_data - maxn as sum of maxns by group")
    
    # Check 7: bruv_data check on ['in_out','certainty']
    print('begin check 7')
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[(bruvdata['in_out'] == 'in') & (pd.isnull(bruvdata['certainty']))].tmp_row.tolist(),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate",
        "error_type": "Value Error",
        "error_message": "Invalid entry for certainty column. Since in_out column is 'in', then certainty must have a nonempty value."
    }
    errs = [*errs, checkData(**args)]
    print("check 7 ran - value - bruv_data - invalid certainty value") #tested

    # Check: if scientificname = unknown fish, then MaxNs must be empty
    # Removed on 11/28/22 per Jan's request.
    # args = {
    #     "dataframe": bruvdata,
    #     "tablename": 'tbl_bruv_data',
    #     "badrows": bruvdata.query("scientificname == 'unknown fish' and not maxns.isnull()").index.tolist(),
    #     "badcolumn": "scientificname, maxns",
    #     "error_type": "Value Error",
    #     "error_message": "If scientificname = unknown fish, then MaxNs must be empty"
    # }
    # errs = [*errs, checkData(**args)]
    # print("check ran - if scientificname = unknown fish, then MaxNs must be empty")
    ##################################### FINISH CUSTOM CHECKS ######################################################
    
    
    
    
    ##################################### LOGIC CHECKS ######################################################
    print("Begin BRUV Lab Logic Checks...")
    eng = g.eng
    bruvmeta = pd.read_sql("SELECT * FROM tbl_bruv_metadata", eng)
    
    ## NOTE: Logic Checks (Against Database)
    # check 9: Each videolog data must correspond to bruv_metadata in database
    print('begin check 9')
    badrows = pd.merge(
        bruvvideo.assign(tmp_row=bruvvideo.index),
        bruvmeta, 
        on=['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate'],
        how='left',
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'").get('tmp_row').tolist()

    args.update({
        "dataframe": bruvvideo,
        "tablename": "tbl_bruv_videolog",
        "badrows": badrows, 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate",
        "error_type": "Logic Error",
        "error_message": "Records in bruv_videolog should have corresponding records in bruv_metadata. Please submit the metadata for these records first."
    })
    errs = [*errs, checkData(**args)]
    print("check 9 ran - Each videolog data must correspond to metadata in database")

    ## NOTE: Logic Checks (within Submission)
    # Logic Check 11: Each metadata (videolog) with fish is yes and bait is visible should have a corresponding record in data
    print('begin check 11')
    badrows = pd.merge(
        bruvvideo.assign(tmp_row=bruvvideo.index).query("fish == 'Yes' and bait == 'visible'"),
        bruvdata, 
        on=['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate', 'filename', 'videoorder', 'projectid'],
        how='left',
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'").get('tmp_row').tolist()

    args.update({
        "dataframe": bruvvideo,
        "tablename": "tbl_bruv_videolog",
        "badrows": badrows, 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate, filename, videoorder, projectid",
        "error_type": "Logic Error",
        "error_message": "We have metadata in videolog where fish is yes, and bait is visible, but no corresponding data in bruv_data."
    })
    errs = [*errs, checkData(**args)]
    print("check 11 ran - Each metadata (videolog) with fish is yes and bait is visible should have a corresponding record in data")
    
    # Logic Check 12: Each metadata (videolog) with fish is no should not have a corresponding record in data
    print('begin check 12')
    badrows = pd.merge(
        bruvvideo.assign(tmp_row=bruvvideo.index).query("fish == 'No'"),
        bruvdata, 
        on=['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate', 'filename', 'videoorder', 'projectid'],
        how='left',
        indicator='in_which_df'
    ).query("in_which_df == 'both'").get('tmp_row').tolist()
    
    args.update({
        "dataframe": bruvvideo,
        "tablename": "tbl_bruv_videolog",
        "badrows": badrows, 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate, filename, videoorder, projectid",
        "error_type": "Logic Error",
        "error_message": "Each metadata (videolog) with fish is no should not have a corresponding record in data."
    })
    errs = [*errs, checkData(**args)]
    print("check 12 ran -  If fish is 'no' in videolog then data should not have corresponding records")

    # Logic Check 10: Each data (bruv_data) should have a corresponding record in videolog
    print('begin check 10')
    badrows = pd.merge(
        bruvdata.assign(tmp_row=bruvdata.index),
        bruvvideo, 
        on=['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate', 'filename', 'videoorder', 'projectid'],
        how='left',
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'").get('tmp_row').tolist()
    
    args.update({
        "dataframe": bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": badrows, 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate, filename, videoorder, projectid",
        "error_type": "Logic Error",
        "error_message": "Each data (bruv_data) should have a corresponding record in videolog."
    })
    errs = [*errs, checkData(**args)]
    print("check 10 ran -  Each data (bruv_data) should have a corresponding record in videolog")        

    ##################################### FINISH LOGIC CHECKS ######################################################
    
    #check 8: Scientificname/commoname pair for species must match lookup
    print('begin check 8')
    # Multicol lookup check
    def multicol_lookup_check(df_to_check,lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")

        for c in check_cols:
            df_to_check[c] = df_to_check[c].apply(lambda x: str(x).lower().strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())
        
        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)

    print("read in fish lookup")
    lookup_sql = f"SELECT * from lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['scientificname', 'commonname', 'status']
    #check_cols = ['scientificname', 'commonname']
    lookup_cols = ['scientificname', 'commonname', 'status']
    #lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(bruvdata, lu_species, check_cols, lookup_cols)
    
    args.update({
        "dataframe":  bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": badrows,
        "badcolumn": "scientificname,commonname,status",
        "error_type" : "Multicolumn Lookup Error",
        "error_message" : f'The scientificname/commonname/status entry did not match the lookup list.'
                         '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
    })
    errs = [*errs, checkData(**args)]
    print("check 8 ran - bruv_data - multicol species")
    '''
    return {'errors': errs, 'warnings': warnings}




    '''
    # #Check 7: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required
    # print("Check 7 begin:")
    # args.update({
    #     "dataframe": bruvmeta,
    #     "tablename": "tbl_bruv_metadata",
    #     "badrows": bruvmeta[(~bruvmeta['elevation_ellipsoid'].isna() | ~bruvmeta['elevation_orthometric'].isna()) & ( bruvmeta['elevation_units'].isna() | (bruvmeta['elevation_units'] == -88))].tmp_row.tolist(),
    #     "badcolumn": "elevation_units",
    #     "error_type": "Empty value",
    #     "error_message": "Elevation_units is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # })
    # errs = [*errs, checkData(**args)]

    # print('check 7 ran - ele_ellip or ele_ortho is reported then ele_units is required')

    # #Check 8: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Corr is required
    # print("Check 8 begin:")
    # args.update({
    #     "dataframe": bruvmeta,
    #     "tablename": "tbl_bruv_metadata",
    #     "badrows": bruvmeta[(~bruvmeta['elevation_ellipsoid'].isna() | ~bruvmeta['elevation_orthometric'].isna()) & ( bruvmeta['elevation_corr'].isna() | (bruvmeta['elevation_corr'] == -88))].tmp_row.tolist(),
    #     "badcolumn": "Elevation_Corr",
    #     "error_type": "Empty value",
    #     "error_message": "Elevation_Corr is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # })
    # errs = [*errs, checkData(**args)]

    # print('check 8 ran - ele_ellip or ele_ortho is reported then Elevation_Corr is required')

    # #Check 9: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Datum is required
    # print("Check 9 begin:")
    # args.update({
    #     "dataframe": bruvmeta,
    #     "tablename": "tbl_bruv_metadata",
    #     "badrows": bruvmeta[(~bruvmeta['elevation_ellipsoid'].isna() | ~bruvmeta['elevation_orthometric'].isna()) & ( bruvmeta['elevation_datum'].isna() | (bruvmeta['elevation_datum'] == -88))].tmp_row.tolist(),
    #     "badcolumn": "Elevation_Datum",
    #     "error_type": "Empty value",
    #     "error_message": "Elevation_Datum is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # })
    # errs = [*errs, checkData(**args)]

    # print('check 9 ran - ele_ellip or ele_ortho is reported then Elevation_Datum is required')

    

    

    # # Check: bruvintime format validation    24 hour clock HH:MM time validation
    # timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    # args.update({
    #     "dataframe": bruvmeta,
    #     "tablename": "tbl_bruv_metadata",
    #     "badrows": bruvmeta[~bruvmeta['bruvintime'].str.match(timeregex)].index.tolist(),
    #     "badcolumn": "bruvintime",
    #     "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - bruv_metadata - bruvintime format") 


    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": df[df.temperature != 'asdf'].index.tolist(),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]
    
    #(1) maxnspecies is nonnegative
 
    timeregex ="([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    args.update({
        "dataframe":bruvdata,
        "tablename":'tbl_bruv_data',
        "badrows":bruvdata[bruvdata['maxnspecies'] < 0].index.tolist(),
        "badcolumn":"maxnspecies",
        "error_type":"Value out of range",
        #"error_message":"Max number of species should be between 0 and 100"
        "error_message":"Max number of species must be nonnegative."
    })
    errs = [*errs, checkData(**args)]

    #(2) maxnspecies should not exceed 100 (warning)
    args.update({
        "dataframe":bruvdata,
        "tablename":'tbl_bruv_data',
        "badrows":bruvdata[(bruvdata['maxnspecies'] < 0) | (bruvdata['maxnspecies'] > 100)].index.tolist(),
        "badcolumn":"maxnspecies",
        "error_type":"Value out of range",
        "error_message":"Max number of species should NOT exceed 100."
    })
    warnings = [*warnings, checkData(**args)]

    
    args.update({
        "dataframe":bruvdata,
        "tablename":'tbl_bruv_data',
        "badrows":bruvdata[bruvdata.foventeredtime.apply(pd.Timestamp) > bruvdata.fovlefttime.apply(pd.Timestamp)].index.tolist(),
        "badcolumn":"foventeredtime,fovlefttime",
        "error_type": "Value out of range",
        "error_message":"FOV entered time must be before FOV left time"
    })
    errs = [*errs, checkData(**args)]
    print("bruvmeta.head()['bruvintime']")
    print(bruvmeta.head()['bruvintime'])
    print("bruvmeta.head()['bruvouttime']")
    print(bruvmeta.head()['bruvouttime'])

    #NOTE THIS SHOULD BE TAKEN CARE OF BY CORE CHECK
########################################################################################################################################################
    # # Check: bruvmetadata bruvintime time validation
    # timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    # badrows_bruvintime = bruvmeta[
    #     bruvmeta['bruvintime'].apply(
    #         lambda x: not bool(re.match(timeregex, str(x))) if str(x) != "00:00:00" else False)
    #         ].index.tolist()
    # args.update({
    #     "dataframe": bruvmeta,
    #     "tablename": "tbl_bruv_metadata",
    #     "badrows": badrows_bruvintime,
    #     "badcolumn": "bruvintime",
    #     "error_type" : "Time Format Error",
    #     "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - bruv_metadata - bruvintime") 
    # # Check: bruvmetadata bruvouttime time validation
    # badrows_bruvouttime = bruvmeta[
    #     bruvmeta['bruvouttime'].apply(
    #         lambda x: not bool(re.match(timeregex, str(x))) if str(x) != "00:00:00" else False)
    #         ].index.tolist()
    # args.update({
    #     "dataframe": bruvmeta,
    #     "tablename": "tbl_bruv_metadata",
    #     "badrows": badrows_bruvouttime,
    #     "badcolumn": "bruvouttime",
    #     "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - tbl_bruv_metadata - bruvouttime format") 
########################################################################################################################################################
    # NOTE This check needs to take into consideration that the data is clean if the start date is before the end date
    # Note: starttime and endtime format checks must pass before entering the starttime before endtime check
    '''
    df = bruvmeta[(bruvmeta['bruvintime'] != "00:00:00") & (bruvmeta['bruvouttime'] != "00:00:00")]
    print(" =========================================")
    print("subsetting df on bruv time: ")
    print(" =========================================")
    print(df['bruvintime'])
    print(df['bruvouttime'])
    if (len(badrows_bruvintime) == 0 & (len(badrows_bruvouttime) == 0)):
        args.update({
            "dataframe": bruvmeta,
            "tablename": "tbl_bruv_metadata",
            "badrows": df[df['bruvintime'].apply(
                lambda x: pd.Timestamp(str(x)).strftime('%H:%M') 
                if not "00:00:00" else '') >= df['bruvouttime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') 
                if not "00:00:00" else '')].index.tolist(),
            "badcolumn": "bruvintime",
            "error_message": "Bruvintime value must be before bruvouttime."
            })
        errs = [*errs, checkData(**args)]
    print("check ran - tbl_bruv_metadata - bruvintime before bruvouttime")

    del badrows_bruvintime
    del badrows_bruvouttime
    '''
    # Check 2: depth_m is positive for tbl_bruv_metadata 
    print("begin check 2")
    args.update({
        "dataframe": bruvmeta,
        "tablename": 'tbl_bruv_metadata',
        "badrows": bruvmeta[(bruvmeta['depth_m'] < 0) & (bruvmeta['depth_m'] != -88)].tmp_row.tolist(),
        "badcolumn": "depth_m",
        "error_type" : "Value out of range",
        "error_message" : "Depth measurement should not be a negative number, must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("check 2 ran - tbl_bruv_metadata - nonnegative depth_m") # tested
    '''