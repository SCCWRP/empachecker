# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch, get_primary_key
import re
import time

def fishseines(all_dfs):
    
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
    
    fishmeta = all_dfs['tbl_fish_sample_metadata']
    fishabud = all_dfs['tbl_fish_abundance_data']
    fishdata = all_dfs['tbl_fish_length_data']

    fishabud['tmp_row'] = fishabud.index
    fishdata['tmp_row'] = fishdata.index
    fishmeta['tmp_row'] = fishmeta.index

    # Begin Fish Custom Checks
    print("# --- Begin Fish Custom Checks --- #")
    
    
    
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
    # ------------------------------------------------ Fish Logic Checks ----------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    fishmeta_pkey = get_primary_key('tbl_fish_sample_metadata', g.eng)
    fishabud_pkey = get_primary_key('tbl_fish_abundance_data', g.eng)
    fishdata_pkey = get_primary_key('tbl_fish_length_data', g.eng)

    fishmeta_fishabud_shared_pkey = list(set(fishmeta_pkey).intersection(set(fishabud_pkey)))
    fishmeta_fishdata_shared_pkey = list(set(fishmeta_pkey).intersection(set(fishdata_pkey)))
    fishabud_fishdata_shared_pkey = list(set(fishabud_pkey).intersection(set(fishdata_pkey)))

    print("# CHECK - 1")
    # Description: Each sample metadata must include corresponding abundance data (🛑 ERROR 🛑)
    # Created Coder: Zaib(?)
    # Created Date: 2021
    # Last Edited Date: 08/17/23
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the code so it follows the coding standard.
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows": mismatch(fishmeta, fishabud, fishmeta_fishabud_shared_pkey), 
        "badcolumn": ','.join(fishmeta_fishabud_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each sample metadata must include corresponding abundance data"
    })
    errs = [*errs, checkData(**args)]
    # END OF CHECK - Each sample metadata must include corresponding abundance data (🛑 ERROR 🛑)
    print("# END OF CHECK - 1")
    


    print("# CHECK - 2")
    # Description: Each abundance data must include corresponding sample metadata (🛑 ERROR 🛑)
    # Created Coder: Zaib(?)
    # Created Date: 2021
    # Last Edited Date: 08/17/2023
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the code so it follows the coding standard.
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": mismatch(fishabud, fishmeta, fishmeta_fishabud_shared_pkey), 
        "badcolumn": ','.join(fishmeta_fishabud_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each abundance data must include corresponding sample metadata"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")



    print("# CHECK - 3")
    # Description: Each sample metadata must include corresponding length data (🛑 ERROR 🛑)
    # Created Coder: Zaib(?)
    # Created Date: 2021
    # Last Edited Date: 08/17/2023
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the code so it follows the coding standard.
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_abundance_data",
        "badrows": mismatch(fishmeta, fishdata, fishmeta_fishdata_shared_pkey), 
        "badcolumn": ','.join(fishmeta_fishdata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each sample metadata must include corresponding length data"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")



    print("# CHECK - 4")
    # Description: Each length data must include corresponding sample metadata (🛑 ERROR 🛑)
    # Created Coder: Zaib(?)
    # Created Date: 2021
    # Last Edited Date: 08/17/2023
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the code so it follows the coding standard.
    args.update({
        "dataframe": fishdata,
        "tablename": "tbl_fish_abundance_data",
        "badrows": mismatch(fishdata, fishmeta, fishmeta_fishdata_shared_pkey), 
        "badcolumn": ','.join(fishmeta_fishdata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each length data must include corresponding sample metadata"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 4")



    print("# CHECK - 5")
    # Description: Each abundance data must include corresponding length data (🛑 ERROR 🛑)
    # Created Coder: Zaib(?)
    # Created Date: 2021
    # Last Edited Date: 08/17/2023
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the code so it follows the coding standard.
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": mismatch(fishabud, fishdata, fishabud_fishdata_shared_pkey), 
        "badcolumn": ','.join(fishabud_fishdata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each abundance data must include corresponding length data"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 5")



    print("# CHECK - 6")
    # Description: Each length data data must include corresponding abundance data (🛑 ERROR 🛑)
    # Created Coder: Zaib(?)
    # Created Date: 2021
    # Last Edited Date: 08/17/2023
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the code so it follows the coding standard.
    args.update({
        "dataframe": fishdata,
        "tablename": "tbl_fish_abundance_data",
        "badrows": mismatch(fishdata, fishabud, fishabud_fishdata_shared_pkey), 
        "badcolumn": ','.join(fishabud_fishdata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each length data data must include corresponding abundance data"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 6")



    print("# CHECK - 7")
    # Description: The number of records in abundance should match with the number of records in length only if abundance number is between 1-10. 
    # If abundance number > 10, then the number of matching records in length should be the minimum of 10. (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 08/17/23
    # Last Edited Date: Duy Nguyen
    # Last Edited Coder: 08/17/23
    # NOTE (MM/DD/YY):

    print("# END OF CHECK - 7")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END OF Fish Logic Checks --------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################




    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------ Sample Metadata Checks ---------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
    print("# CHECK - 8")
    # Description: Netreplicate must be consecutive within a projectid,siteid,estuaryname,stationno,samplecollectiondate,surveytype group (🛑 ERROR 🛑)
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (MM/DD/YY): 
    print("# END OF CHECK - 8")



    print("# CHECK - 9")
    # Description: area_m2 = seinelength_m x seinedistance_m. Mark the records as errors if any calculation is incorrect (🛑 ERROR 🛑)
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (MM/DD/YY): 

    print("# END OF CHECK - 9")




    print("# CHECK - 10")
    # Description: Start Time must be before End Time (🛑 ERROR 🛑)
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (MM/DD/YY): 

    print("# END OF CHECK - 10")

    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------ ENd of Sample Metadata Checks ---------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #



    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------ Abundance Checks ---------------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
    print("# CHECK - 11")
    # Description: Range for abundance should be between [0, 5000] or -88 (empty) (🛑 ERROR 🛑)
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (MM/DD/YY):     

    print("# END OF CHECK - 11")



    print("# CHECK - 12")
    # Description: If method is "count" and catch is "yes" then abundance should be non zero integer in fish abundance (🛑 ERROR 🛑)
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (08/17/23): 
    
    print("# END OF CHECK - 12")



    print("# CHECK - 13")
    # Description: If method is "pa" and catch is "yes" in sample metadata then abundance should be -88 or a positive number (🛑 ERROR 🛑)
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (MM/DD/YY): 

    print("# END OF CHECK - 13")



    print("# CHECK - 14")
    # Description: If catch is "no" in sample metadata then abundance should be 0 (🛑 ERROR 🛑)
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (MM/DD/YY): 
    print("# END OF CHECK - 14")


    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------End of Abundance Checks ---------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################



    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------ Length Data Checks ---------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
    print("# CHECK - 15")
    # Description: If catch is "no" in sample metadata then length_mm should be -88 (🛑 ERROR 🛑)
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (MM/DD/YY): 

    print("# END OF CHECK - 15")



    print("# CHECK - 16")
    # Description: Replicate must be consecutive within a projectid,siteid,estuaryname,stationno,samplecollectiondate,surveytype,netreplicate,scientificname group (🛑 ERROR 🛑)
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (MM/DD/YY): 

    print("# END OF CHECK - 16")


    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------End of Length Data Checks ---------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
















    #############   START OF CUSTOM CHECKS ############################################################################################################################
    # print("START Fish Seines custom Checks...")
   
    # # Check 4: abundance range [0, 5000]
    # print("Start custom check 4")
    # args.update({
    #     "dataframe": fishabud,
    #     "tablename": "tbl_fish_abundance_data",
    #     "badrows":fishabud[((fishabud['abundance'] < 0) | (fishabud['abundance'] > 5000)) & (fishabud['abundance'] != -88)].tmp_row.tolist(),
    #     "badcolumn": "abundance",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your abundance value must be between 0 to 5000. If this value is supposed to be empty, please fill with -88."
    # })
    # warnings = [*warnings, checkData(**args)]
    # print("check 4 ran - tbl_fish_abundance_data - abundance range") # tested and working 5nov2021

    # #NOTE time checks should be taken care of by Core check -Aria
    # # commenting out time checks for now - zaib 28 oct 2021
    # ## for time fields, in preprocess.py consider filling empty time related fields with NaT using pandas | check format of time?? | should be string

    # # # Check: starttime format validation
    # # timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    # # badrows_starttime = fishmeta[
    # #     fishmeta['starttime'].apply(
    # #         lambda x: 
    # #         not bool(re.match(timeregex, str(x))) 
    # #         if not '-88' else 
    # #         False
    # #     )
    # # ].tmp_row.tolist()
    # # args.update({
    # #     "dataframe": fishmeta,
    # #     "tablename": "tbl_fish_sample_metadata",
    # #     "badrows": badrows_starttime,
    # #     "badcolumn": "starttime",
    # #     "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    # # })
    # # errs = [*errs, checkData(**args)]
    # # print("check ran - tbl_fish_sample_metadata - starttime format") # tested and working 9nov2021

    # # # Check: endtime format validation
    # # timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$"
    # # badrows_endtime = fishmeta[fishmeta['endtime'].apply(lambda x: not bool(re.match(timeregex, str(x))) if not '-88' else False)].tmp_row.tolist()
    # # args.update({
    # #     "dataframe": fishmeta,
    # #     "tablename": "tbl_fish_sample_metadata",
    # #     "badrows": badrows_endtime,
    # #     "badcolumn": "endtime",
    # #     "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    # # })
    # # errs = [*errs, checkData(**args)]
    # # print("check ran - tbl_fish_sample_metadata - endtime format") # tested and working 9nov2021


    # # Check: starttime is before endtime --- crashes when time format is not HH:MM
    # # Note: starttime and endtime format checks must pass before entering the starttime before endtime check
    # # must be revised
    # '''
    # df = fishmeta[(fishmeta['starttime'] != '-88') & (fishmeta['endtime'] != '-88') & (fishmeta['endtime'] != -88)]
    # print("subset df for time check: ")
    # print(df)

    # badrows = df[df['starttime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not '-88' else 'False') >= df['endtime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not '-88' else 'False')].tmp_row.tolist()
    # if (len(badrows_starttime) == 0 & (len(badrows_endtime) == 0)):
    #     args.update({
    #         "dataframe": fishmeta,
    #         "tablename": "tbl_fish_sample_metadata",
    #         "badrows": badrows,
    #         "badcolumn": "starttime",
    #         "error_message": "Starttime value must be before endtime. Time should be entered in HH:MM format on a 24-hour clock."
    #         })
    #     errs = [*errs, checkData(**args)]
    #     print("check ran - tbl_fish_sample_metadata - starttime before endtime")

    # del badrows_starttime
    # del badrows_endtime
    # '''

    # # New checks written by Duy 10-04-22
    # merged = pd.merge(
    #     fishabud,
    #     fishmeta, 
    #     how='left',
    #     suffixes=('', '_meta'),
    #     on = ['siteid','estuaryname','stationno','samplecollectiondate','surveytype','netreplicate']
    # )

    # # Check 5: if method = count, catch = yes in meta, then abundance is non zero integer in fish abundance tab
    # print("Start custom check 5")
    # # badrows = merged[(merged['abundance'] == 0) & (merged['method_meta'].apply(lambda x: str(x).lower().strip()) == 'count') & (merged['catch'].apply(lambda x: str(x).lower().strip()) == 'yes' )].tmp_row.tolist()
    # args.update({
    #     "dataframe": fishabud,
    #     "tablename": "tbl_fish_abundance_data",
    #     "badrows": merged[(merged['abundance'] == 0) & (merged['method_meta'].apply(lambda x: str(x).lower().strip()) == 'count') & (merged['catch'].apply(lambda x: str(x).lower().strip()) == 'yes' )].tmp_row.tolist(),
    #     "badcolumn": "abundance",
    #     "error_type": "Value Out of Range",
    #     "error_message": "If method = count, catch = yes in fish_meta, then abundance should be non zero integer in fish_abundance"
    # })
    # errs = [*errs, checkData(**args)]
    # print("check 5 ran") 
    
    # # Check 6: if method = pa, catch = yes in meta, then abundance is -88 in fish abundance tab
    # print("Start custom check 6")
    # badrows = merged[(merged['abundance'] != -88) & (merged['method_meta'].apply(lambda x: str(x).lower().strip()) == 'pa') & (merged['catch'].apply(lambda x: str(x).lower().strip()) == 'yes' )].tmp_row.tolist()
    # args.update({
    #     "dataframe": fishabud,
    #     "tablename": "tbl_fish_abundance_data",
    #     "badrows": badrows,
    #     "badcolumn": "abundance",
    #     "error_type": "Value Out of Range",
    #     "error_message": "If method = pa, catch = yes in fish_meta, then abundance should be -88 in fish_abundance"
    # })
    # errs = [*errs, checkData(**args)]
    # print("check 6 ran") 

    # # Check 7: if catch = no in meta, then abundance is 0 in fish abundance
    # print("Start custom check 7 ")     
    # badrows = merged[(merged['abundance'] != 0) & (merged['catch'].apply(lambda x: str(x).lower().strip()) == 'no' )].tmp_row.tolist()
    # args.update({
    #     "dataframe": fishabud,
    #     "tablename": "tbl_fish_abundance_data",
    #     "badrows": badrows,
    #     "badcolumn": "abundance",
    #     "error_type": "Value Out of Range",
    #     "error_message": "If catch = no in fish_meta, then abundance should be 0 in fish_abundance"
    # })
    # errs = [*errs, checkData(**args)]
    # print("check 7 ran") 



    # def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
    #     assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
    #     assert isinstance(lookup_cols, list), "lookup columns is not a list"

    #     lookup_df = lookup_df.assign(match="yes")
        
    #     for c in check_cols:
    #         df_to_check[c] = df_to_check[c].apply(lambda x: str(x).lower().strip())
    #     for c in lookup_cols:
    #         lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())

    #     merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
    #     badrows = merged[pd.isnull(merged.match)].tmp_row.tolist()
    #     return(badrows)

    # lookup_sql = f"SELECT * FROM lu_fishmacrospecies;"
    # lu_species = pd.read_sql(lookup_sql, g.eng)
    # # Removing status part of multicolumn check for now as requested by Jan. 16 Dec 2021
    # #check_cols = ['scientificname', 'commonname', 'status']
    # check_cols = ['scientificname', 'commonname']
    # #lookup_cols = ['scientificname', 'commonname', 'status']
    # lookup_cols = ['scientificname', 'commonname']

    # badrows = multicol_lookup_check(fishabud, lu_species, check_cols, lookup_cols)
    
    # # Check 8: multicolumn for species lookup
    # print("Start custom check 8") 
    # args.update({
    #     "dataframe": fishabud,
    #     "tablename": "tbl_fish_abundance_data",
    #     "badrows": multicol_lookup_check(fishabud, lu_species, check_cols, lookup_cols),
    #     "badcolumn":"commonname",
    #     "error_type": "Multicolumn Lookup Error",
    #     "error_message": f'The scientificname/commonname entry did not match the lookup list '
    #                     '<a '
    #                     f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
    #                     'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
        
    # })

    # errs = [*errs, checkData(**args)]
    # print("check 8 ran - fish_abundance_metadata - multicol species") 

    # badrows = multicol_lookup_check(fishdata, lu_species, check_cols, lookup_cols)

    # #check 9: If abundance > 0, then there must be a corresponding record in length data (sort of a logic check)
    # print("Start custom check 9") 
    # groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'scientificname', 'surveytype', 'netreplicate', 'projectid']
    # fishabud_filtered = fishabud[fishabud["abundance"] > 0]

    # args.update({
    #     "dataframe": fishabud,
    #     "tablename": "tbl_fish_abundance_data",
    #     "badrows" : mismatch(fishabud_filtered, fishdata, groupcols),
    #     "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, scientificname, surveytype, netreplicate, projectid",
    #     "error_type": "Logic Error",
    #     "error_message": "If fishabud > 0, then Records in fishabud must have corresponding records in fishdata. Missing records in fishdata."
    # })
    # errs = [*errs, checkData(**args)]

    # print("check ran 9 - If abundance > 0, then there must be a corresponding record in length data") 

    # # Check 10: multicolumn for species lookup
    # print("Start custom check 10") 
    # args.update({
    #     "dataframe": fishdata,
    #     "tablename": "tbl_fish_length_data",
    #     "badrows": multicol_lookup_check(fishdata, lu_species, check_cols, lookup_cols),
    #     "badcolumn": "commonname",
    #     "error_type": "Multicolumn Lookup Error",
    #     "error_message": f'The scientificname/commonname entry did not match the lookup list '
    #                     '<a '
    #                     f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
    #                     'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species

    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran 10 - fish_length_metadata - multicol species") 

    # # #Check 11: If If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Time is required
    # # print("Check 11 fish seines begin:")
    # # args.update({
    # #     "dataframe": fishmeta,
    # #     "tablename": "tbl_fish_sample_metadata",
    # #     "badrows": fishmeta[(~fishmeta['elevation_ellipsoid'].isna() | ~fishmeta['elevation_orthometric'].isna()) & ( fishmeta['elevation_time'].isna() | (fishmeta['elevation_time'] == -88))].tmp_row.tolist(),
    # #     "badcolumn": "elevation_time",
    # #     "error_type": "Empty value",
    # #     "error_message": "Elevation_time is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # # })
    # # errs = [*errs, checkData(**args)]
    # # print('check 11 ran - ele_ellip or ele_ortho is reported then ele_time is required')

    # # #Check 12: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required
   
    # # print("begin check 12 fishsieness")
    # # args.update({
    # #     "dataframe": fishmeta,
    # #     "tablename": "tbl_fish_sample_metadata",
    # #     "badrows":fishmeta[(~fishmeta['elevation_ellipsoid'].isna() | ~fishmeta['elevation_orthometric'].isna()) & ( fishmeta['elevation_units'].isna() | (fishmeta['elevation_units'] == -88))].tmp_row.tolist(),
    # #     "badcolumn": "elevation_units",
    # #     "error_type": "Empty value",
    # #     "error_message": "Elevation_units is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # # })
    # # errs = [*errs, checkData(**args)]
    # # print('check 12 ran - ele_units required when ele_ellip and ele_ortho are reported')

    # # #Check 13: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Corr is required
    # # print('beging check 13 fishsienes:')
    # # args.update({
    # #     "dataframe": fishmeta,
    # #     "tablename": "tbl_fish_sample_metadata",
    # #     "badrows":fishmeta[(~fishmeta['elevation_ellipsoid'].isna() | ~fishmeta['elevation_orthometric'].isna()) & ( fishmeta['elevation_corr'].isna() | (fishmeta['elevation_corr'] == -88))].tmp_row.tolist(),
    # #     "badcolumn": "elevation_corr",
    # #     "error_type": "Empty value",
    # #     "error_message": "Elevation_corr is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # # })
    # # errs = [*errs, checkData(**args)]
    # # print('check 13 ran - ele_corr required when ele_ellip and ele_ortho are reported')

    # # #Check 14  If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Datum is required
    # # print('begin check 14 fishsienes:')
    # # args.update({
    # #     "dataframe": fishmeta,
    # #     "tablename": "tbl_fish_sample_metadata",
    # #     "badrows":fishmeta[(~fishmeta['elevation_ellipsoid'].isna() | ~fishmeta['elevation_orthometric'].isna()) & ( fishmeta['elevation_datum'].isna() | (fishmeta['elevation_datum'] == -88))].tmp_row.tolist(),
    # #     "badcolumn": "elevation_datum",
    # #     "error_type": "Empty value",
    # #     "error_message": "Elevation_datum is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # # })
    # # errs = [*errs, checkData(**args)]
    # # print('check 14 ran - elev_datum is required when ele_ellip and elev_ortho are reported')
    # print("END Fish Seines custom Checks...")
    #############   END OF CUSTOM CHECKS ############################################################################################################################
    


    # End of Microplastics Custom Checks
    print("# --- End of Microplastics Custom Checks --- #")

    return {'errors': errs, 'warnings': warnings}