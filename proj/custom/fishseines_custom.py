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
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/17/23
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard.
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
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/17/2023
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard.
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
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/17/2023
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard.
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows": mismatch(fishmeta, fishdata, fishmeta_fishdata_shared_pkey), 
        "badcolumn": ','.join(fishmeta_fishdata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each sample metadata must include corresponding length data"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")



    print("# CHECK - 4")
    # Description: Each length data must include corresponding sample metadata (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/17/2023
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard.
    args.update({
        "dataframe": fishdata,
        "tablename": "tbl_fish_length_data",
        "badrows": mismatch(fishdata, fishmeta, fishmeta_fishdata_shared_pkey), 
        "badcolumn": ','.join(fishmeta_fishdata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each length data must include corresponding sample metadata"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 4")



    print("# CHECK - 5")
    # Description: Each abundance data must include corresponding length data (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/17/2023
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard.
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
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/17/2023
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard.
    args.update({
        "dataframe": fishdata,
        "tablename": "tbl_fish_length_data",
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
    # Created Date: 8/23/23
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (8/23/23): Duy wrote this check but has not tested it
    
    # merge the two dataframes
    merged_data = pd.merge(
        fishabud,
        fishdata,
        on=fishabud_fishdata_shared_pkey,
        how='inner'
    )
    
    # count the matching records and store the result in a column called num_matching_recs
    counted_records = merged_data.groupby(fishabud_fishdata_shared_pkey).size().reset_index(name='num_matching_recs')

    # merge the result back to the fish abundance table to compare num_matching_recs with the abundance column
    merged_tmp = pd.merge(
        fishabud,
        counted_records,
        on=fishabud_fishdata_shared_pkey,
        how='left'
    ).filter(items=[*fishabud_fishdata_shared_pkey,*['abundance','num_matching_recs','tmp_row']])

    badrows = merged_tmp[
        (   
            (merged_tmp['abundance'] > 0) &
            (merged_tmp['abundance'] <= 10) &
            (merged_tmp['abundance'] != merged_tmp['num_matching_recs'])
        ) | 
        (
            (merged_tmp['abundance'] > 10) &
            (merged_tmp['num_matching_recs'] < 10)
        )
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": badrows, 
        "badcolumn": 'abundance',
        "error_type": "Logic Error",
        "error_message": "The number of records in abundance should match with the number of records in length only if abundance number is between 1-10. If abundance number > 10, then the number of matching records in length should be the minimum of 10."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 7")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END OF Fish Logic Checks --------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################







    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------ Sample Metadata Checks ---------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
    print("# CHECK - 8")
    # Description: Netreplicate must be consecutive within a projectid,siteid,estuaryname,stationno,samplecollectiondate,surveytype group (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 8/23/23
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (8/23/23): Duy wrote this check but did not test it
    badrows = []
    for _, subdf in fishmeta.groupby([x for x in fishmeta_pkey if x != 'netreplicate']):
        df = subdf.filter(items=[*fishmeta_pkey,*['tmp_row']])
        df = df.sort_values(by='netreplicate').fillna(0)
        rep_diff = df['netreplicate'].diff().dropna()
        all_values_are_one = (rep_diff == 1).all()
        if not all_values_are_one:
            badrows.extend(df.tmp_row.tolist())
    
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows": badrows, 
        "badcolumn": 'netreplicate',
        "error_type": "Custom Error",
        "error_message": " Netreplicate must be consecutive within a projectid,siteid,estuaryname,stationno,samplecollectiondate,surveytype group"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 8")



    print("# CHECK - 9")
    # Description: area_m2 = seinelength_m x seinedistance_m. Mark the records as errors if any calculation is incorrect (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 8/23/23
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (8/23/23): Duy wrote this check but did not test it
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows": fishmeta[
            round(fishmeta['area_m2'], 2) != round((fishmeta['seinelength_m'] * fishmeta['seinedistance_m']), 2)
        ].tmp_row.tolist(), 
        "badcolumn": 'area_m2',
        "error_type": "Custom Error",
        "error_message": "area_m2 does not equal to seinelength_m x seinedistance_m."
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 9")




    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------ End of Sample Metadata Checks --------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################






    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------ Abundance Checks ---------------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
    print("# CHECK - 11")
    # Description: Range for abundance should be between [0, 5000] or -88 (empty) (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 8/18/23  
    # Last Edited Coder: Duy Nguyen
    # NOTE (8/18/23): Duy adjusts the format so it follows the coding standard
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows":fishabud[
            (
                fishabud['abundance'] != -88
            ) &
            (
                (fishabud['abundance'] < 0) | (fishabud['abundance'] > 5000)
            )
        ].tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type" : "Value out of range",
        "error_message" : "Range for abundance should be between [0, 5000] or -88 (empty)"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 11")



    print("# CHECK - 12")
    # Description: If method is "count" and catch is "yes" then abundance should be non zero integer in fish abundance (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 8/18/23
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard
    merged = pd.merge(
        fishabud,
        fishmeta, 
        how='left',
        suffixes=('', '_meta'),
        on=fishmeta_fishabud_shared_pkey
    )
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": merged[
            (merged['method_meta'] == 'count') & 
            (merged['catch'] == 'yes')  &
            (merged['abundance'] <= 0)
        ].tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Logic Error",
        "error_message": "If method = count, catch = yes in fish_meta, then abundance should be non zero integer in fish_abundance"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 12")



    print("# CHECK - 13")
    # Description: If method is "pa" and catch is "yes" in sample metadata then abundance should be -88 or a positive number (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 8/18/23 
    # Last Edited Coder: Duy Nguyen
    # NOTE (8/18/23): Duy adjusts the format so it follows the coding standard
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": merged[
            (merged['method_meta'] == 'pa') & 
            (merged['catch'] == 'yes')  &
            (merged['abundance'] != -88) & 
            (merged['abundance'] <= 0) 
        ].tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Logic Error",
        "error_message": "If method is pa and catch is yes in sample metadata then abundance should be -88 or a positive number"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 13")



    print("# CHECK - 14")
    # Description: If catch is "no" in sample metadata then abundance should be 0 (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 8/18/23 
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/18/23): Duy adjusts the format so it follows the coding standard
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": merged[
            (merged['catch'] == 'no') &
            (merged['abundance'] != 0) 
        ].tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Logic Error",
        "error_message": "If catch is no in sample metadata then abundance should be 0"
    })
    errs = [*errs, checkData(**args)]    
    print("# END OF CHECK - 14")


    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------End of Abundance Checks ---------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################







    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------ Length Data Checks ---------------------------------------------------------#
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
    print("# CHECK - 15")
    # Description: If catch is "no" in sample metadata then length_mm should be -88 (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 8/18/23 
    # Last Edited Coder: Duy Nguyen
    # NOTE (8/18/23): Duy adjusts the format so it follows the coding standard
    merged = pd.merge(
        fishdata,
        fishmeta, 
        how='left',
        suffixes=('', '_meta'),
        on=fishmeta_fishdata_shared_pkey
    )
    args.update({
        "dataframe": fishdata,
        "tablename": "tbl_fish_length_data",
        "badrows": merged[
            (merged['catch'] == 'no') &
            (merged['length_mm'] != -88)
        ].tmp_row.tolist(),
        "badcolumn": "length_mm",
        "error_type": "Logic Error",
        "error_message": "If catch is no in sample metadata then length_mm should be -88"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 15")



    print("# CHECK - 16")
    # Description: Replicate must be consecutive within a projectid,siteid,estuaryname,stationno,samplecollectiondate,surveytype,netreplicate,scientificname group (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 8/23/23
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (8/23/23): Duy wrote this check but did not test it
    badrows = []
    for _, subdf in fishdata.groupby([x for x in fishdata_pkey if x != 'replicate']):
        df = subdf.filter(items=[*fishdata_pkey,*['tmp_row']])
        df = df.sort_values(by='replicate').fillna(0)
        rep_diff = df['replicate'].diff().dropna()
        all_values_are_one = (rep_diff == 1).all()
        if not all_values_are_one:
            badrows = [*badrows, *df.tmp_row.tolist()]
    args.update({
        "dataframe": fishdata,
        "tablename": "tbl_fish_length_data",
        "badrows": badrows,
        "badcolumn": "replicate",
        "error_type": "Custom Error",
        "error_message": "Replicate must be consecutive within a projectid,siteid,estuaryname,stationno,samplecollectiondate,surveytype,netreplicate,scientificname group"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 16")


    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ------------------------------------------------------End of Length Data Checks -------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################







    # End of Fish Custom Checks
    print("# --- End of Fish Custom Checks --- #")

    return {'errors': errs, 'warnings': warnings}