# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch
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
    
    fishabud = all_dfs['tbl_fish_abundance_data']
    fishdata = all_dfs['tbl_fish_length_data']
    fishmeta = all_dfs['tbl_fish_sample_metadata']

    fishabud['tmp_row'] = fishabud.index
    fishdata['tmp_row'] = fishdata.index
    fishmeta['tmp_row'] = fishmeta.index

   
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
    

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]
    
    ############################### --Start of Logic Checks -- #############################################################
    print("Begin Fish Seines Logic Checks...")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'surveytype', 'netreplicate', 'projectid']

    # Logic Check 1: fish_sample_metadata & fish_abundance_data
    # Logic Check 1a: fishmeta records not found in fishabud
    print("Start logic check 1a")
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows": mismatch(fishmeta, fishabud, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, surveytype, netreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in sample_metadata must have corresponding records in abundance_data."
    })
    errs = [*errs, checkData(**args)]
    print("check 1a ran - logic - sample_metadata records not found in abundance_data") 

    # Logic Check 1b: fishmeta records missing for records provided by fishabud
    print("Start logic check 1b")
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": mismatch(fishabud, fishmeta, groupcols),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, surveytype, netreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in abundance_data must have corresponding records in sample_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check 1b ran - logic - sample_metadata records missing for records provided in abundance_data") 

    # Logic Check 2: fish_abundance_data & fish_length_data
    # Logic Check 2a: fishabud records not found in fishdata - Disabled by Duy 02/01/2023. NOTE: The checklist says this one should not be written
    # args.update({
    #     "dataframe": fishabud,
    #     "tablename": "tbl_fish_abundance_data",
    #     "badrows": checkLogic(fishabud, fishdata, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'surveytype', 'netreplicate', 'scientificname','commonname','status','projectid'], df1_name = "abundance_data", df2_name = "length_data"), 
    #     "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, surveytype, netreplicate, scientificname",
    #     "error_type": "Logic Error",
    #     "error_message": "Records in abundance_data must have corresponding records in length_data."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logic - abundance_data records not found in length_data")
 
    # Logic Check 2: fishabud records missing for records provided by fishdata
    print("Start logic check 2")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'surveytype', 'netreplicate', 'scientificname','projectid']
    
    args.update({
        "dataframe": fishdata,
        "tablename": "tbl_fish_length_data",
        "badrows": mismatch(fishdata, fishabud, groupcols),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, surveytype, netreplicate, scientificname",
        "error_type": "Logic Error",
        "error_message": "Records in length_data must have corresponding records in abundance_data."
    })
    errs = [*errs, checkData(**args)]
    print("check 2 ran - logic - abundance_data records missing for records provided in length_data") 
    
    print("End Fish Seines Logic Checks...")
    #############   END OF FISH LOGIC CHECKS ############################################################################################################################
    
    
    #############   START OF CUSTOM CHECKS ############################################################################################################################
    print("START Fish Seines custom Checks...")
   
    # Check 4: abundance range [0, 5000]
    print("Start custom check 4")
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows":fishabud[((fishabud['abundance'] < 0) | (fishabud['abundance'] > 5000)) & (fishabud['abundance'] != -88)].tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type" : "Value out of range",
        "error_message" : "Your abundance value must be between 0 to 5000. If this value is supposed to be empty, please fill with -88."
    })
    warnings = [*warnings, checkData(**args)]
    print("check 4 ran - tbl_fish_abundance_data - abundance range") # tested and working 5nov2021

    #NOTE time checks should be taken care of by Core check -Aria
    # commenting out time checks for now - zaib 28 oct 2021
    ## for time fields, in preprocess.py consider filling empty time related fields with NaT using pandas | check format of time?? | should be string

    # # Check: starttime format validation
    # timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    # badrows_starttime = fishmeta[
    #     fishmeta['starttime'].apply(
    #         lambda x: 
    #         not bool(re.match(timeregex, str(x))) 
    #         if not '-88' else 
    #         False
    #     )
    # ].tmp_row.tolist()
    # args.update({
    #     "dataframe": fishmeta,
    #     "tablename": "tbl_fish_sample_metadata",
    #     "badrows": badrows_starttime,
    #     "badcolumn": "starttime",
    #     "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - tbl_fish_sample_metadata - starttime format") # tested and working 9nov2021

    # # Check: endtime format validation
    # timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$"
    # badrows_endtime = fishmeta[fishmeta['endtime'].apply(lambda x: not bool(re.match(timeregex, str(x))) if not '-88' else False)].tmp_row.tolist()
    # args.update({
    #     "dataframe": fishmeta,
    #     "tablename": "tbl_fish_sample_metadata",
    #     "badrows": badrows_endtime,
    #     "badcolumn": "endtime",
    #     "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - tbl_fish_sample_metadata - endtime format") # tested and working 9nov2021


    # Check: starttime is before endtime --- crashes when time format is not HH:MM
    # Note: starttime and endtime format checks must pass before entering the starttime before endtime check
    # must be revised
    '''
    df = fishmeta[(fishmeta['starttime'] != '-88') & (fishmeta['endtime'] != '-88') & (fishmeta['endtime'] != -88)]
    print("subset df for time check: ")
    print(df)

    badrows = df[df['starttime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not '-88' else 'False') >= df['endtime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not '-88' else 'False')].tmp_row.tolist()
    if (len(badrows_starttime) == 0 & (len(badrows_endtime) == 0)):
        args.update({
            "dataframe": fishmeta,
            "tablename": "tbl_fish_sample_metadata",
            "badrows": badrows,
            "badcolumn": "starttime",
            "error_message": "Starttime value must be before endtime. Time should be entered in HH:MM format on a 24-hour clock."
            })
        errs = [*errs, checkData(**args)]
        print("check ran - tbl_fish_sample_metadata - starttime before endtime")

    del badrows_starttime
    del badrows_endtime
    '''

    # New checks written by Duy 10-04-22
    merged = pd.merge(
        fishabud,
        fishmeta, 
        how='left',
        suffixes=('', '_meta'),
        on = ['siteid','estuaryname','stationno','samplecollectiondate','surveytype','netreplicate']
    )

    # Check 5: if method = count, catch = yes in meta, then abundance is non zero integer in fish abundance tab
    print("Start custom check 5")
    # badrows = merged[(merged['abundance'] == 0) & (merged['method_meta'].apply(lambda x: str(x).lower().strip()) == 'count') & (merged['catch'].apply(lambda x: str(x).lower().strip()) == 'yes' )].tmp_row.tolist()
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": merged[(merged['abundance'] == 0) & (merged['method_meta'].apply(lambda x: str(x).lower().strip()) == 'count') & (merged['catch'].apply(lambda x: str(x).lower().strip()) == 'yes' )].tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Value Out of Range",
        "error_message": "If method = count, catch = yes in fish_meta, then abundance should be non zero integer in fish_abundance"
    })
    errs = [*errs, checkData(**args)]
    print("check 5 ran") 
    
    # Check 6: if method = pa, catch = yes in meta, then abundance is -88 in fish abundance tab
    print("Start custom check 6")
    badrows = merged[(merged['abundance'] != -88) & (merged['method_meta'].apply(lambda x: str(x).lower().strip()) == 'pa') & (merged['catch'].apply(lambda x: str(x).lower().strip()) == 'yes' )].tmp_row.tolist()
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": badrows,
        "badcolumn": "abundance",
        "error_type": "Value Out of Range",
        "error_message": "If method = pa, catch = yes in fish_meta, then abundance should be -88 in fish_abundance"
    })
    errs = [*errs, checkData(**args)]
    print("check 6 ran") 

    # Check 7: if catch = no in meta, then abundance is 0 in fish abundance
    print("Start custom check 7 ")     
    badrows = merged[(merged['abundance'] != 0) & (merged['catch'].apply(lambda x: str(x).lower().strip()) == 'no' )].tmp_row.tolist()
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": badrows,
        "badcolumn": "abundance",
        "error_type": "Value Out of Range",
        "error_message": "If catch = no in fish_meta, then abundance should be 0 in fish_abundance"
    })
    errs = [*errs, checkData(**args)]
    print("check 7 ran") 



    def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        
        for c in check_cols:
            df_to_check[c] = df_to_check[c].apply(lambda x: str(x).lower().strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())

        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].tmp_row.tolist()
        return(badrows)

    lookup_sql = f"SELECT * FROM lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    # Removing status part of multicolumn check for now as requested by Jan. 16 Dec 2021
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(fishabud, lu_species, check_cols, lookup_cols)
    
    # Check 8: multicolumn for species lookup
    print("Start custom check 8") 
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": multicol_lookup_check(fishabud, lu_species, check_cols, lookup_cols),
        "badcolumn":"commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
        
    })

    errs = [*errs, checkData(**args)]
    print("check 8 ran - fish_abundance_metadata - multicol species") 

    badrows = multicol_lookup_check(fishdata, lu_species, check_cols, lookup_cols)

    # # merged[(merged['abundance'] != 0) & (merged['catch'].apply(lambda x: str(x).lower().strip()) == 'no' )].tmp_row.tolist()
    #check 9: If abundance > 0, then there must be a corresponding record in length data (sort of a logic check)
    print("Start custom check 9") 
    
    # groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplelocation', 'scientificname', 'fieldreplicate', 'projectid']
    # args.update({
    #     "dataframe": fishabud,
    #     "tablename": "tbl_fish_abundance_data",
    #     "badrows": fishabud[(fishabud["abundance"] > 0)].tmp_row.tolist(),
    #     # & (mismatch(fishabud, fishdata, groupcols)),
    #     "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplelocation, scientificname, fieldreplicate, projectid",
    #     "error_type": "Logic Error",
    #     "error_message": "If fishabud > 0, then Records in fishabud must have corresponding records in fishdata. Missing records in fishdata."
    # })
    # errs = [*errs, checkData(**args)]

    print("check ran 9 - If abundance > 0, then there must be a corresponding record in length data") 

    # Check 10: multicolumn for species lookup
    print("Start custom check 10") 
    args.update({
        "dataframe": fishdata,
        "tablename": "tbl_fish_length_data",
        "badrows": multicol_lookup_check(fishdata, lu_species, check_cols, lookup_cols),
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species

    })
    errs = [*errs, checkData(**args)]
    print("check ran 10 - fish_length_metadata - multicol species") 

    #Check 11: If If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Time_Ele is required
    print("Check 11 fish seines begin:")
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows": fishmeta[(~fishmeta['elevation_ellipsoid'].isna() | ~fishmeta['elevation_orthometric'].isna()) & ( fishmeta['elevation_time'].isna() | (fishmeta['elevation_time'] == -88))].tmp_row.tolist(),
        "badcolumn": "elevation_time",
        "error_type": "Empty value",
        "error_message": "Elevation_time is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    })
    errs = [*errs, checkData(**args)]
    print('check 11 ran - ele_ellip or ele_ortho is reported then ele_time is required')

    #Check 12: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required
   
    print("begin check 12 fishsieness")
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows":fishmeta[(~fishmeta['elevation_ellipsoid'].isna() | ~fishmeta['elevation_orthometric'].isna()) & ( fishmeta['elevation_units'].isna() | (fishmeta['elevation_units'] == -88))].tmp_row.tolist(),
        "badcolumn": "elevation_units",
        "error_type": "Empty value",
        "error_message": "Elevation_units is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    })
    errs = [*errs, checkData(**args)]
    print('check 12 ran - ele_units required when ele_ellip and ele_ortho are reported')

    #Check 13: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Corr is required
    print('beging check 13 fishsienes:')
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows":fishmeta[(~fishmeta['elevation_ellipsoid'].isna() | ~fishmeta['elevation_orthometric'].isna()) & ( fishmeta['elevation_corr'].isna() | (fishmeta['elevation_corr'] == -88))].tmp_row.tolist(),
        "badcolumn": "elevation_corr",
        "error_type": "Empty value",
        "error_message": "Elevation_corr is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    })
    errs = [*errs, checkData(**args)]
    print('check 13 ran - ele_corr required when ele_ellip and ele_ortho are reported')

    #Check 14  If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Datum is required
    print('begin check 14 fishsienes:')
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows":fishmeta[(~fishmeta['elevation_ellipsoid'].isna() | ~fishmeta['elevation_orthometric'].isna()) & ( fishmeta['elevation_datum'].isna() | (fishmeta['elevation_datum'] == -88))].tmp_row.tolist(),
        "badcolumn": "elevation_datum",
        "error_type": "Empty value",
        "error_message": "Elevation_datum is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    })
    errs = [*errs, checkData(**args)]
    print('check 14 ran - elev_datum is required when ele_ellip and elev_ortho are reported')
    print("END Fish Seines custom Checks...")
    #############   END OF CUSTOM CHECKS ############################################################################################################################
    
    return {'errors': errs, 'warnings': warnings}