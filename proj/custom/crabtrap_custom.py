from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch, get_primary_key, check_replicate
import re

def crabtrap(all_dfs):
    
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
    
    crabmeta = all_dfs['tbl_crabtrap_metadata']
    crabinvert = all_dfs['tbl_crabfishinvert_abundance']
    crabmass = all_dfs['tbl_crabbiomass_length']

    crabmeta['tmp_row'] = crabmeta.index
    crabinvert['tmp_row'] = crabinvert.index
    crabmass['tmp_row'] = crabmass.index

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
    # ------------------------------------------------ Crab Logic Checks ----------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    crabmeta_pkey = get_primary_key('tbl_crabtrap_metadata', g.eng)
    crabinvert_pkey = get_primary_key('tbl_crabfishinvert_abundance', g.eng)
    crabmass_pkey = get_primary_key('tbl_crabbiomass_length', g.eng)

    crabmeta_crabinvert_shared_pkey = list(set(crabmeta_pkey).intersection(set(crabinvert_pkey)))
    crabmeta_crabmass_shared_pkey = list(set(crabmeta_pkey).intersection(set(crabmass_pkey)))
    crabinvert_crabmass_shared_pkey = list(set(crabinvert_pkey).intersection(set(crabmass_pkey)))
    print(f"crabmeta_crabinvert_shared_pkey: {crabmeta_crabinvert_shared_pkey}")
    print(f"crabmeta_crabmass_shared_pkey: {crabmeta_crabmass_shared_pkey}")
    print(f"crabinvert_crabmass_shared_pkey: {crabinvert_crabmass_shared_pkey}")

    print("# CHECK - 1")
    # Description: Each metadata must include corresponding abundance data (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Zaib Quraishi
    # NOTE (08/29/23): Zaib adjusts the format so it follows the coding standard.
    args.update({
        "dataframe": crabmeta,
        "tablename": "tbl_crabtrap_metadata",
        "badrows": mismatch(crabmeta, crabinvert, crabmeta_crabinvert_shared_pkey), 
        "badcolumn": ','.join(crabmeta_crabinvert_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each metadata must include corresponding abundance data"
    })
    errs = [*errs, checkData(**args)]
    # END OF CHECK - Each sample metadata must include corresponding abundance data (🛑 ERROR 🛑)
    print("# END OF CHECK - 1")



    print("# CHECK - 2")
    # Description: Each abundance data must include corresponding metadata (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/29/2023
    # Last Edited Coder: Zaib Quraishi
    # NOTE (08/29/23): Zaib adjusts the format so it follows the coding standard.
    args.update({
        "dataframe": crabinvert,
        "tablename": "tbl_crabfishinvert_abundance",
        "badrows": mismatch(crabinvert, crabmeta, crabmeta_crabinvert_shared_pkey), 
        "badcolumn": ','.join(crabmeta_crabinvert_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each abundance data must include corresponding metadata"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")



    print("# CHECK - 3")
    # Description: Each metadata must include corresponding length data (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/29/2023
    # Last Edited Coder: Zaib Quraishi
    # NOTE (08/29/23): Zaib adjusts the format so it follows the coding standard.
    args.update({
        "dataframe": crabmeta,
        "tablename": "tbl_crabtrap_metadata",
        "badrows": mismatch(crabmeta, crabmass, crabmeta_crabmass_shared_pkey), 
        "badcolumn": ','.join(crabmeta_crabmass_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each metadata must include corresponding length data"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")



    print("# CHECK - 4")
    # Description: Each length data must include corresponding metadata (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/29/2023
    # Last Edited Coder: Zaib Quraishi
    # NOTE (08/29/23): Zaib adjusts the format so it follows the coding standard.
    args.update({
        "dataframe": crabmass,
        "tablename": "tbl_crabbiomass_length",
        "badrows": mismatch(crabmass, crabmeta, crabmeta_crabmass_shared_pkey), 
        "badcolumn": ','.join(crabmeta_crabmass_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each length data must include corresponding metadata"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 4")


    print("# CHECK - 5")
    # Description: Each abundance data must include corresponding length data (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/29/2023
    # Last Edited Coder: Zaib Quraishi
    # NOTE (08/29/23): Zaib adjusts the format so it follows the coding standard.
    args.update({
        "dataframe": crabinvert,
        "tablename": "tbl_crabfishinvert_abundance",
        "badrows": mismatch(crabinvert, crabmass, crabinvert_crabmass_shared_pkey), 
        "badcolumn": ','.join(crabinvert_crabmass_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each abundance data must include corresponding length data"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 5")



    print("# CHECK - 6")
    # Description: Each length data data must include corresponding abundance data (🛑 ERROR 🛑)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/29/2023
    # Last Edited Coder: Zaib Quraishi
    # NOTE (08/29/23): Zaib adjusts the format so it follows the coding standard.
    args.update({
        "dataframe": crabmass,
        "tablename": "tbl_crabbiomass_length",
        "badrows": mismatch(crabmass, crabinvert, crabinvert_crabmass_shared_pkey), 
        "badcolumn": ','.join(crabinvert_crabmass_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each length data data must include corresponding abundance data"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 6")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END OF Crab Logic Checks --------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################







    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ----------------------------------------------------- Crab Trap Metadata Checks -------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
    print("# CHECK - 7")
    # Description: If trapsuccess is yes then catch must be either yes or no (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 10/04/2022
    # Last Edited Date: 8/29/2023
    # Last Edited Coder: Zaib Quraishi
    # NOTE (2/16/23): (Duy) Fixed this check. I made a mistake where I was trying to get the boolean values of a pandas Series by doing something like ([pd.Series] not in [values]). 
    # This yields the error the truth value is ambiguous. 
    # badrows = crabmeta[
    #             (crabmeta['trapsuccess'].apply(lambda x: str(x).strip().lower()) == 'yes') & 
    #     ([x not in ['yes','no'] for x in crabmeta['catch'].apply(lambda x: str(x).strip().lower()) ])
    # ].tmp_row.tolist()  
    # NOTE (8/29/23): Zaib adjusts the format so it follows the coding standard.
    args.update({
        "dataframe": crabmeta,
        "tablename": "tbl_crabtrap_metadata",
        "badrows": crabmeta[(crabmeta['trapsuccess'].apply(lambda x: str(x).strip().lower()) == 'yes') & ([x not in ['yes','no'] for x in crabmeta['catch'].apply(lambda x: str(x).strip().lower()) ])].tmp_row.tolist(),
        "badcolumn": "catch",
        "error_type": "Undefined Error",
        "error_message": "If trapsuccess is yes then catch must be either yes or no"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 7")



    print("# CHECK - 8")
    # Description: If trapsuccess is no then catch must be NULL or empty (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 10/04/2022
    # Last Edited Date: 8/29/2023
    # Last Edited Coder: Zaib Quraishi
    # NOTE (2/16/23): (Duy) Fixed this one too. Using the keyword 'no' to get the contradiction of a boolean series is not recommended. 
    # Use '~' instead.
    # NOTE (8/29/23): Zaib adjusts the format so it follows the coding standard.
    args.update({
        "dataframe": crabmeta,
        "tablename": 'tbl_crabtrap_metadata',
        "badrows": crabmeta[(crabmeta['trapsuccess'].apply(lambda x: str(x).strip().lower()) == 'no') & (~pd.isnull(crabmeta['catch']))].tmp_row.tolist(),
        "badcolumn": "catch",
        "error_type": "Undefined Error",
        "error_message": "If trapsuccess is no then catch must be NULL or empty"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 8")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ----------------------------------------- END OF Crab Trap Metadata Checks ----------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################



    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ----------------------------------------------- CrabFishInvert Abundance Checks -------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################
    print("# CHECK - 9")
    # Description: If catch is no then abundance should be 0 (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 10/04/2022
    # Last Edited Date: 8/29/2023
    # Last Edited Coder: Zaib Quraishi
    # NOTE (11/02/2022): (Robert) We cant use index to get the bad rows since we merged dataframes and the index gets reset
    # We must preserve the original index values of the dataframe that we are checking 
    #  so that we can correctly tell the user which rows are bad
    # So we will assign a temp column in this merged dataframe and use that for the "badrows"
    # NOTE (11/02/2022): Robert 2022-11-02 added .lower() to the merged['catch'] for the following reason
    # We cannot simply assume that the 'catch' column will be a lowercase 'no'
    # ESPECIALLY since that column is tied to the lookup list lu_yesno
    # It is actually impossible for the catch column to have a value of 'no' all lowercase, since lu_yesno has a capitalized 'Yes' and 'No'
    # That would have got flagged at core checks
    # NOTE (8/29/23): Zaib adjusts the format so it follows the coding standard.
    merged = pd.merge(
        crabinvert.assign(invert_tmp_row = crabinvert.index),
        crabmeta, 
        how='left',
        suffixes=('_abundance', '_meta'),
        on = ['siteid','estuaryname','traptype','samplecollectiondate', 'traplocation','stationno','replicate', 'projectid']
    )

    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        "badrows":  merged[(merged['catch'].str.lower() == 'no') & (merged['abundance'] > 0)].invert_tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "error_message": "If catch = No in crab_meta, then abundance shoud be 0 in invert_abundance tab"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 9")



    print("# CHECK - 10")
    # Description:  (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 10/04/2022
    # Last Edited Date: 8/29/2023
    # Last Edited Coder: Zaib Quraishi
    # NOTE (8/29/23): Zaib adjusts the format so it follows the coding standard.




    # New checks written by Duy - 2022-10-04
    
    # Check 6: If catch = No in meta, then abundance = 0 in abundance tab
    # Robert 2022-11-02
    # We cant use index to get the bad rows since we merged dataframes and the index gets reset
    # We must preserve the original index values of the dataframe that we are checking 
    #  so that we can correctly tell the user which rows are bad
    # So we will assign a temp column in this merged dataframe and use that for the "badrows"
    print('Start custom check 6')
    merged = pd.merge(
        crabinvert.assign(invert_tmp_row = crabinvert.index),
        crabmeta, 
        how='left',
        suffixes=('_abundance', '_meta'),
        on = ['siteid','estuaryname','traptype','samplecollectiondate', 'traplocation','stationno','replicate', 'projectid']
    )

    # Robert 2022-11-02 - added .lower() to the merged['catch'] for the following reason
    # We cannot simply assume that the 'catch' column will be a lowercase 'no'
    # ESPECIALLY since that column is tied to the lookup list lu_yesno
    # It is actually impossible for the catch column to have a value of 'no' all lowercase, since lu_yesno has a capitalized 'Yes' and 'No'
    # That would have got flagged at core checks
    # badrows = merged[(merged['catch'].str.lower() == 'no') & (merged['abundance'] > 0)].invert_tmp_row.tolist()
    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        "badrows":  merged[(merged['catch'].str.lower() == 'no') & (merged['abundance'] > 0)].invert_tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "error_message": "If catch = No in crab_meta, then abundance shoud be 0 in invert_abundance tab"
    })
    errs = [*errs, checkData(**args)]
    print("End Check 6: If catch is no in metadata then abundance should be 0 in crabinvert abundnace ")
    
    # Check 7: If catch = Yes in meta, then abundance is non-zero integer in abundance tab
    print('Start custom check 7')
    # badrows = merged[(merged['catch'].str.lower() == 'yes') & (merged['abundance'] <= 0)].invert_tmp_row.tolist()
    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        "badrows":  merged[(merged['catch'].str.lower() == 'yes') & (merged['abundance'] <= 0)].invert_tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "error_message": "If catch = Yes in crab_meta, then abundance should be a non-zero integer in abundance tab."
    })
    errs = [*errs, checkData(**args)]
    print("End Check 7: If catch = Yes in crab_meta, then abundance should be a non-zero integer in abundance tab. ") 

    #lookup lists
    print("before multicol check")
    def multicol_lookup_check(df_tocheck, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_tocheck.columns)), "columns do not exist in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        df_tocheck['status'] = df_tocheck['status'].astype(str)
        
        for c in check_cols:
            df_tocheck[c] = df_tocheck[c].apply(lambda x: str(x).lower().strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())

        merged = pd.merge(df_tocheck, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].tmp_row.tolist()
        return(badrows)


    lookup_sql = f"SELECT * FROM lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(crabinvert,lu_species, check_cols, lookup_cols)
    
    #check 9: Scientificname/commoname pair for species must match lookup
    print("Start check custom 9")
    args.update({
        "dataframe": crabinvert,
        "tablename": "tbl_crabfishinvert_abundance",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": "The scientificname/commonname entry did not match the lookup list."
                        '<a ' 
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
    })
    errs = [*errs, checkData(**args)]
    print("check  9 ran - crabfishinvert_abundance - multicol species") 
    badrows = multicol_lookup_check(crabmass, lu_species, check_cols, lookup_cols)

    #check 11: Scientificname/commoname pair for species must match lookup
    print("Start check custom 11")
    args.update({
        "dataframe": crabmass,
        "tablename": "tbl_crabbiomass_length",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list.'
                         '<a ' 
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species

    })
    errs = [*errs, checkData(**args)]
    print("check 11 ran - crabbiomass_length - multicol species") 

    # #Check 10: if abundance >0 then there needs to be a corresponding record in length:
    print("Start check custom 10")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'traptype', 'samplecollectiondate', 'scientificname', 'traplocation', 'replicate', 'projectid']
    crabinvert_filtered = crabinvert[crabinvert["abundance"] > 0]

    args.update({
        "dataframe": crabinvert,
        "tablename": "tbl_crabfishinvert_abundance",
        "badrows" : mismatch(crabinvert_filtered, crabmass, groupcols),
        "badcolumn": "siteid, estuaryname, stationno, traptype, samplecollectiondate, scientificname, traplocation, replicate, projectid",
        "error_type": "Logic Error",
        "error_message": "If fishabud > 0, then Records in crabinvert must have corresponding records in crabmass. Missing records in crabmass."
    })
    errs = [*errs, checkData(**args)]
    print("check ran 10 - If abundance > 0, then there must be a corresponding record in length data")
    ################################################






    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    ############################### --Start of Logic Checks -- #############################################################
    print("Begin Crab Trap Logic Checks...")

    # Logic Check 1: crabtrap_metadata & crabfishinvert_abundance
    # Logic Check 1a: crabmeta records not found in crabinvert
    print("Start logic check 1a")
    groupcols = ['siteid', 'estuaryname', 'samplecollectiondate', 'traptype', 'traplocation', 'stationno', 'replicate', 'projectid']
    args.update({
        "dataframe": crabmeta,
        "tablename": "tbl_crabtrap_metadata",
        "badrows": mismatch(crabmeta, crabinvert, groupcols), 
        "badcolumn": "siteid, estuaryname, traptype, traplocation, stationno, replicate",
        "error_type": "Logic Error",
        "error_message": "The Records in Crabtrap_metadata must have corresponding records in Crab_fish_invert_abundance."
    })
    errs = [*errs, checkData(**args)]
    print("check 1a ran - logic - crabtrap_metadata records not found in crabfishinvert_abundance") 

    # Logic Check 1b: crabmeta records missing for records provided by crabinvert
    print("Start logic check 1b")
    groupcols = ['siteid', 'estuaryname', 'samplecollectiondate', 'traptype', 'traplocation', 'stationno', 'replicate', 'projectid']
    args.update({
        "dataframe": crabinvert,
        "tablename": "tbl_crabfishinvert_abundance",
        "badrows": mismatch(crabinvert, crabmeta, groupcols), 
        "badcolumn": "siteid, estuaryname, traptype, traplocation, stationno, replicate",
        "error_type": "Logic Error",
        "error_message": "Records in Crab_fish_invert_abundance must have corresponding records in Crabtrap_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check 1b ran - logic - crabtrap_metadata records missing for records provided in crabfishinvert_abundance") 

    # Logic Check 2: crabfishinvert_abundance & crabbiomass_length
    #NOTE this is commented out check the Data Product Review doc SOP 10 - Aria 
    # # Logic Check 2a: crabinvert records not found in crabmass
    # # changed order of colnames based off template rather than pkey from db
    # # only check for matching records when abundance not equal 0
    # args.update({
    #     "dataframe": crabinvert,
    #     "tablename": "tbl_crabfishinvert_abundance",
    #     "badrows": checkLogic(crabinvert.query("abundance != 0"), crabmass, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'traptype', 'traplocation', 'replicate', 'scientificname', 'commonname', 'status', 'projectid'], df1_name = "Crab_fish_invert_abundance_data", df2_name = "Crab_biomass_length_data"), 
    #     "badcolumn": "siteid, estuaryname, samplecollectiondate, traptype, scientificname, traplocation, stationno, replicate",
    #     "error_type": "Logic Error",
    #     "error_message": "Records in Crab_fish_invert_abundance must have corresponding records in Crab_biomass_length."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logic - crabfishinvert_abundance records not found in crabbiomass_length")

    # Logic Check 2: crabinvert abundance records missing for records provided by crabmass length
    # changed ordering of colnames off template rather than db pkey fields
    print("Start logic check 2")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'traptype', 'traplocation', 'replicate', 'scientificname', 'commonname', 'status', 'projectid']
    args.update({
        "dataframe": crabmass,
        "tablename": "tbl_crabbiomass_length",
        "badrows": mismatch(crabmass, crabinvert, groupcols), 
        "badcolumn": "siteid, estuaryname, samplecollectiondate, traptype, scientificname, traplocation, stationno, replicate",
        "error_type": "Logic Error",
        "error_message": "Records in Crab_biomass_length must have corresponding records in Crab_fish_invert_abundance."
    })
    errs = [*errs, checkData(**args)]
    print("check 2 ran - logic - crabinvert records missing for records provided in crabmass") 

    print("End Crab Trap Logic Checks...")
    ####################    END of  LOGIC CHECKS FOR CRABTRAP   ######################################################################################################################

    ''' disabled check by Paul - doesn't make any sessions should be a combination of deployment date/time vs retrieve date/time - 3dec2021
    # Check: starttime format validation
    timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    # replacing null with -88 since data has been filled w/ -88
    #badrows_deploymenttime = crabmeta[crabmeta['deploymenttime'].apply(lambda x: not bool(re.match(timeregex, str(x))) if not pd.isnull(x) else False)].tmp_row.tolist()
    badrows_deploymenttime = crabmeta[crabmeta['deploymenttime'].apply(lambda x: not bool(re.match(timeregex, str(x))) if not '-88' else False)].tmp_row.tolist()
    args.update({
        "dataframe": crabmeta,
        "tablename": "tbl_crabtrap_metadata",
        "badrows": badrows_deploymenttime,
        "badcolumn": "deploymenttime",
        "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_crabtrapmeta_metadata - deploymenttime format") 

    badrows_retrievaltime = crabmeta[crabmeta['retrievaltime'].apply(lambda x: not bool(re.match(timeregex, str(x))) if not '-88' else False)].tmp_row.tolist()
    args.update({
        "dataframe": crabmeta,
        "tablename": "tbl_crabtrap_metadata",
        "badrows": badrows_retrievaltime,
        "badcolumn": "retrievaltime",
        "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_crabtrapmeta_metadata - retrievaltime format") 

    # Check: starttime is before endtime --- crashes when time format is not HH:MM
    # Note: starttime and endtime format checks must pass before entering the starttime before endtime check
    if (len(badrows_deploymenttime) == 0 & (len(badrows_retrievaltime) == 0)):
        args.update({
            "dataframe": crabmeta,
            "tablename": "tbl_crabtrap_metadata",
            "badrows": crabmeta[crabmeta['deploymenttime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not '-88' else '') >= crabmeta['retrievaltime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not '-88' else '')].tmp_row.tolist(),
            "badcolumn": "deploymenttime",
            "error_message": "Deploymenttime value must be before retrievaltime. Time should be entered in HH:MM format on a 24-hour clock."
            })
        errs = [*errs, checkData(**args)]
        print("check ran - tbl_crabtrap_metadata - deploymenttime before retrievaltime")

    del badrows_deploymenttime
    del badrows_retrievaltime
    ''' 
    '''
    args.update({
        "dataframe": crabmeta,
        "tablename": 'tbl_crabtrap_metadata',
        # I changed badrows to 'deploymenttime > retrivaltime' because of the error_message
        "badrows":crabmeta[crabmeta.deploymenttime > crabmeta.retrievaltime].tmp_row.tolist(),
        "badcolumn":"deploymenttime,retrievaltime",
        "error_type": "Date Value out of range",
        "error_message" : "Deployment time should be before retrieval time."
    })
    errs = [*errs, checkData(**args)]
    print('Finished: Compare deployment time to retrieval time')
    '''

    # End CRAB Logic Checks
    ############################## -- End of logic checks --####################################################################
    
    ############################### --Start of CrabTrap Metadata Checks -- #############################################################
    print("Begin CrabTrap Metadata Checks...")

    # check 8: Range for abundance must be between [0, 100]
    print("enter abundance check")
    print("Start custom check 8")
    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        #"badrows":crabinvert[((crabinvert['abundance'] < 0) | (crabinvert['abundance'] > 100) & (crabinvert['abundance'] != -88)].tmp_row.tolist(),
        "badrows": crabinvert[crabinvert['abundance'].apply(lambda x: ((x < 0) | (x > 100)) & (x != -88))].tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Value out of range",
        "error_message": "Your abundance value must be between 0 to 100."
    })
    errs = [*errs, checkData(**args)]
    print("check 8 ran - tbl_crabfishinvert_abundance - abundance check") 

  
    # New checks written by Duy - 2022-10-04
    # Check 4: if trapsuccess = yes in meta, then needs to be value for catch either yes or no
    print('Start custom check 4')
    # 02/16/23: (Duy) Fixed this check. I made a mistake where I was trying to get the boolean values of a pandas Series
    # by doing something like ([pd.Series] not in [values]). This yields the error the truth value is ambiguous. 
    # badrows = crabmeta[
    #             (crabmeta['trapsuccess'].apply(lambda x: str(x).strip().lower()) == 'yes') & 
    #     ([x not in ['yes','no'] for x in crabmeta['catch'].apply(lambda x: str(x).strip().lower()) ])
    # ].tmp_row.tolist()  
    args.update({
        "dataframe": crabmeta,
        "tablename": 'tbl_crabtrap_metadata',
        "badrows": crabmeta[(crabmeta['trapsuccess'].apply(lambda x: str(x).strip().lower()) == 'yes') & ([x not in ['yes','no'] for x in crabmeta['catch'].apply(lambda x: str(x).strip().lower()) ])].tmp_row.tolist(),
        "badcolumn": "catch",
        "error_type": "Undefined Error",
        "error_message": "If trapsuccess = yes, then needs to be value for catch (either yes or no)"
    })
    errs = [*errs, checkData(**args)]
    print("End Check 4: if trapsuccess = yes in meta, then needs to be value for catch either yes or no")
    
    # Check 5: if trapsuccess = no in meta, catch = Null
    # (Duy) Fixed this one too. Using the keyword 'no' to get the contradiction of a boolean series is not recommended. 
    # Use '~' instead.
    print('Start custom check 5')
    # badrows = crabmeta[
    #     (crabmeta['trapsuccess'].apply(lambda x: str(x).strip().lower()) == 'no') & (~pd.isnull(crabmeta['catch']))
    # ].tmp_row.tolist()  
    args.update({
        "dataframe": crabmeta,
        "tablename": 'tbl_crabtrap_metadata',
        "badrows": crabmeta[(crabmeta['trapsuccess'].apply(lambda x: str(x).strip().lower()) == 'no') & (~pd.isnull(crabmeta['catch']))].tmp_row.tolist()  ,
        "badcolumn": "catch",
        "error_type": "Undefined Error",
        "error_message": "If trapsuccess = no, catch needs to be empty"
    })
    errs = [*errs, checkData(**args)]
    print("End Check 5: If trapsuccess = no, catch needs to be empty")
    
    # Check 6: If catch = No in meta, then abundance = 0 in abundance tab
    # Robert 2022-11-02
    # We cant use index to get the bad rows since we merged dataframes and the index gets reset
    # We must preserve the original index values of the dataframe that we are checking 
    #  so that we can correctly tell the user which rows are bad
    # So we will assign a temp column in this merged dataframe and use that for the "badrows"
    print('Start custom check 6')
    merged = pd.merge(
        crabinvert.assign(invert_tmp_row = crabinvert.index),
        crabmeta, 
        how='left',
        suffixes=('_abundance', '_meta'),
        on = ['siteid','estuaryname','traptype','samplecollectiondate', 'traplocation','stationno','replicate', 'projectid']
    )

    # Robert 2022-11-02 - added .lower() to the merged['catch'] for the following reason
    # We cannot simply assume that the 'catch' column will be a lowercase 'no'
    # ESPECIALLY since that column is tied to the lookup list lu_yesno
    # It is actually impossible for the catch column to have a value of 'no' all lowercase, since lu_yesno has a capitalized 'Yes' and 'No'
    # That would have got flagged at core checks
    # badrows = merged[(merged['catch'].str.lower() == 'no') & (merged['abundance'] > 0)].invert_tmp_row.tolist()
    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        "badrows":  merged[(merged['catch'].str.lower() == 'no') & (merged['abundance'] > 0)].invert_tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "error_message": "If catch = No in crab_meta, then abundance shoud be 0 in invert_abundance tab"
    })
    errs = [*errs, checkData(**args)]
    print("End Check 6: If catch is no in metadata then abundance should be 0 in crabinvert abundnace ")
    
    # Check 7: If catch = Yes in meta, then abundance is non-zero integer in abundance tab
    print('Start custom check 7')
    # badrows = merged[(merged['catch'].str.lower() == 'yes') & (merged['abundance'] <= 0)].invert_tmp_row.tolist()
    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        "badrows":  merged[(merged['catch'].str.lower() == 'yes') & (merged['abundance'] <= 0)].invert_tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "error_message": "If catch = Yes in crab_meta, then abundance should be a non-zero integer in abundance tab."
    })
    errs = [*errs, checkData(**args)]
    print("End Check 7: If catch = Yes in crab_meta, then abundance should be a non-zero integer in abundance tab. ") 

    #lookup lists
    print("before multicol check")
    def multicol_lookup_check(df_tocheck, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_tocheck.columns)), "columns do not exist in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        df_tocheck['status'] = df_tocheck['status'].astype(str)
        
        for c in check_cols:
            df_tocheck[c] = df_tocheck[c].apply(lambda x: str(x).lower().strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())

        merged = pd.merge(df_tocheck, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].tmp_row.tolist()
        return(badrows)


    lookup_sql = f"SELECT * FROM lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(crabinvert,lu_species, check_cols, lookup_cols)
    
    #check 9: Scientificname/commoname pair for species must match lookup
    print("Start check custom 9")
    args.update({
        "dataframe": crabinvert,
        "tablename": "tbl_crabfishinvert_abundance",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": "The scientificname/commonname entry did not match the lookup list."
                        '<a ' 
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
    })
    errs = [*errs, checkData(**args)]
    print("check  9 ran - crabfishinvert_abundance - multicol species") 
    badrows = multicol_lookup_check(crabmass, lu_species, check_cols, lookup_cols)

    #check 11: Scientificname/commoname pair for species must match lookup
    print("Start check custom 11")
    args.update({
        "dataframe": crabmass,
        "tablename": "tbl_crabbiomass_length",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list.'
                         '<a ' 
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species

    })
    errs = [*errs, checkData(**args)]
    print("check 11 ran - crabbiomass_length - multicol species") 

    # #Check 10: if abundance >0 then there needs to be a corresponding record in length:
    print("Start check custom 10")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'traptype', 'samplecollectiondate', 'scientificname', 'traplocation', 'replicate', 'projectid']
    crabinvert_filtered = crabinvert[crabinvert["abundance"] > 0]

    args.update({
        "dataframe": crabinvert,
        "tablename": "tbl_crabfishinvert_abundance",
        "badrows" : mismatch(crabinvert_filtered, crabmass, groupcols),
        "badcolumn": "siteid, estuaryname, stationno, traptype, samplecollectiondate, scientificname, traplocation, replicate, projectid",
        "error_type": "Logic Error",
        "error_message": "If fishabud > 0, then Records in crabinvert must have corresponding records in crabmass. Missing records in crabmass."
    })
    errs = [*errs, checkData(**args)]
    print("check ran 10 - If abundance > 0, then there must be a corresponding record in length data")

    # print("# CHECK - 17")
    # Description: Replicate must be consecutive within a primary key  (🛑 ERROR 🛑)
    # Created Coder: Ayah H
    # Created Date: 09/01/2023
    # Last Edited Date: 
    # Last Edited Coder: Ayah H.
    # NOTE (09/01/2023): Ayah H. coded added Check 17.
    # NOTE (09/05/2023): Ayah checked that the check works 
    # NOTE (09/06/2023): Nick commented out so we could pull from github

    # crabmeta_pkey = list(get_primary_key('tbl_crabtrap_metadata', g.eng))
    # def check_replicate(tablename,rep_column,pkeys):
    #     badrows = []
    #     for _, subdf in tablename.groupby([x for x in pkeys if x != rep_column]):
    #             df = subdf.filter(items=[*pkeys,*['tmp_row']])
    #             df = df.sort_values(by=f'{rep_column}').fillna(0)
    #             rep_diff = df[f'{rep_column}'].diff().dropna()
    #             all_values_are_one = (rep_diff == 1).all()
    #             if not all_values_are_one:
    #                 badrows.extend(df.tmp_row.tolist())
    #     return badrows

    # args.update({
    #     "dataframe": crabmeta,
    #     "tablename": "tbl_crabtrap_metadata",
    #     "badrows" : check_replicate(crabmeta,'replicate',crabmeta_pkey)[0],
    #     "badcolumn": "replicate",
    #     "error_type": "Replicate Error",
    #     "error_message": f"Replicate must be consecutive within{','.join(check_replicate(crabmeta,'replicate',crabmeta_pkey)[1])}."
    # })
    # errs = [*errs, checkData(**args)]
    # print(" Check 17 ran - Replicate must be consecutive within a primary key.")

    # Disabling Time Check for Crab Trap to submit data.
    '''
    # Crab Trap Time Logic Check (Mina N.).....currently a work in progress
    ddate = crabmeta[crabmeta['deploymentdate']].dt.strftime('%Y-%m-%d')
    print(ddate)
    rdate = crabmeta[crabmeta['retrievaldate']].dt.strftime('%Y-%m-%d')
    dtime = crabmeta[crabmeta['deploymenttime']].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not pd.isnull(x) else "00:00:00")
    rtime = crabmeta[crabmeta['retrievaltime']].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not pd.isnull(x) else "00:00:00")

    # Merge date columns with respectvie time columns:
    deployment = ddate + ' ' + dtime
    ret = rdate + ' ' + rtime
    deployment = pd.to_datetime(deployment)
    retrieval = pd.to_datetime(ret)

    # Return rows if deployment is after retrieval
    badrows = crabmeta[crabmeta[deployment]][deployment > retrieval]

    args.update({
     "dataframe": crabmeta,
     "tablename": "tbl_crabtrap_metadata",
     "badrows": badrows,
     "badcolumn": "deploymentdate,deploymenttime,retrievaldate,retrievaltime",
     "error_message": "Values in 'deployment time' column is before values in 'retrieval time' column"
    
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_crabtrap_metadata - time logic check")
    '''

    return {'errors': errs, 'warnings': warnings}
