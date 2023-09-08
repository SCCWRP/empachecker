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




    print("# CHECK - 9")
    # Description: Replicate must be consecutive within a primary key  (🛑 ERROR 🛑)
    # Created Coder: Ayah H
    # Created Date: 09/01/2023
    # Last Edited Date: 
    # Last Edited Coder: Ayah H.
    # NOTE (09/01/2023): Ayah H. coded added Check 17.
    # NOTE (09/05/2023): Ayah checked that the check works 
    # NOTE (09/06/2023): Nick commented out so we could pull from github
    crabmeta_pkey = list(get_primary_key('tbl_crabtrap_metadata', g.eng))
    def check_replicate(tablename,rep_column,pkeys):
        badrows = []
        for _, subdf in tablename.groupby([x for x in pkeys if x != rep_column]):
                df = subdf.filter(items=[*pkeys,*['tmp_row']])
                df = df.sort_values(by=f'{rep_column}').fillna(0)
                rep_diff = df[f'{rep_column}'].diff().dropna()
                all_values_are_one = (rep_diff == 1).all()
                if not all_values_are_one:
                    badrows.extend(df.tmp_row.tolist())
        return badrows

    args.update({
        "dataframe": crabmeta,
        "tablename": "tbl_crabtrap_metadata",
        "badrows" : check_replicate(crabmeta,'replicate',crabmeta_pkey)[0],
        "badcolumn": "replicate",
        "error_type": "Replicate Error",
        "error_message": f"Replicate must be consecutive within{','.join(check_replicate(crabmeta,'replicate',crabmeta_pkey)[1])}."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 9")






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
    print("# CHECK - 10")
    # Description: If catch is no then abundance should be 0 (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 10/04/2022
    # Last Edited Date: 9/7/2023
    # Last Edited Coder: Robert Butler
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
    # NOTE (9/7/2023): assigned invert tmp row to be crabinvert tmp_row rather than crabinvert.index - Robert
    merged = pd.merge(
        crabinvert.assign(invert_tmp_row = crabinvert.tmp_row),
        crabmeta, 
        how='left',
        suffixes=('_abundance', '_meta'),
        on = ['siteid','estuaryname','traptype','samplecollectiondate', 'traplocation','stationno','replicate', 'projectid']
    )

    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        
        "badrows":  merged[
            (merged['catch'].str.lower() == 'no') & ( merged['abundance'] > 0 )  
        ].invert_tmp_row.tolist(),
        
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "error_message": "If catch = No in crab_meta, then abundance shoud be 0 in invert_abundance tab"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 10")

    

    print("# CHECK - 11")
    # Description: If catch is yes then abundance is non-zero integer (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 10/04/2022
    # Last Edited Date: 9/7/2023
    # Last Edited Coder: Robert Butler
    
    # NOTE: (2022-11-02) (Robert)
    # # # We cant use index to get the bad rows since we merged dataframes and the index gets reset
    # # # We must preserve the original index values of the dataframe that we are checking 
    # # #  so that we can correctly tell the user which rows are bad
    # # # So we will assign a temp column in this merged dataframe and use that for the "badrows"

    # NOTE:(2023-09-07) Robert adjusted it to match the QA doc and the new coding standard
    # NOTE:(2023-09-07) Robert added merged['abundance'].isnull() since it should also be an error if there is no abundance, but there was a catch
    # NOTE:(2023-09-07) Robert changed 
    # # # # crabinvert.assign(invert_tmp_row = crabinvert.index ) 
    # # # # to 
    # # # # crabinvert.assign(invert_tmp_row = crabinvert.tmp_row)

    merged = pd.merge(
        crabinvert.assign(invert_tmp_row = crabinvert.tmp_row),
        crabmeta, 
        how='left',
        suffixes=('_abundance', '_meta'),
        on = ['siteid','estuaryname','traptype','samplecollectiondate', 'traplocation','stationno','replicate', 'projectid']
    )
    
    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        "badrows":  merged[(merged['catch'].str.lower() == 'yes') & ( (merged['abundance'] <= 0) | merged['abundance'].isnull() )].invert_tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "error_message": "If catch = Yes in crab_meta, then abundance should be a positive integer in abundance tab."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 11")



    
    
    print("# CHECK - 12")
    # Description: Range for abundance must be between [0, 100] unless it is -88 (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 10/04/2022
    # Last Edited Date: 9/7/2023
    # Last Edited Coder: Robert Butler
    # NOTE (8/29/23): Robert adjusts the format so it follows the coding standard. (I also added the & pd.notnull(x) part)
    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        "badrows": crabinvert[crabinvert['abundance'].apply(lambda x: ((x < 0) | (x > 100)) & ( (x != -88) & pd.notnull(x) ) )].tmp_row.tolist(),
        "badcolumn": "abundance",
        "error_type": "Value out of range",
        "error_message": "Your abundance value must be between 0 to 100, unless it is a -88 indicating a missing value."
    })
    errs.append(checkData(**args))
    print("# END OF CHECK - 12")

    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ----------------------------------------------- END OF CrabFishInvert Abundance Checks ------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################









    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ----------------------------------------------- Crab Biomass Length Checks ------------------------------------------------------- #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################


    #print("# CHECK - 13")
    # Description: speciesreplicate must be consecutive within primary keys (🛑 ERROR 🛑)
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE ():
    #print("# END OF CHECK - 13")



    ######################################################################################################################################
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    # ----------------------------------------------- END OF Crab Biomass Length Checks ------------------------------------------------ #
    # ---------------------------------------------------------------------------------------------------------------------------------- #
    ######################################################################################################################################






    return {'errors': errs, 'warnings': warnings}
