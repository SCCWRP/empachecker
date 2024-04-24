# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
import numpy as np 
from .functions import checkData, checkLogic, mismatch,get_primary_key, check_consecutiveness
print('before vegatation funcntion')

def vegetation(all_dfs):
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
    
    vegmeta = all_dfs['tbl_vegetation_sample_metadata']
    vegdata = all_dfs['tbl_vegetativecover_data']
    epidata = all_dfs['tbl_epifauna_data']
    cordgrass = all_dfs['tbl_cordgrass']

    vegmeta['tmp_row'] = vegmeta.index
    vegdata['tmp_row'] = vegdata.index
    epidata['tmp_row'] = epidata.index
    cordgrass['tmp_row'] = cordgrass.index

    vegmeta_pkey = get_primary_key('tbl_vegetation_sample_metadata',g.eng)
    vegdata_pkey = get_primary_key('tbl_vegetativecover_data',g.eng)
    epidata_pkey = get_primary_key('tbl_epifauna_data',g.eng)
    cordgrass_pkey = get_primary_key('tbl_cordgrass', g.eng)

    vegmeta_vegdata_shared_pkey = [x for x in vegmeta_pkey if x in vegdata_pkey]
    vegdata_vegmeta_shared_pkey = [x for x in vegdata_pkey if x in vegmeta_pkey]
    epidata_vegmata_shareed_pkey = [x for x in epidata_pkey if x in vegmeta_pkey]
    cordgrass_vegmeta_shareed_pkey = [x for x in cordgrass_pkey if x in vegmeta_pkey]
    cordgrass_vegdata_shareed_pkey = [x for x in cordgrass_pkey if x in vegdata_pkey]


    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    
    args = {
        "dataframe":pd.DataFrame({}),
        "tablename":'',
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

    print("# CHECK - 1")
    # Description: Each sample metadata must include corresponding cover data
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (9/19/2023): Updated the shared_pkeys, was matching the wrong keys with the wrong tables 
    # NOTE (10/05/2023): Aria revised the error message

    check_one_keys = vegmeta_vegdata_shared_pkey + ['habitat']

    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows": mismatch(vegmeta, vegdata, check_one_keys), 
        "badcolumn": ','.join(check_one_keys),
        "error_type": "Logic Error",
        "error_message": "Records in sample_metadata must have corresponding records in vegetationcover_data. "+\
            "Records are matched based on these columns: {}".format(
            ','.join(check_one_keys)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: Each cover data must include corresponding sample metadata
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (9/19/2023): Updated the shared_pkeys, was matching the wrong keys with the wrong tables 
    # NOTE (10/05/2023): Aria revised the error message

    check_two_keys = vegdata_vegmeta_shared_pkey + ['habitat']

    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows": mismatch(vegdata, vegmeta, check_two_keys), 
        "badcolumn": ','.join(check_two_keys),
        "error_type": "Logic Error",
        "error_message": "Records in vegetationcover_data should have the corresponding records in vegetation_sample_metadata based on these columns {}".format(
            ','.join(check_two_keys))
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")


    print("# CHECK - 3")
    # Description: Each epifauna data must include corresponding sample metadata
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/19/2023): Updated code, was not matching correct tables so changed "vegdata" to "vegmeta" which is what the checks wants. 
    # NOTE (10/05/2023): Aria revised the error message
    args.update({
        "dataframe": epidata,
        "tablename": "tbl_epifauna_data",
        "badrows": mismatch(epidata, vegmeta, epidata_vegmata_shareed_pkey),
        "badcolumn": ','.join(epidata_vegmata_shareed_pkey),
        "error_type": "Logic Error",
        "error_message": "Records in epifauna data must have corresponding records in vegetationcover_data based on these columns: {}".format(
            ','.join(epidata_vegmata_shareed_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")

    print("# CHECK - 23")
    # Description: Each sample metadata must include corresponding epifauna data
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 2/1/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (2/1/2024): Duy created the check
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows": mismatch(vegmeta, epidata, epidata_vegmata_shareed_pkey),
        "badcolumn": ','.join(epidata_vegmata_shareed_pkey),
        "error_type": "Logic Error",
        "error_message": "Each sample metadata must include corresponding epifauna data based on these columns: {}".format(
            ','.join(epidata_vegmata_shareed_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 23")

    print("# CHECK - 25")
    # Description: Each cordgrass data must include corresponding cover data
    # Created Coder: Caspian
    # Created Date: 2/16/2024
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE ():
    args.update({
        "dataframe": cordgrass,
        "tablename": "tbl_cordgrass",
        "badrows": mismatch(cordgrass, vegdata, cordgrass_vegdata_shareed_pkey),
        "badcolumn": ','.join(cordgrass_vegdata_shareed_pkey),
        "error_type": "Logic Error",
        "error_message": "Each cordgrass data must include corresponding vegdata based on these columns: {}".format(
            ','.join(cordgrass_vegdata_shareed_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 25")




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Logic Checks --------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################






    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Sample Metadata Checks ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 4")
    # Description: Range for coordinates for transectbeginlongitude must be greater than -114.043 or transectendlongitude must be less than -124.502 (within CA)
    # Created Coder:
    # Created Date: 
    # Last Edited Date: 09/29/2023
    # Last Edited Coder: Caspian
    # NOTE (09/14/2023): Adjust code to match coding standard
    # NOTE (09/29/2023): transectbeginlongitude has been removed from this table

    # args.update({
    #     "dataframe": vegmeta,
    #     "tablename": "tbl_vegetation_sample_metadata",
    #     "badrows":vegmeta[(vegmeta['transectbeginlongitude'] < -114.0430560959) | (vegmeta['transectendlongitude'] > -124.5020404709)].tmp_row.tolist(),
    #     "badcolumn": "transectbeginlongitude,transectendlongitude",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your longitude coordinates are outside of california, check your minus sign in your longitude data."
    # })
    # warnings = [*warnings, checkData(**args)]    
    print("# END OF CHECK - 4")

    print("# CHECK - 5")
    # Description: Range for coordinates for transectbeginlatitude must be greater than 28 or transectendlatitude must be less than 41.992 (within CA including Baja)
    # Created Coder:
    # Created Date: 
    # Last Edited Date: 09/29/2023
    # Last Edited Coder: Caspian
    # NOTE (09/14/2023): Adjust code to match coding standard
    # NOTE (09/29/2023): transectbeginlatitude has been removed from this table

    # args.update({
    #     "dataframe": vegmeta,
    #     "tablename": "tbl_vegetation_sample_metadata",
    #     "badrows":vegmeta[(vegmeta['transectbeginlatitude'] < 32.5008497379) | (vegmeta['transectendlatitude'] > 41.9924715343)].tmp_row.tolist(),
    #     "badcolumn": "transectbeginlatitude,transectendlatitude",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your latitude coordinates are outside of california."
    # })
    # warnings = [*warnings, checkData(**args)]


    print("# END OF CHECK - 5")

    print("# CHECK - 6")
    # Description: If value is not plant then lat and long are req, but if it is plant then lat and long can be -88
    # Created Coder: Aria Askaryar
    # Created Date: 10/04/2023
    # Last Edited Date: 10/04/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (10/04/2023): Created check 6

    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows": vegmeta[(vegmeta['method'] == 'obs_plant') & (((vegmeta['vegetated_cover'] != -88) & (vegmeta['vegetated_cover'].notna())) | ((vegmeta['non_vegetated_cover'] != -88) & (vegmeta['non_vegetated_cover'].notna())))].tmp_row.tolist(),
        "badcolumn": "vegetated_cover,non_vegetated_cover",
        "error_type": "Empty value",
        "error_message": "Method is obs_plant. Vegetated_cover and non_vegetated_cover must be -88 or empty."
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 6a")

    print("# CHECK - 6b")
    # Description: If method is not equal to obs_plant, combined vegetated_cover and non_vegetated_cover must equal 100
    # Created Coder: Caspian
    # Created Date: 10/6/2023
    # Last Edited Date: 10/6/2023
    # Last Edited Coder: Caspian

    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows": vegmeta[(vegmeta['method'] != 'obs_plant') & (vegmeta['vegetated_cover'] + vegmeta['non_vegetated_cover'] != 100)].tmp_row.tolist(),
        "badcolumn": "vegetated_cover,non_vegetated_cover",
        "error_type": "Empty value",
        "error_message": "Method is something other than obs_plant. vegetated_cover and non_vegetated_cover are required and must equal 100. If either vegetated_cover or non_vegetated_cover are 100, a 0 must be recorded for the other."
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 6b")

    print("# CHECK - 7")
    # Description: Transectreplicate must be consecutive within primary keys
    # Created Coder: Aria Askaryar
    # Created Date: 09/28/2023
    # Last Edited Date:  2/1/2024
    # Last Edited Coder: Duy
    # NOTE (09/28/2023): Aria wrote the check, it has not been tested yet
    # NOTE (2/1/2024): Should groupby these columns
    #groupby_cols = [x for x in vegmeta_pkey if x != 'transectreplicate']
    groupby_cols = ['projectid','siteid','estuaryname','stationno','samplecollectiondate']
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows" : check_consecutiveness(vegmeta, groupby_cols, 'transectreplicate'),
        "badcolumn": "transectreplicate",
        "error_type": "Replicate Error",
        "error_message": f"transectreplicate values must be consecutive. Records are grouped by {','.join(groupby_cols)}"
    })
    errs = [*errs, checkData(**args)]


    groupby_cols = ['projectid','siteid','estuaryname','stationno','samplecollectiondate','transectreplicate']
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows" : check_consecutiveness(vegmeta, groupby_cols, 'plotreplicate'),
        "badcolumn": "plotreplicate",
        "error_type": "Replicate Error",
        "error_message": f"plotreplicate values must be consecutive within a transect"
    })
    errs = [*errs, checkData(**args)]
    
    print("# END OF CHECK - 7")

    print("# CHECK - 15")
    # Description: If method is obs_plant, then transectreplicate must be -88
    # Created Coder: Duy    
    # Created Date: 10/30/23
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (10/30/23): Duy created the check
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows" : vegmeta[
            (vegmeta['method'] == 'obs_plant') & 
            (vegmeta['transectreplicate'] != -88)
        ].tmp_row.tolist(),
        "badcolumn": "method,transectreplicate",
        "error_type": "Value Error",
        "error_message": "If method is obs_plant, then transectreplicate must be -88"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 15")

    print("# CHECK - 16")
    # Description: If method is obs_plant, then habitat must be 'site-wide'
    # Created Coder: Duy
    # Created Date: 10/30/23
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (10/30/23): Duy created the check
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows" : vegmeta[
            (vegmeta['method'] == 'obs_plant') & 
            (vegmeta['habitat'].str.lower() != 'site-wide')
        ].tmp_row.tolist(),
        "badcolumn": "method,habitat",
        "error_type": "Value Error",
        "error_message": "If method is obs_plant, then habitat must be 'site-wide'"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 16")


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Sample Metadata Checks ----------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################









    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Vegetative Cover Data Checks ------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 8")
    # Description: If method is not 'obs_plant' and estimatedcover is -88, then percentcovercode must be provided (cannot be -88). If method is 'obs_plant', both estimatedcover and percentcover must be -88.
    # Created Coder:
    # Created Date:
    # Last Edited Date: 10/30/23
    # Last Edited Coder: Duy
    # NOTE (09/14/2023): Adjust code to match coding standard
    # NOTE (10/30/2023): Duy changed code's logic based on Jan's request
    args = ({
        "dataframe":vegdata,
        "tablename":'tbl_vegetativecover_data',
        "badrows":vegdata[
            (
                (vegdata['method'] != 'obs_plant') &
                (vegdata['estimatedcover'] == -88) &
                (vegdata['percentcovercode'] == -88) 
            ) |
            (
                (vegdata['method'] == 'obs_plant') &
                (
                    (vegdata['estimatedcover'] != -88) |
                    (vegdata['percentcovercode'] != -88)   
                ) 
            )
        ].tmp_row.tolist(),
        "badcolumn": "method,estimatedcover,percentcovercode",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If method is not 'obs_plant' and estimatedcover is -88, then percentcovercode must be provided (cannot be -88). If method is 'obs_plant', both estimatedcover and percentcover must be -88."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 8")

    print("# CHECK - 9")
    # Description: EstimatedCover must be nonnegative. If no value, -88 is OK.
    # Created Coder:
    # Created Date:
    # Last Edited Date: 09/14/2023
    # Last Edited Coder: Ayah
    # NOTE (09/14/2023): Adjust code to match coding standard
    args = ({
        "dataframe":vegdata,
        "tablename":"tbl_vegetativecover_data",
        "badrows":vegdata[(vegdata['estimatedcover'] <0) & (vegdata['estimatedcover'] != -88)].tmp_row.tolist(),
        "badcolumn": "estimatedcover",
        "error_type": "wrong value",
        "is_core_error": False,
        "error_message": "EstimatedCover must be non-negative, except when  no value exists then  -88 can be used."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 9")

    print("# CHECK - 11")
    # Description: Range for tallestplantheight_cm must be between [0, 300]
    # Created Coder:
    # Created Date:
    # Last Edited Date: 2/1/24
    # Last Edited Coder: Duy
    # NOTE (09/14/2023): Adjust code to match coding standard
    # NOTE (10/30/23): Duy adjusted the error msg
    # NOTE (10/31/23): Ignored -88 values
    # NOTE (2/1/24): Jan requested the max to be 450
    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows":vegdata[
            (vegdata['tallestplantheight_cm'] != -88) &
            (vegdata['tallestplantheight_cm'] < 0) | (vegdata['tallestplantheight_cm'] > 450)
        ].tmp_row.tolist(),
        "badcolumn": "tallestplantheight_cm",
        "error_type" : "Value is out of range.",
        "error_message" : "Height should be between 0 to 300 cm"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 11")

    print("# CHECK - 12")
    # Description: transectreplicate must be consecutive within primary keys
    # Created Coder: Aria Askaryar
    # Created Date: 09/28/2023
    # Last Edited Date:  11/1/23
    # Last Edited Coder: Duy Nguyen
    # NOTE (09/28/2023): Aria wrote the check, it has not been tested yet
    # NOTE (11/1/23): transecreplicate should be consecutive within a station for a given site 
    
    groupby_cols = ['projectid','siteid','estuaryname','stationno','samplecollectiondate']
    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows" : check_consecutiveness(vegdata, groupby_cols, 'transectreplicate'),
        "badcolumn": "transectreplicate",
        "error_type": "Replicate Error",
        "error_message": f"transectreplicate must be consecutive within primary keys (siteid, estuaryname, stationno, samplecollectiondate, transectreplicate, plotreplicate, covertype, scientificname, live_dead, unknownreplicate, projectid)"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 12")

    print("# CHECK - 13")
    # Description: plotreplicate must be consecutive within primary keys
    # Created Coder: Aria Askaryar
    # Created Date: 09/28/2023
    # Last Edited Date:  09/28/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/28/2023): Aria wrote the check, it has not been tested yet
    # NOTE (11/1/23): plotreplicate must be consecutive for a given transectreplicate
    
    groupby_cols = ['projectid','siteid','estuaryname','stationno','samplecollectiondate','transectreplicate']
    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows" : check_consecutiveness(vegdata, groupby_cols, 'plotreplicate'),
        "badcolumn": "plotreplicate",
        "error_type": "Replicate Error",
        "error_message": f"plotreplicate must be consecutive within primary keys (siteid, estuaryname, stationno, samplecollectiondate, transectreplicate, plotreplicate, covertype, scientificname, live_dead, unknownreplicate, projectid)"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 13")



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Vegetative Cover Data Checks ----------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################








    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Epifauna Data  Checks ------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################


    print("# CHECK - 14")
    # Description: If burrows is "yes" then entered abundance must be greater than or equal to 0 and cannot be -88
    # Created Coder: Caspian
    # Created Date:
    # Last Edited Date: 10/2/2023
    # Last Edited Coder: Caspian
    # NOTE (Date): Caspian created this check
    args.update({
        "dataframe": epidata,
        "tablename": "tbl_epifauna_data",
        "badrows":epidata[(epidata['burrows'] == 'Yes') & (epidata['enteredabundance'].apply(lambda x: x < 0))].tmp_row.tolist(),
        "badcolumn": "enteredabundance",
        "error_type" : "Value out of range",
        "error_message" : 'If burrows is "yes" then entered abundance must be greater than or equal to 0 and cannot be -88.'
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 14")



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Epifauna Data  Checks ------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ BEGIN cordgrass checks ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################


    print("# CHECK - 17")
    # Description: If scientificname in tbl_vegetativecover_data = Spartina foliosa, then at least 1 record expected in tbl_cordgrass
    # Created Coder: Aria Askaryar
    # Created Date: 12/13/2023
    # Last Edited Date: 12/13/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (12/13/2023): Aria - Ran through QA process and updated Doc
    # NOTE (01/25/2024): Zaib retested and validated this check for cordgrass since the schema was updated. 
    #                    QA doc has been updated. No changes made to check.
    filtered_vegdata = vegdata[(vegdata['scientificname'] == 'Spartina foliosa')]
    
    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows": mismatch(filtered_vegdata, cordgrass, cordgrass_vegdata_shareed_pkey), 
        "badcolumn": ','.join(cordgrass_vegdata_shareed_pkey),
        "error_type" : "Logic Error",
        "error_message" : 'If scientificname in tbl_vegetativecover_data = Spartina foliosa, then at least 1 record is expected to be in tbl_cordgrass based on these columns {}'.format(
            ','.join(cordgrass_vegdata_shareed_pkey))
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 17")

    print("# CHECK - 18")
    # Description: If total_stems is LESS THAN OR EQUAL TO 10, then the total number of plantheight_replicate must match the total_stems value.
    # Created Coder: Zaib Quraishi (groupby approach) | Aria Askaryar (totalstems_match_heights)
    # Created Date: 1/25/2024 | 12/13/2023
    # Last Edited Date: 1/26/2024
    # Last Edited Coder: Zaib Quraishi
    # NOTE (12/13/2023): Aria - Ran through QA process and updated Doc    
    # NOTE (12/18/2023): Aria - Changed totalstems_match_heights function to accept negative cases and empty rows when total_stems is defined 
    # NOTE (01/25/2024): Zaib disabled check. The schema for tbl_cordgrass has been changed. Multiple columns of the form 'plantheight_cm_{1, 2, ..., 10}'
    #                    has been changed to record the heights as replicates with the following columns: [plantheight_replicate, plantheight_cm]
    # NOTE (01/26/2024): Zaib - The original description of the check was the following "If total_stems < 10, then the number of heights should equal the number of stems; 
    #                    If total_stems > 10, then 10 heights expected." Check 18 is split into two checks: Check 18 and Check 19 to check the two cases separately.
    #                    Check has been tested and validated. QA doc had been updated.
    # NOTE (01/29/2024): Zaib modified groupby approach. Two grouped_dfs were created. Both grouped_df AND grouped_df_2 aggregate total_stems and tmp_row using max 
    #                    function. grouped_df aggs plantheight_replicate with count function, but grouped_df_2 aggs plantheight_replicate with max function. 
    #                    grouped_df is used to verify that the plantheight_replicate counts are checked against total_stems value entry (max).
    #                    grouped_df_2 is used to verify that the plantheight_replicate value entry is checked against total stems value entry (max).
    #                    Since both grouped dfs retain original tmp_row value, both grouped dfs conditions can be used to subset the badrows in grouped_df.
    #                    Check 18 is unaffected by this update, but is defined in this block. See Check 20.


    def totalstems_match_heights(df):
        bad_rows = []
        for index, row in df.iterrows():
            if row['total_stems'] < 0:
                bad_rows.append(index)
            elif row['total_stems'] >= 10:
                for i in range(1, 11):
                    column_name = f'plantheight_cm_{i}'
                    if pd.isna(row[column_name]):
                        bad_rows.append(index)
                        break  
            elif 0 < row['total_stems'] < 10:
                total_stems_int = int(row['total_stems'])
                for i in range(1, total_stems_int + 1):
                    column_name = f'plantheight_cm_{i}'
                    if pd.isna(row[column_name]):
                        bad_rows.append(index)
                        break
                else:  
                    for i in range(total_stems_int + 1, 11):
                        column_name = f'plantheight_cm_{i}'
                        if not pd.isna(row[column_name]):
                            bad_rows.append(index)
                            break
            elif row['total_stems'] == 0:
                # If total_stems is 0, all plantheight columns should be NaN
                if any(not pd.isna(row[f'plantheight_cm_{i}']) for i in range(1, 11)):
                    bad_rows.append(index)
        return bad_rows
    
    print(" # GROUP BY FOR CHECKS 18, 19, 20")

    cordgrass_group = ['projectid',
                       'siteid',
                       'estuaryname',
                       'stationno',
                       'samplecollectiondate',
                       'transectreplicate',
                       'plotreplicate',
                       'live_dead'
                       ]
    
    # Both grouped_df and grouped_df_2 retain original tmp_row of cordgrass dataframe to track where errors occur
    # grouped_df is defined by plantheight_replicate with count
    grouped_df = cordgrass.groupby(cordgrass_group).agg({'plantheight_replicate':'count',
                                           'total_stems':'max',
                                           'tmp_row':'max'}
                                        ).reset_index()
    
    # grouped_df_2 defined by plantheight_replicate with max
    grouped_df_2 = cordgrass.groupby(cordgrass_group).agg({'plantheight_replicate':'max',
                                           'total_stems':'max',
                                           'tmp_row':'max'}
                                        ).reset_index()
    
    print(" # END OF GROUP BY FOR CHECKS 18, 19, 20, 21")
    
    args.update({
        "dataframe": cordgrass,
        "tablename": "tbl_cordgrass",
        "badrows": grouped_df[(grouped_df['total_stems'] < 11) & 
                              (grouped_df['total_stems'] != 0) & (grouped_df['total_stems'] != -88) &
                              (grouped_df['total_stems'] != grouped_df['plantheight_replicate'])
                              ].tmp_row.to_list(),
        "badcolumn": 'total_stems',
        "error_type" : "Logic Error",
        "error_message" : 'If total_stems is LESS THAN OR EQUAL TO 10, then the total number of plantheight_replicate must match the total_stems value.'
    })
    errs = [*errs, checkData(**args)]


    print("# END OF CHECK - 18")

    print("# CHECK - 19")
    # Description: If total_stems is GREATER THAN 10, then the EXPECTED total number of plantheight_replicate must be AT LEAST 10.
    # Created Coder: Zaib Quraishi
    # Created Date: 1/26/23
    # Last Edited Date: 1/26/23
    # Last Edited Coder: Zaib Quraishi
    # NOTE (01/26/2024): Zaib - The original description of the check was the following "If total_stems < 10, then the number of heights should equal the number of stems; 
    #                    If total_stems > 10, then 10 heights expected." Check 18 is split into two checks: Check 18 and Check 19 to check the two cases separately.
    #                    This note was copied from Check 18.
    # NOTE (01/26/2024): Zaib tested and validated. QA doc had been updated.
    
    args.update({
        "dataframe": cordgrass,
        "tablename": "tbl_cordgrass",
        "badrows": grouped_df[(grouped_df['total_stems'] > 10) & (grouped_df['plantheight_replicate'] < 10)].tmp_row.to_list(),
        "badcolumn": 'total_stems',
        "error_type" : "Logic Error",
        "error_message" : 'If total_stems is GREATER THAN 10, then the EXPECTED total number of plantheight_replicate must be AT LEAST 10.'
    })
    errs = [*errs, checkData(**args)]


    print("# END OF CHECK - 19")


    print("# CHECK - 20")
    # Description: The total number of plantheight_replicate CANNOT exceed the number of total_stems
    # Created Coder: Zaib Quraishi
    # Created Date: 1/26/2024
    # Last Edited Date: 1/29/2024
    # Last Edited Coder: Zaib Quraishi
    # NOTE (1/26/2024): Zaib created an additional check based on one of the cases written in the totalstems_match_height() function written by Aria.
    # NOTE (1/29/2024): Zaib revised the check to consider the case where total_stems is -88 (plantheight_replicate is then expected to be -88). Two groupby dfs
    #                   are used to flag the badrows. See Check 18 for comments on grouped_df and grouped_df_2. Both grouped dfs are defined immediately after 
    #                   one another. 
    # NOTE (1/29/2024): Zaib tested and validated. QA doc has been updated.
    
    args.update({
        "dataframe": cordgrass,
        "tablename": "tbl_cordgrass",
        "badrows": grouped_df[(grouped_df['plantheight_replicate'] > grouped_df['total_stems']) &
                              (grouped_df_2['plantheight_replicate'] != grouped_df_2['total_stems'])
                              ].tmp_row.to_list(),
        "badcolumn": 'plantheight_replicate',
        "error_type" : "Logic Error",
        "error_message" : 'The total number of plantheight_replicate CANNOT exceed the number of total_stems. For the case where total_stems provided is -88, the expected plantheight_replicate should be -88.'
    })
    errs = [*errs, checkData(**args)]


    print("# END OF CHECK - 20")


    print("# CHECK - 22")
    # Description: total_stems is nonnegative, unless no value then -88 OK
    # Created Coder: Zaib Quraishi
    # Created Date: 1/26/2024
    # Last Edited Date: 1/26/2024
    # Last Edited Coder: Zaib Quraishi
    # NOTE (1/26/2024): Zaib created an additional check based on one of the cases written in the totalstems_match_height() function written by Aria.
    # NOTE (1/29/2024): Zaib tested and validated. QA doc had been updated.
    
    args.update({
        "dataframe": cordgrass,
        "tablename": "tbl_cordgrass",
        "badrows": cordgrass[(cordgrass['total_stems'] < 0) & (cordgrass['total_stems'] != -88)].tmp_row.to_list(),
        "badcolumn": 'total_stems',
        "error_type" : "Logic Error",
        "error_message" : 'total_stems MUST be nonnegative, unless there supposed to be no value (-88 OK)'
    })
    errs = [*errs, checkData(**args)]


    # print("# END OF CHECK - 22")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END cordgrass checks -------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
        

    return {'errors': errs, 'warnings': warnings}