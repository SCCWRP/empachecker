# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch,get_primary_key

def macroalgae(all_dfs):
    
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

    algaemeta = all_dfs['tbl_macroalgae_sample_metadata']
    algaecover = all_dfs['tbl_algaecover_data']
    algaefloating = all_dfs['tbl_floating_data']

    algaemeta['tmp_row'] = algaemeta.index
    algaecover['tmp_row'] = algaecover.index
    algaefloating['tmp_row'] = algaefloating.index

    algaemeta_pkey = get_primary_key('tbl_macroalgae_sample_metadata',g.eng)
    algaecover_pkey = get_primary_key('tbl_algaecover_data',g.eng)
    algaefloating_pkey = get_primary_key('tbl_floating_data',g.eng)

    algaemeta_algaecover_shared_pkey = list(set(algaemeta_pkey).intersection(algaecover_pkey))
    algaemeta_algaefloating_shared_pkey = list(set(algaemeta_pkey).intersection(algaefloating_pkey))

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
    # ------------------------------------------------ Macroalgae Logic Checks ----------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 1")
    # Description: Each metadata must include a corresponding coverdata
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 1")

    #print("# CHECK - 2")
    # Description: Each cover data must include a corresponding metadata
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 2")

    #print("# CHECK - 3")
    # Description: Each metadata must include a corresponding floating data
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 3")

    #print("# CHECK - 4")
    # Description: Each floating data must include a corresponding metadata
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 4")




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------END OF Macroalgae Logic Checks ----------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################








    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Sample Metadata Checks ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 5")
    # Description: Transectreplicate must be greater than 0
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 5")

    #print("# CHECK - 6")
    # Description: Transectlength_m must be greater than 0
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 6")

    #print("# CHECK - 7")
    # Description: Transectreplicate must be consecutive within primary keys
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 7")



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------END OF  Sample Metadata Checks ----------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Cover Data Checks ----------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 8")
    # Description: Transectreplicate must be greater than 0
    # Created Coder: 
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 8")

    #print("# CHECK - 9")
    # Description: Plotreplicate must be greater than 0
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 9")

    #print("# CHECK - 10")
    # Description: If covertype is "plant" then scientificname cannot be "Not recorded"
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 10")

    #print("# CHECK - 11")
    # Description: Plotreplicate must be consecutive within primary keys
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 11")



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------END OF Cover Data Checks ----------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################









    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Floating Data Checks -------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 12")
    # Description: If estimatedcover is 0 then scientificname must be "Not recorded"
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 12")


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Floating Data Checks ------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################




    '''
    # generalizing multicol_lookup_check
    def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        #bug fix: read 'status' as string to avoid merging on float64 (from df_to_check) and object (from lookup_df) error
        if 'status' in df_to_check.columns.tolist():
            df_to_check['status'] = df_to_check['status'].astype(str)
        
        for c in check_cols:
            df_to_check[c] = df_to_check[c].apply(lambda x: str(x).lower().strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())
        
        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].tmp_row.tolist()
        return(badrows)

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]
    print("----------------------START SAMPLE METADATA CHECKS---------------------------------")

    #check 1: Siteid/estuaryname pair must match lookup list
    print("Begin check 1: Macroalgae Multicol Checks for matching SiteID to EstuaryName...")
    lookup_sql = f"SELECT * FROM lu_siteid;"
    lu_siteid = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['siteid','estuaryname']
    lookup_cols = ['siteid','estuary']
    
    # Multicol - algaemeta
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": multicol_lookup_check(algaemeta,lu_siteid, check_cols, lookup_cols),
        "badcolumn":"siteid, estuaryname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The siteid/estuaryname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_siteid" '
                        'target="_blank">lu_siteid</a>'
        
    })
    print("check ran - multicol lookup, siteid and estuaryname - algaemeta")
    errs = [*errs, checkData(**args)]
    
    # Multicol - algaecover
    #check 8: Siteid/estuaryname pair must match lookup list
    print("begin check 8")
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": multicol_lookup_check(algaecover,lu_siteid, check_cols, lookup_cols),
        "badcolumn":"siteid, estuaryname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The siteid/estuaryname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_siteid" '
                        'target="_blank">lu_siteid</a>'
        
    })
    print("check ran - multicol lookup, siteid and estuaryname - algaecover")
    errs = [*errs, checkData(**args)]
    # Multicol - algaefloating
    #check 13: Siteid/estuaryname pair must match lookup list algaefloating
    print("begin check 13")
    args.update({
        "dataframe": algaefloating,
        "tablename": "tbl_floating_data",
        "badrows": multicol_lookup_check(algaefloating,lu_siteid, check_cols, lookup_cols),
        "badcolumn":"siteid, estuaryname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The siteid/estuaryname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_siteid" '
                        'target="_blank">lu_siteid</a>'
        
    })
    print("check ran - multicol lookup, siteid and estuaryname - algaefloating")
    errs = [*errs, checkData(**args)]

    print("End check 1, 8, and 13: Macroalgae Multicol Checks for matching SiteID to EstuaryName...")


    # Check 2: transectreplicate must be positive or -88 for tbl_macroalgae_sample_metadata
    print('begin check 2')
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": algaemeta[(algaemeta['transectreplicate'] <= 0) & (algaemeta['transectreplicate'] != -88)].tmp_row.tolist(),
        "badcolumn": "transectreplicate",
        "error_type" : "Value Error",
        "error_message" : "TransectReplicate must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - positive transectreplicate - algaemeta")
    print('end check 2')

    # Check 3: transectreplicate must be positive or -88 for tbl_algaecover_data
    print('begin check 3')
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": algaecover[(algaecover['transectreplicate'] <= 0) & (algaecover['transectreplicate'] != -88)].tmp_row.tolist(),
        "badcolumn": "transectreplicate",
        "error_type" : "Value Error",
        "error_message" : "TransectReplicate must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - positive transectreplicate - algaecover")
    print('end check 3')

    # #Check 16: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_time is required
    # print("Check 16 begin:")
    # args.update({
    #     "dataframe": algaemeta,
    #     "tablename": "tbl_macroalgae_sample_metadata",
    #     "badrows": algaemeta[(~algaemeta['elevation_ellipsoid'].isna() | ~algaemeta['elevation_orthometric'].isna()) & ( algaemeta['elevation_time'].isna() | (algaemeta['elevation_time'] == -88))].tmp_row.tolist(),
    #     "badcolumn": "elevation_time",
    #     "error_type": "Empty value",
    #     "error_message": "Elevation_time is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # })
    # errs = [*errs, checkData(**args)]

    # print('check 16 ran - ele_ellip or ele_ortho is reported then ele_time is required')

    # #Check 17: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required
    # print("Check 17 begin:")
    # args.update({
    #     "dataframe": algaemeta,
    #     "tablename": "tbl_macroalgae_sample_metadata",
    #     "badrows": algaemeta[(~algaemeta['elevation_ellipsoid'].isna() | ~algaemeta['elevation_orthometric'].isna()) & ( algaemeta['elevation_units'].isna() | (algaemeta['elevation_units'] == -88))].tmp_row.tolist(),
    #     "badcolumn": "elevation_units",
    #     "error_type": "Empty value",
    #     "error_message": "Elevation_units is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # })
    # errs = [*errs, checkData(**args)]

    # print('check 17 ran - ele_ellip or ele_ortho is reported then ele_units is required')

    # #Check 18: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Corr is required
    # print("Check 18 begin:")
    # args.update({
    #     "dataframe": algaemeta,
    #     "tablename": "tbl_macroalgae_sample_metadata",
    #     "badrows": algaemeta[(~algaemeta['elevation_ellipsoid'].isna() | ~algaemeta['elevation_orthometric'].isna()) & ( algaemeta['elevation_corr'].isna() | (algaemeta['elevation_corr'] == -88))].tmp_row.tolist(),
    #     "badcolumn": "Elevation_Corr",
    #     "error_type": "Empty value",
    #     "error_message": "Elevation_Corr is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # })
    # errs = [*errs, checkData(**args)]

    # print('check 18 ran - ele_ellip or ele_ortho is reported then Elevation_Corr is required')

    # #Check 19: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Datum is required
    # print("Check 19 begin:")
    # args.update({
    #     "dataframe": algaemeta,
    #     "tablename": "tbl_macroalgae_sample_metadata",
    #     "badrows": algaemeta[(~algaemeta['elevation_ellipsoid'].isna() | ~algaemeta['elevation_orthometric'].isna()) & ( algaemeta['elevation_datum'].isna() | (algaemeta['elevation_datum'] == -88))].tmp_row.tolist(),
    #     "badcolumn": "Elevation_Datum",
    #     "error_type": "Empty value",
    #     "error_message": "Elevation_Datum is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # })
    # errs = [*errs, checkData(**args)]

    # print('check 19 ran - ele_ellip or ele_ortho is reported then Elevation_Datum is required')

    # Check 10: plotreplicate must be positive or -88 for tbl_algaecover_data
    print('begin check 10')
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": algaecover[(algaecover['plotreplicate'] <= 0) & (algaecover['plotreplicate'] != -88)].tmp_row.tolist(),
        "badcolumn": "plotreplicate",
        "error_type" : "Value Error",
        "error_message" : "PlotReplicate must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("check 10 ran - positive plotreplicate - algaecover")


    #check 9: transectlength_m must be postive (> 0)
    print('begin check 9')
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": algaemeta[algaemeta['transect_length_m'] <= 0].tmp_row.tolist(),
        "badcolumn": "transect_length_m",
        "error_type" : "Value out of range",
        "error_message" : "Transect length must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("check 9 ran - positive transect_length_m - algaemeta")
    
   # return {'errors': errs, 'warnings': warnings}
   ################################################################################################################
    print("Begin Macroalgae Logic Checks...")
    # Logic Checks: sample_metadata & algaecover_data
    # Logic Check 4: algaemeta records not found in algaecover
    print('begin check 4')
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate']

    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": mismatch(algaemeta, algaecover, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, transectreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in sample_metadata must have corresponding records in Algaecover_data."
    })
    errs = [*errs, checkData(**args)]
    print("check 4 ran - logic - sample_metadata records not found in algaecover_data") 

    # Logic Check 5: algaemeta records missing for records provided by algaecover
    print('begin check 5')
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": mismatch(algaecover, algaemeta, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, transectreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in Algaecover_data must have corresponding records in sample_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check 5 ran - logic - sample_metadata records missing for records provided in algaecover_data") 

    #check 6: Each metadata must include a corresponding floating data
    print("begin check 6")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate', 'projectid']
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": mismatch(algaemeta, algaefloating, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, transectreplicate, projectid",
        "error_type": "Logic Error",
        "error_message": "Records in tbl_macroalgae_sample_metadata must have corresponding records in algaefloating. Missing records in algaefloating."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - missing algaefloating records")
    print("end check 6")

    #check 7: Each floating data must include a corresponding metadata
    print("begin check 7")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'scientificname', 'projectid']
    args.update({
        "dataframe": algaefloating,
        "tablename": "tbl_floating_data",
        "badrows": mismatch(algaefloating, algaemeta, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, scientificname, projectid",
        "error_type": "Logic Error",
        "error_message": "Records in tbl_floating_data must have corresponding records in algaemeta. Missing records in algaemeta."
    })
    errs = [*errs, checkData(**args)]
    print("check ran 7- logic - missing algaemeta records")
    print("end check 7")
 
    print("End Macroalgae Logic Checks...")
    ##########  - END OF LOGIC CHECKS  -   ###############################################################################################################################

    # check 11: CoverType & Species Check: if covertype is plant, then scientificname CANNOT be 'Not recorded'
    # if covertype is not plant, then scientificname can be 'Not recorded' - no check needs to be written for this one
    print('begin check 11')
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": algaecover[(algaecover['covertype'] == 'plant') & (algaecover['scientificname'] == 'Not recorded')].tmp_row.tolist(), 
        "badcolumn": "covertype, scientificname",
        "error_type": "Value Error",
        "error_message": "CoverType is 'plant' so the ScientificName must be a value other than 'Not recorded'."
    })
    errs = [*errs, checkData(**args)]
    print("check 11 ran - algaecover_data - covertype is plant, sciname must be an actual plant") 

    #check 12 : Scientificname/commoname pair for species must match lookup
    print('begin check 12')
    lookup_sql = f"SELECT * FROM lu_plantspecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(algaecover, lu_species, check_cols, lookup_cols)

    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": multicol_lookup_check(algaecover, lu_species, check_cols, lookup_cols),
        "badcolumn":"commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_plantspecies" '
                        'target="_blank">lu_plantspecies</a>' # need to add href for lu_species
        
    })

    errs = [*errs, checkData(**args)]
    print("check 12 ran - algeacover_data - multicol species") 

    # ALGAE FLOATING DATA CHECKS
    # check 14: If estimatedcover is 0 then scientificname must be "Not recorded"
    # EstimatedCover & ScientificName Check: if estimatedcover is 0, then scientificname MUST be 'Not recorded'
    print('begin check 14')
    args.update({
        "dataframe": algaefloating,
        "tablename": "tbl_floating_data",
        "badrows": algaefloating[(algaefloating['estimatedcover'] == 0) & (algaefloating['scientificname'] != 'Not recorded')].tmp_row.tolist(), 
        "badcolumn": "estimatedcover, scientificname",
        "error_type": "Value Error",
        "error_message": "EstimatedCover is 0. The ScientificName MUST be 'Not recorded'."
    })
    errs = [*errs, checkData(**args)]
    print("check 14 ran - floating_data - estimatedcover is 0, sciname must be NR") 


    #check 15: Scientificname/commoname pair for species must match lookup
    print('begin check 15')
    # badrows = multicol_lookup_check(algaefloating, lu_species, check_cols, lookup_cols)
    args.update({
        "dataframe": algaefloating,
        "tablename": "tbl_floating_data",
        "badrows":  multicol_lookup_check(algaefloating, lu_species, check_cols, lookup_cols),
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_plantspecies" '
                        'target="_blank">lu_plantspecies</a>' # need to add href for lu_species

    })

    errs = [*errs, checkData(**args)]
    print("check 15 ran - floating_data - multicol species") 

    '''
    return {'errors': errs, 'warnings': warnings}
