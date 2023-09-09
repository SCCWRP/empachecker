# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch

def sav(all_dfs):
    
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
    
    savmeta = all_dfs['tbl_sav_metadata']
    savper = all_dfs['tbl_savpercentcover_data']

    savmeta['tmp_row'] = savmeta.index
    savper['tmp_row'] = savper.index
    
    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    
    args = {
        "dataframe":pd.DataFrame({}),
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
    #print("# CHECK - 1")
    # Description: Each metadata must include corresponding percentcoverdata
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 1")

    #print("# CHECK - 2")
    # Description: Each metadata must include corresponding percentcoverdata
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 2")
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Logic Checks --------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################





    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ SAV Metadata Checks --------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 3")
    # Description: Transectlength_m must be -88 or greater than or equal to 0 (positive)
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 3")

    #print("# CHECK - 4")
    # Description: Range for transectlength_m is expected within [0, 50] or -88
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 4")

    #print("# CHECK - 5")
    # Description: savbedreplicate must be consecutive within primary keys
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 5")

    #print("# CHECK - 6")
    # Description: transectreplicate must be consecutive within primary keys
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 6")


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF SAV Metadata Checks -------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################








    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ SAV PercentCoverData -------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 7")
    # Description: sdquadratreplicate must be consecutive within primary keys
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 7")

    #print("# CHECK - 8")
    # Description: pcquadratreplicate must be consecutive within primary keys
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - 8")



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF  SAV PercentCoverData ------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################






    '''
    #Function for multi column checks
    def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"
        
        lookup_df = lookup_df.assign(match="yes")
        #bug fix: read 'status' as string to avoid merging on float64 (from df_to_check) and object (from lookup_df) error
        df_to_check['status'] = df_to_check['status'].astype(str)
        
        for c in check_cols:
            df_to_check[c] = df_to_check[c].apply(lambda x: str(x).lower().strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())
        
        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)
    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    print("Begin SAV Logic Checks...")
    # Logic Check 1: sav_metadata & savpercentcover_data
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'savbedreplicate', 'transectreplicate', 'projectid']

    # Logic Check 1a: savmeta records not found in savper
    print('begin check 1a')
    args.update({
        "dataframe": savmeta,
        "tablename": "tbl_sav_metadata",
        "badrows": mismatch(savmeta, savper, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, savbedreplicate, transectreplicate,projectid",
        "error_type": "Logic Error",
        "error_message": "Records in SAV_metadata must have corresponding records in SAVpercentcover_data."
    })
    errs = [*errs, checkData(**args)]
    print("check 1a ran - logic - sav_metadata records not found in savpercent_data") 

    # Logic Check 1b: savmeta records missing for records provided by savper
    print('begin check 1b')
    args.update({
        "dataframe": savper,
        "tablename": "tbl_savpercentcover_data",
        "badrows": mismatch(savper, savmeta, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, savbedreplicate, transectreplicate,projectid",
        "error_type": "Logic Error",
        "error_message": "Records in SAVpercentcover_data must have corresponding records in SAV_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check 1b ran - logic - sav_metadata records missing for records provided in  savpercent_data") 

    print("End SAV Logic Checks...")
    ################################################################################################################################
    print("START SAV Custom Checks...")

    #check 2: Siteid/estuaryname pair must match lookup list (Ayah- 08/03/2023)
    print("Begin check 2: SAV Multicol Checks for matching SiteID to EstuaryName...")
    del badrows
    lookup_sql = f"SELECT * FROM lu_siteid;"
    lu_siteid = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['siteid','estuaryname']
    lookup_cols = ['siteid','estuary']
    badrows = multicol_lookup_check(savmeta, lu_siteid, check_cols, lookup_cols)
    args.update({
        "dataframe": savmeta,
        "tablename": "tbl_sav_metadata",
        "badrows": multicol_lookup_check(savmeta, lu_siteid, check_cols, lookup_cols),
        "badcolumn": "siteid,estuaryname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The siteid/estuaryname entry did not match the lu_siteid lookup list.'
                         '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_siteid" '
                        'target="_blank">lu_siteid</a>'
    })
    errs = [*errs, checkData(**args)]
    print("End check 2: SAV Multicol Checks for matching SiteID to EstuaryName...")
    
    #check 3: transectlength_m is nonnegative # tested
    print('begin check 3')
    args.update({
        "dataframe": savmeta,
        "tablename": "tbl_sav_metadata",
        "badrows":savmeta[(savmeta['transectlength_m'] < 0) & (savmeta['transectlength_m'] != -88)].tmp_row.tolist(),
        "badcolumn": "transectlength_m",
        "error_type" : "Value out of range",
        "error_message" : "Your transect length must be nonnegative."
    })
    errs = [*errs, checkData(**args)]
    print('end check 3')

    #check 4: transectlength_m range check [0, 50] #tested
    print('begin check 4')
    args.update({
        "dataframe": savmeta,
        "tablename": "tbl_sav_metadata",
        "badrows":savmeta[(savmeta['transectlength_m'] > 50) & (savmeta['transectlength_m'] != -88)].tmp_row.tolist(),
        "badcolumn": "transectlength_m",
        "error_type" : "Value out of range",
        "error_message" : "Your transect length exceeds 50 m. A value over 50 will be accepted, but is not expected."
    })
    warnings = [*warnings, checkData(**args)]
    print("check 4 ran - tbl_sav_metadata - transectlength range")

    #check 5: mulitcolumn check for species (scientificname, commonname, status) for tbl_savpercentcover_data
    print('start check 5')
    lookup_sql = f"SELECT * FROM lu_plantspecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(savper, lu_species, check_cols, lookup_cols)
    
    args.update({
        "dataframe": savper,
        "tablename": "tbl_savpercentcover_data",
        "badrows": multicol_lookup_check(savper, lu_species, check_cols, lookup_cols),
        "badcolumn": "commonname",
        "error_type" : "Multicolumn Lookup Error",
        "error_message" : f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_plantspecies" '
                        'target="_blank">lu_plantspecies</a>' # need to add href for lu_species
    })
    errs = [*errs, checkData(**args)]
    print("check 5 ran - savpercentcover_data - multicol species")
    print("END check 5")

    # #Check 6: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_time is required
    # print("Check 6 begin:")
    # args.update({
    #     "dataframe": savmeta,
    #     "tablename": "tbl_sav_metadata",
    #     "badrows": savmeta[(~savmeta['elevation_ellipsoid'].isna() | ~savmeta['elevation_orthometric'].isna()) & ( savmeta['elevation_time'].isna() | (savmeta['elevation_time'] == -88))].tmp_row.tolist(),
    #     "badcolumn": "elevation_time",
    #     "error_type": "Empty value",
    #     "error_message": "Elevation_time is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # })
    # errs = [*errs, checkData(**args)]

    # print('check 6 ran - ele_ellip or ele_ortho is reported then ele_time is required')

    # #Check 7: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required
    # print("Check 7 begin:")
    # args.update({
    #     "dataframe": savmeta,
    #     "tablename": "tbl_sav_metadata",
    #     "badrows": savmeta[(~savmeta['elevation_ellipsoid'].isna() | ~savmeta['elevation_orthometric'].isna()) & ( savmeta['elevation_units'].isna() | (savmeta['elevation_units'] == -88))].tmp_row.tolist(),
    #     "badcolumn": "elevation_units",
    #     "error_type": "Empty value",
    #     "error_message": "Elevation_units is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # })
    # errs = [*errs, checkData(**args)]

    # print('check 7 ran - ele_ellip or ele_ortho is reported then ele_units is required')

    # #Check 8: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Corr is required
    # print("Check 8 begin:")
    # args.update({
    #     "dataframe": savmeta,
    #     "tablename": "tbl_sav_metadata",
    #     "badrows": savmeta[(~savmeta['elevation_ellipsoid'].isna() | ~savmeta['elevation_orthometric'].isna()) & ( savmeta['elevation_corr'].isna() | (savmeta['elevation_corr'] == -88))].tmp_row.tolist(),
    #     "badcolumn": "Elevation_Corr",
    #     "error_type": "Empty value",
    #     "error_message": "Elevation_Corr is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # })
    # errs = [*errs, checkData(**args)]

    # print('check 8 ran - ele_ellip or ele_ortho is reported then Elevation_Corr is required')

    # #Check 9: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Datum is required
    # print("Check 9 begin:")
    # args.update({
    #     "dataframe": savmeta,
    #     "tablename": "tbl_sav_metadata",
    #     "badrows": savmeta[(~savmeta['elevation_ellipsoid'].isna() | ~savmeta['elevation_orthometric'].isna()) & ( savmeta['elevation_datum'].isna() | (savmeta['elevation_datum'] == -88))].tmp_row.tolist(),
    #     "badcolumn": "Elevation_Datum",
    #     "error_type": "Empty value",
    #     "error_message": "Elevation_Datum is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    # })
    # errs = [*errs, checkData(**args)]

    # print('check 9 ran - ele_ellip or ele_ortho is reported then Elevation_Datum is required')
    '''
    return {'errors': errs, 'warnings': warnings}