# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch
print('before vegatation funcntion')

def vegetation(all_dfs):
    print('vegatation funcntion')
    current_function_name = str(currentframe().f_code.co_name)
    lu_list_script_root = current_app.script_root
    print('inside vegatation funcntion')

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

    vegmeta['tmp_row'] = vegmeta.index
    vegdata['tmp_row'] = vegdata.index
    epidata['tmp_row'] = epidata.index

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

    ######################  LOGIC CHECKS #############################################################################################################
    print("Begin Vegetation Logic Checks..")
    # Note: Vegetation submission should always have vegetativecover_data. Metadata and vegetation data must have corresponding records.
    # Epifauna will sometimes, but not always be submitted. There may be only a subset of epifauna_data or none at all. - confirmed by Jan (26 May 2022)
    # Logic Check 1: sample_metadata & vegetativecover_data

    # Logic Check 1a: vegmeta records not found in vegdata
    print("begin logic check 1a sample_metadata records not found in vegetationcover_data ")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate', 'habitat', 'projectid']
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows": mismatch(vegmeta, vegdata, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, transectreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in sample_metadata must have corresponding records in vegetationcover_data."
    })
    errs = [*errs, checkData(**args)]
    print("check 1a ran - logic - sample_metadata records not found in vegetationcover_data") 


    # Logic Check 1b: vegmeta records missing for records provided by vegdata
    # Note: checkLogic() did not output badrows properly for Logic Check 1b. 
    print("begin logic check 1b: sample_metadata records missing for records provided in vegetationcover_data")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate', 'habitat', 'projectid']
    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows": mismatch(vegdata, vegmeta, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, transectreplicate, habitat, projectid",
        "error_type": "Logic Error",
        "error_message": "Records in vegetationcover_data must have corresponding records in sample_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check 1b ran - logic - sample_metadata records missing for records provided in vegetationcover_data") 

    # del badrows

    # Logic Check 2: epidata records have corresponding sample_metadata records (not vice verse since epifauna data may not always be collected)
    # aka sample_metadata records missing for records provided by epidata
    # checkLogic does not work properly for this df comparison - revised to use same approach as Logic Check 1b
    print("beging logic check 2:  sample_metadata records missing for records provided in epifauna_data ")
    tmp = epidata.merge(
        vegmeta.assign(present = 'yes'), 
        on = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate','projectid'],
        how = 'left'
    )
    tmp['tmp_row'] = tmp.index
    badrows = tmp[pd.isnull(tmp.present)].tmp_row.tolist()
   
    args.update({
        "dataframe": epidata,
        "tablename": "tbl_epifauna_data",
        "badrows": badrows,
        "badcolumn": "siteid, estuaryname, samplecollectiondate, stationno, transectreplicate, projectid",
        "error_type": "Logic Error",
        "error_message": "Records in epifauna_data must have corresponding records in sample_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check 2  ran - logic - sample_metadata records missing for records provided in epifauna_data") 
    del badrows

    print("End Vegetation Logic Checks..")

    #END VEGETATION LOGIC CHECK ##########################################################################################################

    ########################################################################################################################################
    #START VEGETATION CUSTOM CHECKS

    
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

    #Check 3: Siteid/estuaryname pair must match lookup list (multicolumn check) 

    print('begin vegetation check 3: Siteid/estuaryname pair must match lookup list (multicolumn check) ')
    lookup_sql = f"SELECT * FROM lu_siteid;"
    lu_siteid = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['siteid','estuaryname']
    lookup_cols = ['siteid','estuary']
    badrows = multicol_lookup_check(vegmeta, lu_siteid, check_cols, lookup_cols)
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows": multicol_lookup_check(vegdata, lu_siteid, check_cols, lookup_cols),
        "badcolumn": "siteid,estuaryname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The siteid/estuaryname entry did not match the lu_siteid lookup list.'
                         '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_siteid" '
                        'target="_blank">lu_siteid</a>' # need to add href for lu_species
    })
    errs = [*errs, checkData(**args)]
    print("check 3 ran - Siteid/estuaryname pair must match lookup list (multicolumn check) ") 
    
    #Check 4a: Range for coordinates for transectbeginlongitude must be greater than -114.043 or transectendlongitude must be less than -124.502 (within CA) 
    print('begin vegetaion check 4: Range for transectbeginlongitude > than -114.043 or transectendlongitude < -124.502 (within CA)')
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows":vegmeta[(vegmeta['transectbeginlongitude'] < -114.0430560959) | (vegmeta['transectendlongitude'] > -124.5020404709)].tmp_row.tolist(),
        "badcolumn": "transectbeginlongitude,transectendlongitude",
        "error_type" : "Value out of range",
        "error_message" : "Your longitude coordinates are outside of california, check your minus sign in your longitude data."
    })
    warnings = [*warnings, checkData(**args)]
    print(" end check 4a warning check for lat long")

    #Check 4b:Range for coordinates for transectbeginlatitude must be greater than 28 or transectendlongitude must be less than 41.992 (within CA including Baja)
    print('begin check 4b')
    # args.update({
    #     "dataframe": vegmeta,
    #     "tablename": "tbl_vegetation_sample_metadata",
    #     "badrows":vegmeta[(vegmeta['transectbeginlatitude'] < 32.5008497379) | (vegmeta['transectendlatitude'] > 41.9924715343)].tmp_row.tolist(),
    #     "badcolumn": "transectbeginlatitude,transectendlatitude",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your latitude coordinates are outside of california."
    # })
    # warnings = [*warnings, checkData(**args)]
    # print("end check 4b warning check for lat long")
    ##### end long and lat check 


    #Check 5: If method is obs_plant, then latitude/longitude is not required (if value is not plant then lat and long are req, but if it is plant then lat and long can be -88)
    # print('begin check 5 vegetation:')
    
    #end check 5 

    #Check 13: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Time_Ele is required
    print("Check 13 fish seines begin:")
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows": vegmeta[(vegmeta['elevation_ellipsoid'].notna() | vegmeta['elevation_orthometric'].notna()) & ( vegmeta['elevation_time'].isna() | (vegmeta['elevation_time'] == -88))].tmp_row.tolist(),
        "badcolumn": "elevation_time",
        "error_type": "Empty value",
        "error_message": "Elevation_time is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    })
    errs = [*errs, checkData(**args)]
    print('check 13 ran - ele_ellip or ele_ortho is reported then ele_time is required')
    
    #Check 14: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required
   
    print("begin check 13 vegetation")
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows":vegmeta[(vegmeta['elevation_ellipsoid'].notna() | vegmeta['elevation_orthometric'].notna()) & ( vegmeta['elevation_units'].isna() | (vegmeta['elevation_units'] == -88))].tmp_row.tolist(),
        "badcolumn": "elevation_units",
        "error_type": "Empty value",
        "error_message": "Elevation_units is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    })
    errs = [*errs, checkData(**args)]
    print('check 14 ran - ele_units required when ele_ellip and ele_ortho are reported')

    #Check 15: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Corr is required

    print('beging check 13 fishsienes:')
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows":vegmeta[(~vegmeta['elevation_ellipsoid'].isna() | ~vegmeta['elevation_orthometric'].isna()) & ( vegmeta['elevation_corr'].isna() | (vegmeta['elevation_corr'] == -88))].tmp_row.tolist(),
        "badcolumn": "elevation_corr",
        "error_type": "Empty value",
        "error_message": "Elevation_corr is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    })
    errs = [*errs, checkData(**args)]
    print('check 15 ran - ele_corr required when ele_ellip and ele_ortho are reported')

    #Check 15: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Datum is required
    print('begin check 16 vegetation:')
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows":vegmeta[(~vegmeta['elevation_ellipsoid'].isna() | ~vegmeta['elevation_orthometric'].isna()) & ( vegmeta['elevation_datum'].isna() | (vegmeta['elevation_datum'] == -88))].tmp_row.tolist(),
        "badcolumn": "elevation_datum",
        "error_type": "Empty value",
        "error_message": "Elevation_datum is required since Elevation_ellipsoid and/or Elevation_orthometric has been reported"
    })
    errs = [*errs, checkData(**args)]
    print('check 16 ran - elev_datum is required when ele_ellip and elev_ortho are reported')


    #Check 6: If EstimatedCover is -88, then PercentCoverCode must be provided (cannot be -88)
    print("Begin check 6 vegtation")

    args = ({
        "dataframe":vegdata,
        "tablename":'tbl_vegetativecover_data',
        "badrows":vegdata[(vegdata['estimatedcover'] == -88) & (vegdata['percentcovercode'] == -88)].tmp_row.tolist(),
        "badcolumn": "percentcovercode",
        "error_type": "wrong value",
        "is_core_error": False,
        "error_message": "Since EstimatedCover is -88, PercentCoverCOde must be provided"
    })
    errs = [*errs, checkData(**args)]
    print('check 6 ran - error check for estimatedcover and percentcovercode')

    #Check 7: EstimatedCover must be nonnegative. If no value, -88 is OK.
    print('Begin check 7 vegetation:')
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
    print('check 7 ran: estimated cover must be nonnegative')

    #Check 8: Multicolumn Lookup Check for EstimatedCover/PercentCoverCode/DaubenmireMidpoint
    print('begin check 8:')
    del badrows
    lookup_sql = f"SELECT * FROM lu_estimatedcover;"
    lu_estimatedcover = pd.read_sql(lookup_sql, g.eng)
    merged = pd.merge(vegdata, lu_estimatedcover, how="left", on=['daubenmiremidpoint','percentcovercode'])
    badrows = merged[(merged['estimatedcover_x'] < merged['estimatedcover_min' ]) | (merged['estimatedcover_x'] >= merged['estimatedcover_max'])].tmp_row.tolist()
   
    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows": badrows,
        "badcolumn": "estimatedcover",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The estimatedcover entry is out of range, refer to estimatedcover column in lu_percentcovercode .'
                         '<a '
                         f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_percentcovercode" '
                        'target="_blank">lu_percentcovercode</a>'
                        
    })
    errs = [*errs, checkData(**args)]
    print('check 8 ran')


    #Check 9: Range for tallestplantheight_cm must be between [0, 300]:
    print('Begin Check 9 vegetation:')
    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows":vegdata[(vegdata['tallestplantheight_cm']<0) | (vegdata['tallestplantheight_cm'] > 300)].tmp_row.tolist(),
        "badcolumn": "tallestplantheight_cm",
        "error_type" : "Value is out of range.",
        "error_message" : "Height should be between 0 to 3 metres"
    })
    warnings = [*warnings, checkData(**args)]
    print("check 9 ran- warning check for tallestplanheight_cm")

    #Check 10: Multicolumn Species Lookup Check for Scientificname/commoname pair for species must match lookup
    print('Begin check 10 vegetation:')
    
    lookup_sql = f"SELECT * FROM lu_plantspecies;"
    #lookup_sql = f"(SELECT * FROM lu_plantspecies) UNION (SELECT * FROM lu_fishmacrospecies);"
        # will not use union of the lu_lists because vegdata has plantspecies and epidata has fishmacrospecies (two separate multicolumn checks)
    lu_species = pd.read_sql(lookup_sql, g.eng)
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(vegdata, lu_species, check_cols, lookup_cols)
    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows": multicol_lookup_check(vegdata, lu_species, check_cols, lookup_cols),
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": "The scientificname/commonname entry did not match the lu_plantspecies lookup list."
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_plantspecies" '
                        'target="_blank">lu_plantspecies</a>' # need to add href for lu_species
    })

    errs = [*errs, checkData(**args)]
    print("check 10 ran - vegetativecover_data -Multicolumn Species Lookup Check for Scientificname/commoname pair for species must match lookup") 
    
    #Check 11: If burrows is "yes" then entered abundance must be greater than or equal to 0 and cannot be -88
    print('begin check 11 vegatation')
    args.update({
        "dataframe": epidata,
        "tablename": "tbl_epifauna_data",
        "badrows":epidata[(epidata['burrows'] == 'Yes') & (epidata['enteredabundance'].apply(lambda x: x < 0))].tmp_row.tolist(),
        "badcolumn": "enteredabundance",
        "error_type" : "Value out of range",
        "error_message" : "Your recorded entered abundance value must be greater than 0 and cannot be -88."
    })
    errs = [*errs, checkData(**args)]
    print("check 11 ran: error check for burrowns and enteredabundance")


    #Check 12: Multicolumn Species Lookup Check for Scientificname/commoname pair for species must match lookup
    print('begin check 12 vegetation:')
    del badrows
    lookup_sql = f"SELECT * FROM lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    badrows = multicol_lookup_check(epidata, lu_species, check_cols, lookup_cols)
    args.update({
        "dataframe": epidata,
        "tablename": "tbl_epifauna_data",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lu_fishmacrospecies lookup list.'
                         '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
    })

    errs = [*errs, checkData(**args)]
    print("check 12 ran - epifauna_data - multicol species") 

    




    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    print("End Vegetation Checks")
    
    return {'errors': errs, 'warnings': warnings}