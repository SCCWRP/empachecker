# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch, get_primary_key, check_bad_time_format, check_bad_start_end_time, check_elevation_columns
import re
import time
import os
import geopandas as gpd


def global_custom(all_dfs, datatype = ''):

    # Having the datatype arg will make it easier for the latitude longitude checks
    # In main.py i passed it in
    # -- Robert 10/24/2023
    assert datatype != '', "In global_custom - datatype argument not defined"
    

    '''
        These checks apply to multiple datatypes. However, we need to carefully assert the tables to determine if a check is applicable to this datatype or not.
        We should not make any assumptions about the data, like assuming 'starttime' column exists in a dataframe.
    '''
    print("begin global custom checks")
    lu_list_script_root = current_app.script_root
    errs = []
    warnings = []

    lu_siteid = pd.read_sql('Select siteid, estuary FROM lu_siteid', g.eng)
    lu_plantspecies = pd.read_sql('Select scientificname, commonname, status from lu_plantspecies', g.eng)
    lu_fishmacrospecies = pd.read_sql('Select scientificname, commonname, status from lu_fishmacrospecies', g.eng)

    # the spatial_empa_sites is for the site map check
    spatial_empa_sites = gpd.read_postgis("SELECT * FROM spatial_empa_sites", g.eng, geom_col='geometry')
    spatial_empa_sites = spatial_empa_sites.to_crs(epsg=4326)
    for table_name in all_dfs:
        if all_dfs[table_name].empty:
            continue
        
        df = all_dfs[table_name]
        df['tmp_row'] = df.index
        #args = {
        #     "dataframe": pd.DataFrame({}),
        #     "tablename": '',
        #     "badrows": [],
        #     "badcolumn": "",
        #     "error_type": "",
        #     "is_core_error": False,
        #     "error_message": ""
        #}
        #errs = [*errs, checkData(**args)]
        
        if {"siteid", "estuaryname"}.issubset(df.columns):
            print("# GLOBAL CUSTOM CHECK - 1 Siteid/estuaryname pair must match lookup list")
            # Description: Siteid/estuaryname pair must match lookup list (multicolumn check)
            # Created Coder: Nick Lombardo
            # Created Date: 09/06/23
            # Last Edited Date: 
            # Last Edited Coder: 
            # NOTE (MM/DD/YY): 
            # NOTE (): 

            args = {
                "dataframe": df,
                "tablename": table_name,
                "badrows": mismatch(
                    df, 
                    lu_siteid, 
                    left_mergecols=["siteid", "estuaryname"],
                    right_mergecols=["siteid", "estuary"]
                ),
                "badcolumn": "siteid, estuaryname",
                "error_type": "Value Error",
                "is_core_error": False,
                "error_message": f'''
                    Siteid/estuaryname pair must match lookup list 
                    <a href="/{lu_list_script_root}/scraper?action=help&layer=lu_siteid" target="_blank">
                        lu_siteid
                    </a>.
                '''
            }
            errs = [*errs, checkData(**args)]

            print("# END GLOBAL CUSTOM CHECK - 1")


        if {"scientificname", "commonname", "status"}.issubset(df.columns): 
            print("# GLOBAL CUSTOM CHECK - 2 Scientificname/commoname/status combination for species must match lookup")
            # Description: Scientificname/commoname pair for species must match lookup
            # Created Coder: Nick Lombardo
            # Created Date: 09/06/23
            # Last Edited Date: 09/07/23
            # Last Edited Coder: Nick Lombardo
            # NOTE (09/07/23): Edited to add status to the subset check above, since it's really checking all 3 columns
            # NOTE (): 
            if table_name in ['tbl_algaecover_data','tbl_floating_data','tbl_vegetativecover_data','tbl_savpercentcover_data']:
                lu_df = lu_plantspecies
                lu_list = 'lu_plantspecies'
            else:
                lu_df = lu_fishmacrospecies
                lu_list = 'lu_fishmacrospecies'

            args = {
                "dataframe": df,
                "tablename": table_name,
                "badrows": mismatch(
                    df, 
                    lu_df, 
                    mergecols=["scientificname", "commonname", "status"]
                ),
                "badcolumn": "scientificname, commonname, status",
                "error_type": "Value Error",
                "is_core_error": False,
                "error_message": f'''
                    The scientificname-commonname-status entry did not match the lookup list 
                    <a href="/{lu_list_script_root}/scraper?action=help&layer={lu_list}" target="_blank">
                        {lu_list}
                    </a>.
                '''
            }
            errs = [*errs, checkData(**args)]

            print("# END GLOBAL CUSTOM CHECK - 2")
       
        time_columns = [x for x in df.columns if x.endswith("time")]
        if len(time_columns) > 0:
            print("# GLOBAL CUSTOM CHECK - 3 columns that end with 'time' should be entered in HH:MM format on 24-hour-clock")
            # Description: columns that end with 'time' should be entered in HH:MM format on 24-hour-clock
            # Created Coder: Nick Lombardo
            # Created Date: 09/06/23
            # Last Edited Date: 
            # Last Edited Coder: 
            # NOTE (): 
            # NOTE (): 
            for column in time_columns:
                args = {
                    "dataframe": df,
                    "tablename": table_name,
                    "badrows": df[df[column].apply(lambda x: check_bad_time_format(x))].tmp_row.tolist(),
                    "badcolumn": column,
                    "error_type": "Value Error",
                    "is_core_error": False,
                    "error_message": "Time values should be entered in HH:MM format on 24-hour-clock. If the time is missing, enter 'Not recorded'."
                }
                errs = [*errs, checkData(**args)]
                

            print("# END GLOBAL CUSTOM CHECK - 3")



        if {"starttime", "endtime"}.issubset(df.columns):
            print("# GLOBAL CUSTOM CHECK - 4 starttime must be before endtime")
            # Description: starttime must be before endtime
            # Created Coder: Nick Lombardo
            # Created Date: 09/07/23
            # Last Edited Date: 
            # Last Edited Coder: 
            # NOTE (): 
            # NOTE (): 
            args = {
                "dataframe": df,
                "tablename": table_name,
                # lambda function over the rows, use unpacking operator to give check_bad... function
                # both required positional arguments. works because we filter df to the two
                # time columns first, so that each row has exactly two values
                "badrows": df[df[['starttime', 'endtime']].apply(lambda times: check_bad_start_end_time(*times), axis = 1)].tmp_row.tolist(),
                "badcolumn": "starttime, endtime",
                "error_type": "Value Error",
                "is_core_error": False,
                "error_message": "Start time must be before end time."
            }
            errs = [*errs, checkData(**args)]
            print("# END GLOBAL CUSTOM CHECK - 4")



        if {'elevation_ellipsoid', 'elevation_orthometric'}.issubset(df.columns):
            print("# GLOBAL CUSTOM CHECK - 5 If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Time_Ele is required")
            # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Time_Ele is required
            # Created Coder: Nick Lombardo
            # Created Date: 09/07/23
            # Last Edited Date: 
            # Last Edited Coder: 
            # NOTE (): 
            # NOTE (): 
            args = {
                "dataframe": df,
                "tablename": table_name,
                "badrows": df[check_elevation_columns(df, 'elevation_time')].tmp_row.tolist(),
                "badcolumn": "elevation_time",
                "error_type": "Value Error",
                "is_core_error": False,
                "error_message": "If elevation_ellipsoid or elevation_orthometric is reported, then elevation_time is required"
            }
            errs = [*errs, checkData(**args)]
            print("# END GLOBAL CUSTOM CHECK - 5")




            print("# GLOBAL CUSTOM CHECK - 6 If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required")
            # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required
            # Created Coder: Nick Lombardo
            # Created Date: 09/07/23
            # Last Edited Date: 
            # Last Edited Coder: 
            # NOTE (): 
            # NOTE (): 
            args = {
                "dataframe": df,
                "tablename": table_name,
                "badrows": df[check_elevation_columns(df, 'elevation_units')].tmp_row.tolist(),
                "badcolumn": "elevation_units",
                "error_type": "Value Error",
                "is_core_error": False,
                "error_message": "If elevation_ellipsoid or elevation_orthometric is reported, then elevation_units is required"
            }
            errs = [*errs, checkData(**args)]
            print("# END GLOBAL CUSTOM CHECK - 6")




            print("# GLOBAL CUSTOM CHECK - 7 If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Corr is required")
            # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Corr is required
            # Created Coder: Nick Lombardo
            # Created Date: 09/07/23
            # Last Edited Date: 
            # Last Edited Coder: 
            # NOTE (): 
            # NOTE (): 
            args = {
                "dataframe": df,
                "tablename": table_name,
                "badrows": df[check_elevation_columns(df, 'elevation_corr')].tmp_row.tolist(),
                "badcolumn": "elevation_corr",
                "error_type": "Value Error",
                "is_core_error": False,
                "error_message": "If elevation_ellipsoid or elevation_orthometric is reported, then elevation_corr is required"
            }
            errs = [*errs, checkData(**args)]
            print("# END GLOBAL CUSTOM CHECK - 7")




            print("# GLOBAL CUSTOM CHECK - 8")
            # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Datum is required
            # Created Coder: Nick Lombardo
            # Created Date: 09/07/23
            # Last Edited Date: 
            # Last Edited Coder: 
            # NOTE (): 
            # NOTE (): 
            args = {
                "dataframe": df,
                "tablename": table_name,
                "badrows": df[check_elevation_columns(df, 'elevation_datum')].tmp_row.tolist(),
                "badcolumn": "elevation_datum",
                "error_type": "Value Error",
                "is_core_error": False,
                "error_message": "If elevation_ellipsoid or elevation_orthometric is reported, then elevation_datum is required"
            }
            errs = [*errs, checkData(**args)]
            print("# END GLOBAL CUSTOM CHECK - 8")


            print("# GLOBAL CUSTOM CHECK - 9")
            # Description: A (lat,long) for a siteid needs to be either in its associate polygon or within a mile if it is outside 
            # Created Coder: Duy
            # Created Date: 11/3/23
            # Last Edited Date:
            # Last Edited Coder:
            # NOTE (11/3/23): Created the check. Need to QA and this check does not consider 1 mile buffer.
            # NOTE (11/6/23): Fixed an error where sites in submitted file do not exist in the spatial_empa_sites table and cause null in geometry column after merging.
            latlong_cols = current_app.datasets.get(datatype).get('latlong_cols', None)

            # latlong_cols is a list of dictionaries of the tables with lat long columns
            if latlong_cols is not None:
                tmp = [
                    (x.get('latcol'), x.get('longcol'))
                    for x in latlong_cols
                    if x.get('tablename') == table_name
                ][0]
                latcol, longcol = tmp[0], tmp[1]

            meta = gpd.GeoDataFrame(
                df, 
                geometry=gpd.points_from_xy(df[longcol], df[latcol])
            )
            meta = meta.merge(
                spatial_empa_sites[['siteid','geometry']],
                how='left',
                on=['siteid'],
                suffixes=('_point', '_polygon')
            )
            meta_matched = meta[~meta['geometry_polygon'].isna()]
            meta_unmatched = meta[meta['geometry_polygon'].isna()]
            args = {
                "dataframe": df,
                "tablename": table_name,
                "badrows": meta_unmatched.tmp_row.tolist(),
                "badcolumn": f"{latcol}, {longcol}",
                "error_type": "Value Error",
                "is_core_error": False,
                "error_message": f"These sites ({','.join(list(set(meta_unmatched['siteid'])))}) were not checked if their locations are valid because their associated polygons were not created. Please contact Jan Walker (janw@sccwrp.org)"
            }
            warnings = [*warnings, checkData(**args)]

            args = {
                "dataframe": df,
                "tablename": table_name,
                "badrows": meta_matched[
                    meta_matched.apply(
                        lambda row: not row['geometry_point'].within(row['geometry_polygon']), 
                        axis=1
                    )
                ].tmp_row.tolist(),
                "badcolumn": f"{latcol}, {longcol}",
                "error_type": "Value Error",
                "is_core_error": False,
                "error_message": "Your points are not within their associated polygon. Please check the Map tab. If you believe their locations are correct, then ignore warnings and submit the data."
            }
            warnings = [*warnings, checkData(**args)]
            print("# END GLOBAL CUSTOM CHECK - 9")



    print("end global custom checks")
    return {'errors': errs, 'warnings': warnings}