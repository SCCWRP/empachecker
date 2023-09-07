# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch, get_primary_key, check_bad_time_format
import re
import time


def global_custom(all_dfs):
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


    for table_name in all_dfs:
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
                "error_message": "Siteid/estuaryname pair must match lookup list"
            }
            errs = [*errs, checkData(**args)]

            print("# END GLOBAL CUSTOM CHECK - 1")



        # do this for now cuz we need to double check benthic scientificname and commonname
        if {"scientificname", "commonname"}.issubset(df.columns) and table_name != 'tbl_benthicinfauna_abundance': 
            print("# GLOBAL CUSTOM CHECK - 2 Scientificname/commoname/status combination for species must match lookup")
            # Description: Scientificname/commoname pair for species must match lookup
            # Created Coder: Nick Lombardo
            # Created Date: 09/06/23
            # Last Edited Date: 
            # Last Edited Coder: 
            # NOTE (): 
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




        print("# GLOBAL CUSTOM CHECK - 4")
        # Description: starttime must be before endtime
        # Created Coder: 
        # Created Date: 
        # Last Edited Date: 
        # Last Edited Coder: 
        # NOTE (): 
        # NOTE (): 
        print("# END GLOBAL CUSTOM CHECK - 4")




        print("# GLOBAL CUSTOM CHECK - 5")
        # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Time_Ele is required
        # Created Coder: 
        # Created Date: 
        # Last Edited Date: 
        # Last Edited Coder: 
        # NOTE (): 
        # NOTE (): 
        print("# END GLOBAL CUSTOM CHECK - 5")




        print("# GLOBAL CUSTOM CHECK - 6")
        # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required
        # Created Coder: 
        # Created Date: 
        # Last Edited Date: 
        # Last Edited Coder: 
        # NOTE (): 
        # NOTE (): 
        print("# END GLOBAL CUSTOM CHECK - 6")




        print("# GLOBAL CUSTOM CHECK - 7")
        # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Corr is required
        # Created Coder: 
        # Created Date: 
        # Last Edited Date: 
        # Last Edited Coder: 
        # NOTE (): 
        # NOTE (): 
        print("# END GLOBAL CUSTOM CHECK - 7")




        print("# GLOBAL CUSTOM CHECK - 8")
        # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Datum is required
        # Created Coder: 
        # Created Date: 
        # Last Edited Date: 
        # Last Edited Coder: 
        # NOTE (): 
        # NOTE (): 
        print("# END GLOBAL CUSTOM CHECK - 8")



    print("end global custom checks")
    return {'errors': errs, 'warnings': warnings}