# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, mismatch, get_primary_key

def discretewq(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
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

    lu_list_script_root = current_app.script_root
    watermeta = all_dfs['tbl_waterquality_metadata']
    waterdata = all_dfs['tbl_waterquality_data']

    watermeta['tmp_row'] = watermeta.index
    waterdata['tmp_row'] = waterdata.index

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename":'',
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

    #Check Format: 
    #Description:
    # Created Coder:
    # Created Date:
    # Last Edited Date: (09/05/2023)
    # Last Edited Coder: 
    # NOTE (Date): Note

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ----------------------------------------- Discrete WQ Logic Checks ----------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    print("# CHECK - 1")
    #Description: Each metadata must include corresponding data (wq_metdata records not found in wq_data)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    # NOTE (10/05/23): Aria revised the error message
    watermeta_pkey = get_primary_key('tbl_waterquality_metadata', g.eng)
    waterdata_pkey = get_primary_key('tbl_waterquality_data', g.eng)
    watermeta_waterdata_shared_pkey = [x for x in watermeta_pkey if x in waterdata_pkey]

    args.update({
        "dataframe": watermeta,
        "tablename": "tbl_waterquality_metadata",
        "badrows": mismatch(watermeta, waterdata, watermeta_waterdata_shared_pkey),
        "badcolumn": ','.join(watermeta_waterdata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in waterquality_metadata must have a corresponding record in waterquality_data. Records are matched based on these columns: {}".format(
            ','.join(watermeta_waterdata_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("#END CHECK - 1")


    print("# CHECK - 2")
    #Description: Each data must include corresponding metadata(wq_metadata records missing for records provided by wq_data)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    # NOTE (10/05/23): Aria revised the error message
    args.update({
        "dataframe": waterdata,
        "tablename": "tbl_waterquality_data",
        "badrows": mismatch(waterdata, watermeta, watermeta_waterdata_shared_pkey), 
        "badcolumn": ','.join(watermeta_waterdata_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in waterquality_data must have a corresponding record in waterquality_metadata. Records are matched based on these columns: {}".format(
            ','.join(watermeta_waterdata_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END CHECK - 2")
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ End Discrete WQ Logic Check ------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    
    
    
    
    
    
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ WQ Metadata Custom Checks --------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    print('START CHECK 3')
    #Description: Depth_m must be greater than or equal to 0
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": watermeta,
        "tablename": 'tbl_waterquality_metadata',
        "badrows": watermeta[(watermeta['depth_m'] != -88) & (watermeta['depth_m'] < 0)].tmp_row.tolist(),
        "badcolumn": "depth_m",
        "error_type" : "Value Out of Range",
        "error_message" : "Your depth value must be larger than or equal to 0."
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 3')
    
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END WQ Metadata Custom Checks ----------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------  WQ Data Custom Checks ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    
    print('START CHECK 7')
    #Description:Range for conductivity, with conductivity_units as uS/cm, must be between [0, 100] or -88
    # Created Coder: NA
    # Created Date: 09/05/2023
    # Last Edited Date: 1/2/2024
    # Last Edited Coder: Duy
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    # NOTE (10/25/2023): Aria adjusted check description requested by Jan changed from [0,100] to [200,80,000]
    # NOTE (1/2/2024): requested by Jan changed from [200, 80000] to [200, 90000]

    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[            
            (waterdata['conductivity'] != -88) &
            (waterdata['conductivity_units'] == 'uS/cm') & 
            (~waterdata['conductivity'].between(200, 90000))
        ].tmp_row.tolist(), 
        "badcolumn": "conductivity",
        "error_type": "Value out of range",
        "error_message" : "If the conductivity unit is uS/cm, then conductivity values must be between 200 and 90000."
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 7')

    print('START CHECK 8')
    #Description:Range for tds, with tds_units as ppt, must be between [0, 100] or -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['tds'] != -88) &
            (waterdata['tds_units'] == 'ppt') & 
            (~waterdata['tds'].between(0,100))
        ].tmp_row.tolist(),
        "badcolumn": "tds",
        "error_type": "Value Out of range",
        "error_message" : "If the tds unit is ppt, then tds values must be between 0 and 100."
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 8')

    print('START CHECK 9')
    #Description: Range for ph_teststrip must be within [1, 14] or -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['ph_teststrip'] != -88) &
            (~waterdata['ph_teststrip'].between(1, 14)) 
        ].tmp_row.tolist(), 
        "badcolumn": "ph_teststrip",
        "error_type": "Value Out of range",
        "error_message" : "ph_teststrip values must be between 1 and 14"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 9')

    print('BEGIN CHECK 10')
    # Description: Range for ph_probe must be within [1, 14] or -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['ph_probe'] != -88) & 
            (~waterdata['ph_probe'].between(1, 14))
        ].tmp_row.tolist(),
        "badcolumn": "ph_probe",
        "error_type": "Value Out of range",
        "error_message" : "ph_probe values must be between 1 and 14"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 10')
    
    print('START CHECK 11')
    # Description: Range for salinity, with salinity_units as ppt, must be between [0, 100] or -88 
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    
    print('begin discretewq-custom-check 11')
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['salinity'] != -88) & 
            (waterdata['salinity_units'] == 'ppt') &
            (~waterdata['salinity'].between(0, 100))
        ].tmp_row.tolist(),
        "badcolumn": "salinity",
        "error_type": "Value Out of range",
        "error_message" : "salinity values must be between 0 and 100" 
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 11')

    print('START CHECK 12')
    # Description:  Range for do, with do_units as mg/l, must be within [0, 70] or -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Zaib Quraishi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    # NOTE (01/04/2024): Zaib increased range for do_mgl from [0, 60] to [0, 70] as requested by Jan.

    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['do_mgl'] != -88) & 
            (waterdata['do_units'].str.lower() == 'mg/l') &
            (~waterdata['do_mgl'].between(0, 70))
        ].tmp_row.tolist(),
        "badcolumn": "do_mgl",
        "error_type": "Value Out of range",
        "error_message" : "do_mgl values must be between 0 and 70."
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 12')

    print('START CHECK 13')
    # Description:  Range for airtemp, with airtemp_units as C, must be between [0, 100] or -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['airtemp'] != -88) &
            (waterdata['airtemp_units'].str.lower() == 'deg c') &
            (~waterdata['airtemp'].between(0, 100))
        ].tmp_row.tolist(),
        "badcolumn": "airtemp",
        "error_type": "Value Out of range",
        "error_message" : "If airtemp_unit is C, airtemp must be between 0 and 100."
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 13')

    print('BEGIN CHECK 14')
    # Description:  Range for h2otemp, with h2otemp_units as C, must be between [0, 50] or -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['h2otemp'] != -88) &
            (waterdata['h2otemp_units'].str.lower()== 'deg c') &
            (~waterdata['h2otemp'].between(0, 50))
        ].tmp_row.tolist(),
        "badcolumn": "h2otemp",
        "error_type": "Value Out of Range",
        "error_message" : "If h2otemp_unit is C, h2otemp values must be between 0 and 50."
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 14')

    
    print('BEGIN CHECK 15')
    # Description:  If H2OTemp is reported except when -88, then H2OTemp_Units cannot be 'Not Recorded'
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/30/23
    # Last Edited Coder: Duy
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    # NOTE (10/30/2023): Duy fixed the code's logic
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (
                (waterdata['h2otemp'] != -88) & (waterdata['h2otemp_units'].str.lower() == 'not recorded')
            ) |
            (
                (waterdata['h2otemp'] == -88) & (waterdata['h2otemp_units'].str.lower() != 'not recorded')
            )
        ].tmp_row.tolist(),
        "badcolumn": "h2otemp,h2otemp_units",
        "error_type": "Bad Value",
        "error_message" : "If h2otemp is reported, then h2otemp_units cannot be 'Not recorded'. If h2otemp is missing, please enter -88 for h2otemp and 'Not recorded' for h2otemp_units"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 15')    

    
    print('START CHECK 16')
    # Description: If AirTemp is reported except when -88, then AirTemp_Units cannot be 'Not Recorded'
    # Created Coder: NA
    # Created Date: 09/05/2023
    # Last Edited Date: 10/30/23
    # Last Edited Coder: Duy
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    # NOTE (10/25/2023): Aria adjusted the check and description based on jans request: to exclude when value is -88 since that has to be Not Recorded. check notes in product review document check16 for more information
    # NOTE (10/30/23): Duy fixed the code's logic
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (
                (waterdata['airtemp'] != -88) & (waterdata['airtemp_units'].str.lower() == 'not recorded')
            ) |
            (
                (waterdata['airtemp'] == -88) & (waterdata['airtemp_units'].str.lower() != 'not recorded')
            )
        ].tmp_row.tolist(),
        "badcolumn": "airtemp,airtemp_units",
        "error_type": "Bad Value",
        "error_message" : "If AirTemp is reported, then AirTemp_Units cannot be 'Not recorded'. If AirTemp is missing, please enter -88 for airtemp and 'Not recorded' for airtemp_units"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 16')  

    print('BEGIN CHECK 17')
    # Description: If DO_mgL is reported, then DO_Units cannot be 'Not Recorded' unless its -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/30/23
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    # NOTE (10/30/23): Duy fixed code's logic
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (
                (waterdata['do_mgl'] != -88) & (waterdata['do_units'].str.lower() == 'not recorded')
            ) |
            (
                (waterdata['do_mgl'] == -88) & (waterdata['do_units'].str.lower() != 'not recorded')
            )
        ].tmp_row.tolist(),
        "badcolumn": "do_mgl,do_units",
        "error_type": "Bad Value",
        "error_message" : "If do is reported, then do_Units cannot be 'Not recorded'. If do is missing, please enter -88 for do and 'Not recorded' for do_units"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 17')   

    print('START CHECK 18')
    # Description:  If Salinity is reported, then Salinity_Units cannot be 'Not Recorded' unless its -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/30/23
    # Last Edited Coder: Duy
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    # NOTE (10/30/23): Duy fixed code's logic
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (
                (waterdata['salinity'] != -88) & (waterdata['salinity_units'].str.lower() == 'not recorded')
            ) |
            (
                (waterdata['salinity'] == -88) & (waterdata['salinity_units'].str.lower() != 'not recorded')
            )
        ].tmp_row.tolist(),
        "badcolumn": "salinity,salinity_units",
        "error_type": "Bad Value",
        "error_message" : "If salinity is reported, then salinity_Units cannot be 'Not Recorded'. If salinity is missing, please enter -88 for salinity and 'Not recorded' for salinity_units"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 18') 

    print('START CHECK 19')
    # Description:  If TDS_ppt is reported, then TDS_Units cannot be 'Not Recorded' unless -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/30/23
    # Last Edited Coder: Duy
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    # NOTE (10/30/23): Duy fixed code's logic
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (
                (waterdata['tds'] != -88) & (waterdata['tds_units'].str.lower() == 'not recorded')
            ) |
            (
                (waterdata['tds'] == -88) & (waterdata['tds_units'].str.lower() != 'not recorded')
            )
        ].tmp_row.tolist(),
        "badcolumn": "tds,tds_units",
        "error_type": "Bad Value",
        "error_message" : "If tds is reported, then tds_Units cannot be 'Not recorded'. If tds is missing, please enter -88 for tds and 'Not recorded' for tds_units"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 19')   

    
    print('START CHECK 20')
    # Description:  If Conductivity is reported, then Conductivity_Units cannot be 'Not Recorded' unless -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/30/23
    # Last Edited Coder: Duy
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    # NOTE (10/30/23): Duy fixed code's logic
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (
                (waterdata['conductivity'] != -88) & (waterdata['conductivity_units'].str.lower() == 'not recorded')
            ) |
            (
                (waterdata['conductivity'] == -88) & (waterdata['conductivity_units'].str.lower() != 'not recorded')
            )
        ].tmp_row.tolist(),
        "badcolumn": "conductivity,conductivity_units",
        "error_type": "Bad Value",
        "error_message" : "If conductivity is reported, then conductivity_Units cannot be 'Not recorded'. If conductivity is missing, please enter -88 for conductivity and 'Not recorded' for conductivity_units"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 20')  

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END WQ Data Custom Checks -------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################


    print('START CHECK 21')
    # Description: pressure range check
    # Created Coder: Duy
    # Created Date: 12/27/2024
    # Last Edited Date:
    # Last Edited Coder:

    # Fetch the pressure unit lookup table
    lu_pressure_unit = pd.read_sql("SELECT * FROM lu_pressure_unit", g.eng)

    # Merge waterdata with lookup table to match units
    merged_data = pd.merge(
        waterdata,
        lu_pressure_unit,
        left_on='pressure_units',
        right_on='unit',
        how='left'
    )

    # Identify rows with invalid pressure values
    merged_data['pressure_valid'] = (
        ((merged_data['pressure_units'].str.lower() == 'not recorded') & (merged_data['pressure'] == -88)) |  # Special case
        (
            ~merged_data['pressure'].isnull() &                        # Ensure pressure is not null
            ~merged_data['min'].isnull() &                            # Ensure min is defined
            ~merged_data['max'].isnull() &                            # Ensure max is defined
            (merged_data['pressure'] >= merged_data['min']) &         # Check min bound
            (merged_data['pressure'] <= merged_data['max'])           # Check max bound
        )
    )

    # Find bad rows
    bad_rows = merged_data[~merged_data['pressure_valid']].tmp_row.tolist()

    # Update args with the bad rows
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": bad_rows,
        "badcolumn": "pressure,pressure_units",
        "error_type": "Out of Range",
        "error_message": "Pressure value is out of the allowable range for the specified unit, or pressure must be -88 if units are 'Not Recorded'."
    })
    errs = [*errs, checkData(**args)]

    print('END CHECK 21')




    print("-------------End of Discrete WQ Custom Check ----------")
    
    
    
    
    return {'errors': errs, 'warnings': warnings}