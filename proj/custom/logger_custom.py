# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch
from .yeahbuoy_custom import yeahbuoy


def logger_raw(all_dfs):
    
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
    logger = all_dfs['tbl_wq_logger_raw']
    logger['tmp_row'] = logger.index
   
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

    # Temporary just to test logger data plotting
    # Ayah commented out temp -> return {'errors': errs, 'warnings': warnings}

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]
    
    # For the barometric pressure routine which apparently is not ready yet, or is not going to be put in the checker
    # df = yeahbuoy(df) if not df.empty else df

    print("Begin Logic Checks")

    # The only check we need to write here is to check that the login fields match a record in the database
    print("# The only check we need to write here is to check that the login fields match a record in the database")
    print("This has not been written yet")

    print("End logic checks.")
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------- Minidot/CTD/TrollShared Check --------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("Begin Minidot/CTD/TrollShared check  data check...")
    print('Begin check 1')
    # Description:Range for raw_h2otemp must be between [0, 100] or -88
    # Created Coder: Ayah
    # Created Date: 10/13/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (10/13/2023): Ayah edited check 
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['raw_h2otemp'] > 100) | ((logger['raw_h2otemp']!=-88) & (logger['raw_h2otemp'] < 0))].index.tolist(),
        "badcolumn": "raw_h2otemp",
        "error_type" : "Value out of range",
        "error_message" : "Your raw_h2otemp is out of range. Value should be within 0-100 degrees C."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_data - raw_h2otemp")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------- End of Minidot/CTD/TrollShared Check --------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ---------------------------------------------- Begin of Minidot Check -------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    print('Begin check 2')
    # Description: Issue warning if do_percent < 0 and not -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah created check 
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['raw_do_pct'] < 0) & (logger['raw_do_pct']!=-88) & (~pd.isnull(logger['raw_do_pct']) )].index.tolist(),
        "badcolumn": "raw_do_pct",
        "error_type" : "Value out of range",
        "error_message" : "Your raw_do_pct is negative. Value must be nonnegative and at least 0."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_mdot_data - do_percent")
    
    print('Begin check 3')
    #Description: Issue warning do_percent > 110 # Jan asked for this to be a warning. (Jan asked for this to be a warning. 4 March 2022)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah edited check 
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['raw_do_pct'] > 110)].index.tolist(),
        "badcolumn": "raw_do_pct",
        "error_type" : "Value out of range",
        "error_message" : "Your raw_do_pct is greater than 110. This is an unexpected value, but will be accepted."
    })
    warnings = [*warnings, checkData(**args)]
    print("check ran - logger_mdot_data - do_percent")
    
    print('Begin check 4')
    # Description: Range for raw_do  must be between [0, 60] or -88 when raw_do_unit is mg/L
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah edited check 
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[((logger['raw_do'] > 60) | ((logger['raw_do']!=-88) & (logger['raw_do'] < 0))) & (logger['raw_do_unit']== "mg/L")].index.tolist(),
        "badcolumn": "raw_do",
        "error_type" : "Value out  of range",
        "error_message" : "Your raw_do value is out of range. Value should be within 0-60 mg/L."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_mdot_data - raw_do")
    
    print('Begin check 5')
    # Description: qvalue range increased from 1 to 1.1 - approved by Jan 4 March 2022
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah edited check  
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['raw_qvalue'] > 1.1) | ((logger['raw_qvalue']!=-88) & (logger['raw_qvalue'] < 0))].index.tolist(),
        "badcolumn": "raw_qvalue",
        "error_type" : "Value out of range",
        "error_message" : "Your raw_qvalue is out of range. Must be within 0-1.1."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_mdot_data - qvalue")
    print("...End minidot data checks.")
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ----------------------------------------------- End of Minidot Check --------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ###################################################################################################################### 

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ----------------------------------------------- Begin of CTD Check ----------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ###################################################################################################################### 
          
    # Need to add not null checks for the measurement columns
    print("Begin CTD data checks...")

    print('Begin check 7')
    # Description: Range for raw_conductivity must be between [0, 10] or -88 when raw_conductiviy_units is mscm
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah edited check  
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(((logger['raw_conductivity'] < 0) & (logger['raw_conductivity'] != -88)) | (logger['raw_conductivity'] > 10)) & \
                              (logger['raw_conductivity_unit'] == "mS/cm")].index.tolist(),
        "badcolumn": "conductivity_mscm",
        "error_type" : "Value out of range",
        "error_message" : "Your conductivity_mscm value is out of range. Value must be within 0-10. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_ctd_data - conductivity_mscm")

    print('Begin check 9')
    # Description: Range for salinity_ppt must be nonnegative or-88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah edited check 
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['raw_salinity'] < 0) & (logger['raw_salinity'] != -88)].index.tolist(),
        "badcolumn": "raw_salinity",
        "error_type" : "Negative value",
        "error_message" : "Your raw_salinity value is less than 0. Value should be nonnegative. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_ctd_data - raw_salinity")

    # # Need to add not null checks for the measurement columns
    print('Begin check 10')
    # Description: If sensortype is CTD, then raw_pressure should be filled and raw_pressure_unit should be cmh2o
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah edited check 
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[
            (logger['sensortype'] == 'CTD') & (logger['raw_pressure'].isna() | logger['raw_pressure_unit']!= "cmh2o")
        ].tmp_row.tolist(),
        "badcolumn": "sensortype,raw_pressure,raw_pressure_unit",
        "error_type" : "Unknown Error",
        "error_message" : 'Since sensortype is CTD, raw_pressure should not be empty and raw_pressure_units must be "cmh2o"'
    })
    errs = [*errs, checkData(**args)]

    print("check ran - wqlogger - pressure_cmh2o")
    print("...End CTD data checks.")
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ End of CTD Check ------------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Begin of Troll Check -------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    
    print('Begin check 12')
    # Description: If sensortype is CTD, then raw_pressure should be filled and raw_pressure_unit should be cmh2o
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah edited check 
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[
            (logger['sensortype'] == 'Troll') & (logger['raw_pressure'].isna() | logger['raw_pressure_unit']!= "cmh2o")
        ].tmp_row.tolist(),
        "badcolumn": "sensortype,raw_pressure,raw_pressure_unit",
        "error_type" : "Unknown Error",
        "error_message" : 'Since sensortype is CTD, raw_pressure should not be empty and raw_pressure_units must be "cmh2o"'
    })
    errs = [*errs, checkData(**args)]

    print("check ran - wqlogger - pressure_cmh2o")
    print("...End CTD data checks.")

    # Need to add not null checks for the measurement columns

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------- End of Troll Check --------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------- Begin of Tidbit Check ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    print("Begin Tidbit data checks...")
    #Only check for tidbit was for h2otemp which is already coded
    print("...End Tidbit data checks.")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------- Begin of Other  Check ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    print("Begin Other data checks...")

    print('Begin check 14')
    # Description: Range for pH must be between [1, 14] or -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah edited check 
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['raw_ph'] < 1) | (logger['raw_ph'] > 14)].index.tolist(),
        "badcolumn": "ph",
        "error_type" : "Value out of range",
        "error_message" : "pH value is out of range. Value should be between 1 and 14. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - pH")


    print('Begin check 15')
    # Description: Range for turbidity_ntu must be between [0, 3000] or -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah edited check 
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['raw_turbidity_unit']=="ntu") & (((logger['raw_turbidity'] < 0) & (logger['raw_turbidity'] != -88)) | (logger['raw_turbidity'] > 3000))].index.tolist(),
        "badcolumn": "turbidity_ntu",
        "error_type" : "Value out of range",
        "error_message" : "Turbidity_NTU value is out of range. Value should be within 0-3000. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - turbidity_ntu")

    print('Begin check 16')
    # Description: Range for do_percent must be between [-1, 300] or -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah edited check 
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['sensortype']!='minidot') & ((logger['raw_do_pct'] < -1) & (logger['raw_do_pct'] != -88)) | (logger['raw_do_pct'] > 300)].index.tolist(),
        "badcolumn": "do_percent",
        "error_type" : "Value out of range",
        "error_message" : "raw_do_pct is out of range. Value must be within 0-300. If no value, enter -88 or leave blank."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - do_percent")
    
    print('Begin check 16')
    # Description: Range for raw_orp must be between [999,-999] when raw_orp_unit is mv
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 10/17/2023
    # Last Edited Coder: Ayah
    # NOTE (10/13/2023): Ayah edited check 
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['raw_orp_unit']=='mv') & ((logger['raw_orp'] < -999) | (logger['raw_orp'] > 999))].index.tolist(),
        "badcolumn": "orp_mv",
        "error_type" : "Value out of range",
        "error_message" : "ORP_mV is out of range. Value must be within -999 to 999."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - orp_mv")

    # Check: pressure_cmh2o must in the range  50 < x < 3000. We want this field in centimeters
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['pressure_cmh2o'] < 50) | (logger['pressure_cmh2o'] > 6000) ].tmp_row.tolist(),
    #     "badcolumn": "pressure_cmh2o",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your pressure_cmh2o values are out of range. The values must be in centimeters."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - wqlogger - pressure_cmh2o")




    # Need to add not null checks for the measurement columns
    print("...End Other data checks.")
    
    return {'errors': errs, 'warnings': warnings}

def logger_meta(all_dfs):
    
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
    meta = all_dfs['tbl_wq_logger_metadata']

   
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
    return {'errors': errs, 'warnings': warnings}
    