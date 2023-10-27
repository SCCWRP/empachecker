# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch
from .yeahbuoy_custom import yeahbuoy

def logger_formatted(all_dfs):
    
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
    loggermeta =  all_dfs['tbl_wq_logger_metadata']
    logger = all_dfs['tbl_wq_logger_raw']

   
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

    ###################################################################################################################################################################
    # checkLogic(df1, df2) returns rows for df1 that do not have corresponding records in df2 --- need to subset on sensortype before passing
    ###################################################################################################################################################################
    
    print("Begin Logic Checks")
    #checkLogic function previously automates the custom error message, but I have not generalized it here.
    #instead, I've written in the error_message explicitly
    # ???????????? ------ consider adding if empty conditions ---- ?????????
    # Logic Check 1: wq_metadata & wqlogger
    # Logic Check 1a: metadata records not found in wglogger
    # checking if metadata specifies that there is logger data within submission, then run checkLogic()
    groupcols = ['siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid']
    args.update({
        "dataframe": loggermeta,
        "tablename": "tbl_wq_logger_metadata",
        "badrows": mismatch(loggermeta, logger, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, sensortype, sensorid",
        "error_type": "Logic Error",
        "error_message": "Each record in the Meta Data must have a corresponding record in the Raw Data Logger tab"
    })
    errs = [*errs, checkData(**args)]

    # Logic Check 1b: metadata record missing for records provided by logger
    groupcols = ['siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid']
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows": mismatch(logger, loggermeta, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, sensortype, sensorid",
        "error_type": "Logic Error",
        "error_message": "Records in Raw Logger data must have a corresponding record in the metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - wqlogger vs wq_metadata")
    
    print("End logic checks.")

    # NEW UPDATED LOGGER LOGIC CHECK --- 
    # samplecollectiontimestamp in logger must lie within samplecollectiontimestampstart and samplecollectiontimestampend range within loggermeta
    # honestly, make this a function and import it into the logger_custom file --- draft here for now
    # for index, row in loggermeta.itterrows():
    #     siteid = row.siteid
    #     estuaryname = row.estuaryname
    #     stationno = row.stationno
    #     sensortype = row.sensortype
    #     sensorid = row.sensorid
    #     start = row.samplecollectiontimestampstart
    #     end = row.samplecollectiontimestampend
    #     submissionid = row.submissionid
    #     start_updated = False
    #     end_updated = False
    #     if any(e in ['T','Z'] for e in start):
    #         start = start.replace('T', ' ')
    #         start = start.replace('Z', '')
    #         start_updated = True
    #     if any(e in ['T','Z'] for e in end):
    #         end = end.replace('T', ' ')
    #         end = end.replace('Z', '')
    #         end_updated = True
    #     qry = f"siteid == '{siteid}' \
    #             & estuaryname == '{estuaryname}' \
    #             & stationno == '{stationno}' \
    #             & sensortype == '{sensortype}' \
    #             & sensorid == '{sensorid}' \
    #             & submissionid == '{submissionid}' \
    #             & samplecollectiontimestamp >= '{start}' \
    #             & samplecollectiontimestamp <= '{end}'"
    #     # using logger data
    #     tmp_data = logger.query(qry)
    #     min_timestamp = min(tmp_data.samplecollectiontimestamp)
    #     max_timestamp = max(tmp_data.samplecollectiontimestamp)
    #     tmp_data = tmp_data.assign(samplecollectiontimestampstart = lambda x: min_timestamp, 
    #                                samplecollectiontimestampend = lambda x: max_timestamp)
    #     expected = len(tmp_data) + len

    #### begin logic check function for logger
    # def checkLogic_logger(df_meta, df_data, cols: list, error_type = "Logic Error"):
    #     group_cols = ['siteid','estuaryname','stationno','sensorid','samplecollectiontimestamp']
    #     df_data['samplecollectiontimestamp'] = df_data['samplecollectiontimestamp'].apply(lambda x: pd.to_datetime(x, utc=True))
    #     for index, row in df_meta.iterrows():
    #         siteid = row.siteid
    #         estuaryname = row.estuaryname
    #         stationno = row.stationno
    #         sensortype = row.sensortype
    #         sensorid = row.sensorid
    #         start = row.samplecollectiontimestampstart

    #     return(badrows)
    #group_cols = ['siteid','estuaryname','stationno','sensorid','samplecollectiontimestamp']
    


    print("Begin logger data checks...")
  

    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['do_percent'] < 0) & (logger['do_percent']!=-88) & (~pd.isnull(logger['do_percent']) )].index.tolist(),
    #     "badcolumn": "do_percent",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your do_percent is negative. Value must be nonnegative and at least 0."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_mdot_data - do_percent")

    # Check: issue warning do_percent > 110 # Jan asked for this to be a warning. 4 March 2022
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['do_percent'] > 110)].index.tolist(),
    #     "badcolumn": "do_percent",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your do_percent is greater than 110. This is an unexpected value, but will be accepted."
    # })
    # warnings = [*warnings, checkData(**args)]
    # print("check ran - tbl_wq_logger_raw - do_percent")

    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['do_mgl'] > 60) | ((logger['do_mgl']!=-88) & (logger['do_mgl'] < 0))].index.tolist(),
        "badcolumn": "do_mgl",
        "error_type" : "Value out of range",
        "error_message" : "Your do_mgl value is out of range. Value should be within 0-60 mg/L."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_wq_logger_raw - do_mgl")

    # Check: qvalue range increased from 1 to 1.1 - approved by Jan 4 March 2022
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['qvalue'] > 1.1) | ((logger['qvalue']!=-88) & (logger['qvalue'] < 0))].index.tolist(),
        "badcolumn": "qvalue",
        "error_type" : "Value out of range",
        "error_message" : "Your qvalue is out of range. Must be within 0-1.1."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_wq_logger_raw - qvalue")
    print("...End minidot data checks.")

    print("Begin CTD data checks...")
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[((logger['conductivity_mscm'] < 0) & (logger['conductivity_mscm'] != -88)) | (logger['conductivity_mscm'] > 80)].index.tolist(),
        "badcolumn": "conductivity_mscm",
        "error_type" : "Value out of range",
        "error_message" : "Your conductivity_mscm value is out of range. Value must be within 0-10. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_wq_logger_raw - conductivity_mscm")

    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['h2otemp_c'] > 100) | ((logger['h2otemp_c'] != -88) & (logger['h2otemp_c'] < 0))].index.tolist(),
    #     "badcolumn": "h2otemp_c",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your h2otemp_c value is out of range. Value should not exceed 100 degrees C. If no value to provide, enter -88."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_ctd_data - h2otemp_c")

    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['salinity_ppt'] < 0) & (logger['salinity_ppt'] != -88)].index.tolist(),
        "badcolumn": "salinity_ppt",
        "error_type" : "Negative value",
        "error_message" : "Your salinity_ppt value is less than 0. Value should be nonnegative. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_wq_logger_raw - salinity_ppt")
    print("...End CTD data checks.")

    print("Begin Troll data checks...")
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['h2otemp_c'] > 100) | ((logger['h2otemp_c'] != -88) & (logger['h2otemp_c'] < 0))].index.tolist(),
    #     "badcolumn": "h2otemp_c",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your h2otemp_c value is out of range. Value should not exceed 100 degrees C. If no value to provide, enter -88."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_troll_data - h2otemp_c")
    print("...End Troll data checks.")

    # print("Begin Tidbit data checks...")
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_logger_tidbit_data",
    #     "badrows":logger[(logger['h2otemp_c'] > 100) | ((logger['h2otemp_c'] != -88) & (logger['h2otemp_c'] < 0))].index.tolist(),
    #     "badcolumn": "h2otemp_c",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your h2otemp_c value is out of range. Value should not exceed 100 degrees C. If no value to provide, enter -88."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_tidbit_data - h2otemp_c")
    print("...End Tidbit data checks.")

    print("Begin Other data checks...")


    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[ (logger['ph'] != -88) & ((logger['ph'] < 1) | (logger['ph'] > 14)) ].index.tolist(),
        "badcolumn": "ph",
        "error_type" : "Value out of range",
        "error_message" : "pH value is out of range. Value should be between 1 and 14. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_wq_logger_raw - pH")



    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[((logger['turbidity_ntu'] < 0) & (logger['turbidity_ntu'] != -88)) | (logger['turbidity_ntu'] > 3000)].index.tolist(),
        "badcolumn": "turbidity_ntu",
        "error_type" : "Value out of range",
        "error_message" : "Turbidity_NTU value is out of range. Value should be within 0-3000. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - turbidity_ntu")


    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[((logger['do_percent'] < -1) & (logger['do_percent'] != -88)) | (logger['do_percent'] > 300)].index.tolist(),
        "badcolumn": "do_percent",
        "error_type" : "Value out of range",
        "error_message" : "do_percent is out of range. Value must be within 0-300. If no value, enter -88 or leave blank."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - do_percent")

    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['orp_mv'] < -999) | (logger['orp_mv'] > 999)].index.tolist(),
        "badcolumn": "orp_mv",
        "error_type" : "Value out of range",
        "error_message" : "ORP_mV is out of range. Value must be within -999 to 999."
    })
    errs = [*errs, checkData(**args)]
    
    print("...End Other data checks.")








    # testing our assertions
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "faketable",
    #     "badrows":logger[(logger['orp_mv'] < -999) | (logger['orp_mv'] > 999)].index.tolist(),
    #     "badcolumn": "orp_mv",
    #     "error_type" : "Value out of range",
    #     "error_message" : "ORP_mV is out of range. Value must be within -999 to 999."
    # })
    # errs = [*errs, checkData(**args)]

    # args.update({
    #     "dataframe": logger,
    #     "tablename": "this_is_a_fake_tablename",
    #     "badrows":logger.index.tolist(),
    #     "badcolumn": "fakecol",
    #     "error_type" : "Value out of range",
    #     "error_message" : "ORP_mV is out of range. Value must be within -999 to 999."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_other_data - orp_mv")


    # cant incorrectly name the table in the checkData function

    assert all([e.get('table') in expectedtables for e in errs if e.get('table')]), "There is a tablename specified in the custom checks arguments which does not match the expected tables of the datatype"
    
    return {'errors': errs, 'warnings': warnings}




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
    # NOTE (10/13/2023): Ayah created check 
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

     # Check: issue warning do_percent < 0 and not -88
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
    
    # Check: issue warning do_percent > 110 # Jan asked for this to be a warning. 4 March 2022
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
    
    #Check: Range for do_mgl must be between [0, 60] or -88
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

    
    # Check: qvalue range increased from 1 to 1.1 - approved by Jan 4 March 2022
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
    
    
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[
    #         (logger['sensortype'] == 'troll') & (logger['pressure_cmh2o'].notnull().any())
    #     ].tmp_row.tolist(),
    #     "badcolumn": "sensortype,pressure_cmh2o,pressure_mbar",
    #     "error_type" : "Unknown Error",
    #     "error_message" : "If the sensortype is troll, you should fill out the column pressure_mbar for pressure reading, not pressure_cmh2o"
    # })
    # errs = [*errs, checkData(**args)]

    # print("check ran - wqlogger - pressure_cmh2o")

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

    print("...End Tidbit data checks.")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------- Begin of Other  Check ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################




    '''
    print("Begin Other data checks...")


    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['ph'] < 1) | (logger['ph'] > 14)].index.tolist(),
        "badcolumn": "ph",
        "error_type" : "Value out of range",
        "error_message" : "pH value is out of range. Value should be between 1 and 14. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - pH")



    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[((logger['turbidity_ntu'] < 0) & (logger['turbidity_ntu'] != -88)) | (logger['turbidity_ntu'] > 3000)].index.tolist(),
        "badcolumn": "turbidity_ntu",
        "error_type" : "Value out of range",
        "error_message" : "Turbidity_NTU value is out of range. Value should be within 0-3000. If no value to provide, enter -88."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - turbidity_ntu")


    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[((logger['do_percent'] < -1) & (logger['do_percent'] != -88)) | (logger['do_percent'] > 300)].index.tolist(),
        "badcolumn": "do_percent",
        "error_type" : "Value out of range",
        "error_message" : "DO_percent is out of range. Value must be within 0-300. If no value, enter -88 or leave blank."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - do_percent")

    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['orp_mv'] < -999) | (logger['orp_mv'] > 999)].index.tolist(),
        "badcolumn": "orp_mv",
        "error_type" : "Value out of range",
        "error_message" : "ORP_mV is out of range. Value must be within -999 to 999."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logger_other_data - orp_mv")


    # Check: pressure_cmh2o must in the range  50 < x < 3000. We want this field in centimeters
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[(logger['pressure_cmh2o'] < 50) | (logger['pressure_cmh2o'] > 6000) ].tmp_row.tolist(),
        "badcolumn": "pressure_cmh2o",
        "error_type" : "Value out of range",
        "error_message" : "Your pressure_cmh2o values are out of range. The values must be in centimeters."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - wqlogger - pressure_cmh2o")

    # Check: If sensortype is CTD, then they should fill out the pressure_cmh2o field. 
    # If sensortype is troll, then they should fill out the pressure_mbar field.

    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[
            (logger['sensortype'] == 'CTD') & (logger['pressure_mbar'].notnull().any())
        ].tmp_row.tolist(),
        "badcolumn": "sensortype,pressure_cmh2o,pressure_mbar",
        "error_type" : "Unknown Error",
        "error_message" : "If the sensortype is CTD, you should fill out the column pressure_cmh2o for pressure reading, not pressure_mbar"
    })
    errs = [*errs, checkData(**args)]
    print("check ran - wqlogger - pressure_cmh2o")


    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[
            (logger['sensortype'] == 'troll') & (logger['pressure_cmh2o'].notnull().any())
        ].tmp_row.tolist(),
        "badcolumn": "sensortype,pressure_cmh2o,pressure_mbar",
        "error_type" : "Unknown Error",
        "error_message" : "If the sensortype is troll, you should fill out the column pressure_mbar for pressure reading, not pressure_cmh2o"
    })
    errs = [*errs, checkData(**args)]

    print("check ran - wqlogger - pressure_cmh2o")

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
    '''