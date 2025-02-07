from inspect import currentframe
from flask import current_app, g, session
import pandas as pd
from .functions import checkData, checkLogic, mismatch,get_primary_key
from .yeahbuoy_custom import yeahbuoy


from numpy import NaN
from sqlalchemy.exc import ProgrammingError

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
    meta['tmp_row'] = meta.index
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

    # CHECK - 2
    print("# CHECK - 2")
    # Description: Ensure there are no overlapping samplecollectiontimestampstart and samplecollectiontimestampend times **only if** there is a match on projectid, siteid, estuaryname, stationno, sensortype, and sensorid in the database. (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 02/07/2025
    # Last Edited Date: 02/07/2025
    # Last Edited Coder: Duy Nguyen
    # NOTE (02/07/2025): Optimized to check overlap **only if a match exists in the database**.

    # Load only relevant columns from database
    meta_db = pd.read_sql(
        "SELECT projectid, siteid, estuaryname, stationno, sensortype, sensorid, samplecollectiontimestampstart, samplecollectiontimestampend FROM tbl_wq_logger_metadata",
        g.eng
    )

    # Keep only records in new data that have a **matching key** in the database
    meta_matched = meta.merge(
        meta_db[['projectid', 'siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid']],
        on=['projectid', 'siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid'],
        how='inner'
    )

    # Merge matched new data with database records for overlap check
    meta_combined = meta_matched.merge(
        meta_db,
        on=['projectid', 'siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid'],
        suffixes=('_new', '_db'),
        how='inner'
    )

    # Identify overlapping rows
    overlapping_rows = meta_combined[
        (meta_combined['samplecollectiontimestampstart_new'] < meta_combined['samplecollectiontimestampend_db']) &  # New start is before existing end
        (meta_combined['samplecollectiontimestampend_new'] > meta_combined['samplecollectiontimestampstart_db'])    # New end is after existing start
    ]

    # Extract bad row indices from new data only
    badrows = meta.merge(
        overlapping_rows[['projectid', 'siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid', 'samplecollectiontimestampstart_new', 'samplecollectiontimestampend_new']],
        left_on=['projectid', 'siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid', 'samplecollectiontimestampstart', 'samplecollectiontimestampend'],
        right_on=['projectid', 'siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid', 'samplecollectiontimestampstart_new', 'samplecollectiontimestampend_new'],
        how='inner'
    )['tmp_row'].tolist()

    args.update({
        "dataframe": meta,
        "tablename": "tbl_wq_logger_metadata",
        "badrows": badrows,
        "badcolumn": "projectid,siteid,estuaryname,stationno,sensortype,sensorid,samplecollectiontimestampstart,samplecollectiontimestampend",
        "error_type": "Logic Error",
        "error_message": "For the same projectid, siteid, estuaryname, stationno, sensortype, and sensorid, overlapping samplecollectiontimestampstart and samplecollectiontimestampend times are not allowed, including database records."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")

    # CHECK - 3
    print("# CHECK - 3")
    # Description: Ensure samplecollectiontimestampstart is less than samplecollectiontimestampend (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 02/07/2025
    # Last Edited Date: 02/07/2025
    # Last Edited Coder: Duy Nguyen
    # NOTE (02/07/2025): Initial implementation.

    args.update({
        "dataframe": meta,
        "tablename": "tbl_wq_logger_metadata",
        "badrows": meta[
            meta['samplecollectiontimestampstart'] >= meta['samplecollectiontimestampend']
        ]['tmp_row'].tolist(),
        "badcolumn": "samplecollectiontimestampstart,samplecollectiontimestampend",
        "error_type": "Logic Error",
        "error_message": "samplecollectiontimestampstart must be less than samplecollectiontimestampend."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")

    # CHECK - 4
    print("# CHECK - 4")
    # Description: Ensure latitude and longitude are not equal to -88 (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 02/07/2025
    # Last Edited Date: 02/07/2025
    # Last Edited Coder: Duy Nguyen
    # NOTE (02/07/2025): Initial implementation.

    args.update({
        "dataframe": meta,
        "tablename": "tbl_wq_logger_metadata",
        "badrows": meta[
            (meta['latitude'] == -88) | (meta['longitude'] == -88)
        ]['tmp_row'].tolist(),
        "badcolumn": "latitude,longitude",
        "error_type": "Value Error",
        "error_message": "Latitude and longitude cannot be -88."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 4")

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

    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # -------------------------------------------------------- tbl_wq_logger_raw check --------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################
    logger = logger.sort_values(by=['samplecollectiontimestamp'])
    
    # print("# CHECK - 0")
    # logger_meta_dat = pd.read_sql("SELECT * FROM tbl_wq_logger_metadata", g.eng)
    # loggerraw_pkey = get_primary_key('tbl_wq_logger_raw', g.eng)
    # loggermeta_pkey = get_primary_key('tbl_wq_logger_metadata', g.eng)

    # raw_meta_shared_key = [x for x in loggerraw_pkey if x in loggermeta_pkey]

    # # Description: Each raw data must correspond to metadata in database
    # # Created Coder: Duy Nguyen
    # # Created Date: 7/31/2024
    # # Last Edited Date: 11/1/2024
    # # Last Edited Coder: Duy Nguyen
    # NOTE: This check is not necessary because below check will catch it
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows": mismatch(logger, logger_meta_dat, raw_meta_shared_key), 
    #     "badcolumn": ','.join(raw_meta_shared_key),
    #     "error_type": "Logic Error",
    #     "error_message": 
    #         "Each record in tbl_wq_logger_raw must have a corresponding metadata. You must submit the metadata before the raw data."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - loggerraw - 0")

    print("# CHECK - 0")
     # Description: All info in logger must match the login info
    # Created Coder: Duy Nguyen
    # Created Date: 11/1/2024 
    # Last Edited Date:
    # Last Edited Coder:
    projectid = session.get('login_info').get('login_project')
    siteid = session.get('login_info').get('login_siteid')
    stationno = session.get('login_info').get('login_stationno')
    sensorid = session.get('login_info').get('login_sensorid')
    sensortype = session.get('login_info').get('login_sensortype')
    print("projectid,siteid,stationno,sensorid,sensortype")
    print(projectid,siteid,stationno,sensorid,sensortype)

    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows": logger[
            (logger['projectid'].astype(str) != str(projectid)) |
            (logger['siteid'].astype(str) != str(siteid)) |
            (logger['stationno'].astype(str) != str(stationno)) |
            (logger['sensorid'].astype(str) != str(sensorid)) |
            (logger['sensortype'].astype(str) != str(sensortype))
        ].tmp_row.tolist(),
        "badcolumn": "projectid,siteid,stationno,sensorid,sensortype",
        "error_type": "Logic Error",
        "error_message": "All info in logger must match the login info"
    })
    errs = [*errs, checkData(**args)]
    print("check ran - loggerraw - 0")

    print("# CHECK - 1")
    # Description: The range of datetime in the submitted file needs to be within the selected [begin_date, end_date] on the login form
    # Created Coder: Duy Nguyen
    # Created Date: 7/5/24 
    # Last Edited Date:
    # Last Edited Coder:


    login_start = pd.Timestamp(session.get('login_info').get('login_start'))
    login_end = pd.Timestamp(session.get('login_info').get('login_end'))
    
    # Comparing timestamps
    login_start = login_start.tz_localize(None)
    login_end = login_end.tz_localize(None)
    logger['samplecollectiontimestamp'] = logger['samplecollectiontimestamp'].apply(lambda x: pd.Timestamp(x).tz_localize(None))
    
    args.update({
        "dataframe": logger,
        "tablename": "tbl_wq_logger_raw",
        "badrows":logger[
            (logger['samplecollectiontimestamp'] < login_start) |
            (logger['samplecollectiontimestamp'] > login_end)
        ].tmp_row.tolist(),
        "badcolumn": "samplecollectiontimestamp",
        "error_type" : "Timestamp Out of Range",
        "error_message" : "The range of datetime in the submitted file needs to be within the selected [begin_date, end_date] on the login form"
    })
    errs = [*errs, checkData(**args)]
    print("check ran - loggerraw - 1")


    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # -----------------------------------------------------END tbl_wq_logger_raw check---------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################








    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # -----------------------------------------------------AUTOMATED QA/QC FLAGGING------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################


    # Need to populate these columns, which were removed from system fields
    # "qaqc_comment" was also removed from system fields but we dont need to populate it, i dont think
    robocols = [
        "raw_depth_qcflag_robot",
        "raw_pressure_qcflag_robot",
        "raw_h2otemp_qcflag_robot", # No range lookup list
        "raw_ph_qcflag_robot",      # No range lookup list
        "raw_conductivity_qcflag_robot",
        "raw_turbidity_qcflag_robot",
        "raw_do_qcflag_robot",
        "raw_do_pct_qcflag_robot",  # No range lookup list
        "raw_salinity_qcflag_robot",
        "raw_chlorophyll_qcflag_robot",
        "raw_orp_qcflag_robot",
        "raw_qvalue_qcflag_robot"   # No range lookup list
    ]
    for c in robocols:
        # wipe out any user input
        logger[c] = None

        # Get the lookup list corresponding with the parameter
        param = str(c).replace('raw_','').replace('_qcflag_robot','')
        try:
            if param == 'h2otemp':
                lu_list = pd.read_sql(f"SELECT * FROM lu_temperature_unit", g.eng).rename(columns={ 'unit': f'raw_{param}_unit' })
            else:
                lu_list = pd.read_sql(f"SELECT * FROM lu_{param}_unit", g.eng).rename(columns={ 'unit': f'raw_{param}_unit' })
        except ProgrammingError as e:
            print("Error reading from the lookup table")
            print(e)
            continue

        # Need to make sure all lookup lists have Not Recorded with both words capitalized so it doesnt crash upon final submit
        logger[ f'raw_{param}_unit' ] = logger[ f'raw_{param}_unit' ].fillna('Not Recorded').astype(str)

        print(f'logger[ raw_{param}_unit ]')
        print(logger[ f'raw_{param}_unit' ])
        print(f'lu_list[ raw_{param}_unit ]')
        print(lu_list[ f'raw_{param}_unit' ])
        
        logger = logger.merge( lu_list, on = [ f'raw_{param}_unit' ], how = 'left' )

        logger['min'] = logger['min'].fillna(NaN)
        logger['max'] = logger['max'].fillna(NaN)


        # QAQC Flags based on this website
        # https://cdmo.baruch.sc.edu/data/qaqc.cfm
        logger[c] = logger.apply(
            lambda row: 
            -2 if pd.isnull(row[f'raw_{param}'])

            else 
            -4 if row[f'raw_{param}'] < row['min']

            else 
            -5 if row[f'raw_{param}'] > row['max']

            else 0

            , axis = 1
        )

        # drop for next iteration
        logger.drop(['objectid','min','max'], axis = 'columns', inplace = True, errors = 'ignore')



    # write the dataframe back out so the qaqcflags show up in the downloaded file
    # drop unnecessary columns that were used just for putting qaqcflags
    logger.drop(['objectid','min','max'], axis = 'columns', inplace = True, errors = 'ignore')
    
    # Order columns in a user friendly manner
    orderedcols = pd.read_sql("SELECT column_name FROM column_order WHERE table_name = 'tbl_wq_logger_raw' ORDER BY custom_column_position", g.eng).column_name.tolist()
    orderedcols = [c for c in orderedcols if c in logger.columns]

    # write the thing back to the original file path in the tbl_wq_logger_raw sheet
    with pd.ExcelWriter(session.get('excel_path')) as writer:
        logger[orderedcols].to_excel(writer, sheet_name = 'tbl_wq_logger_raw', index = False)

 
    # Need to add not null checks for the measurement columns
    print("...End Other data checks.")
    
    return {'errors': errs, 'warnings': warnings}



    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------ END AUTOMATED QA/QC FLAGGING------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################



    

    # # Temporary just to test logger data plotting
    # # Ayah commented out temp -> return {'errors': errs, 'warnings': warnings}

    # # Example of appending an error (same logic applies for a warning)
    # # args.update({
    # #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    # #   "badcolumn": "temperature",
    # #   "error_type" : "Not asdf",
    # #   "error_message" : "This is a helpful useful message for the user"
    # # })
    # # errs = [*errs, checkData(**args)]
    
    # # For the barometric pressure routine which apparently is not ready yet, or is not going to be put in the checker
    # # df = yeahbuoy(df) if not df.empty else df

    # print("Begin Logic Checks")

    # # The only check we need to write here is to check that the login fields match a record in the database
    # print("# The only check we need to write here is to check that the login fields match a record in the database")
    # print("This has not been written yet")

    # print("End logic checks.")
    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------- Minidot/CTD/TrollShared Check --------------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################

    # print("Begin Minidot/CTD/TrollShared check  data check...")
    # print('Begin check 1')
    # # Description:Range for raw_h2otemp must be between [0, 100] or -88
    # # Created Coder: Ayah
    # # Created Date: 10/13/2023
    # # Last Edited Date: 
    # # Last Edited Coder: 
    # # NOTE (10/13/2023): Ayah edited check 
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['raw_h2otemp'] > 100) | ((logger['raw_h2otemp']!=-88) & (logger['raw_h2otemp'] < 0))].tmp_row.tolist(),
    #     "badcolumn": "raw_h2otemp",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your raw_h2otemp is out of range. Value should be within 0-100 degrees C."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_data - raw_h2otemp")

    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------- End of Minidot/CTD/TrollShared Check --------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################


    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ---------------------------------------------- Begin of Minidot Check -------------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################
    # print('Begin check 2')
    # # Description: Issue warning if do_percent < 0 and not -88
    # # Created Coder: NA
    # # Created Date: NA
    # # Last Edited Date: 10/17/2023
    # # Last Edited Coder: Ayah
    # # NOTE (10/13/2023): Ayah created check 
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['raw_do_pct'] < 0) & (logger['raw_do_pct']!=-88) & (~pd.isnull(logger['raw_do_pct']) )].tmp_row.tolist(),
    #     "badcolumn": "raw_do_pct",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your raw_do_pct is negative. Value must be nonnegative and at least 0."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_mdot_data - do_percent")
    
    # print('Begin check 3')
    # #Description: Issue warning do_percent > 110 # Jan asked for this to be a warning. (Jan asked for this to be a warning. 4 March 2022)
    # # Created Coder: NA
    # # Created Date: NA
    # # Last Edited Date: 10/17/2023
    # # Last Edited Coder: Ayah
    # # NOTE (10/13/2023): Ayah edited check 
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['raw_do_pct'] > 110) & (logger['sensortype']=='minidot')].tmp_row.tolist(),
    #     "badcolumn": "raw_do_pct",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your raw_do_pct is greater than 110. This is an unexpected value, but will be accepted."
    # })
    # warnings = [*warnings, checkData(**args)]
    # print("check ran - logger_mdot_data - do_percent")
    
    # print('Begin check 4')
    # # Description: Range for raw_do  must be between [0, 60] or -88 when raw_do_unit is mg/L
    # # Created Coder: NA
    # # Created Date: NA
    # # Last Edited Date: 10/17/2023
    # # Last Edited Coder: Ayah
    # # NOTE (10/13/2023): Ayah edited check 
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[((logger['raw_do'] > 60) | ((logger['raw_do']!=-88) & (logger['raw_do'] < 0))) & (logger['raw_do_unit']== "mg/L")].tmp_row.tolist(),
    #     "badcolumn": "raw_do",
    #     "error_type" : "Value out  of range",
    #     "error_message" : "Your raw_do value is out of range. Value should be within 0-60 mg/L."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_mdot_data - raw_do")
    
    # print('Begin check 5')
    # # Description: qvalue range increased from 1 to 1.1 - approved by Jan 4 March 2022
    # # Created Coder: NA
    # # Created Date: NA
    # # Last Edited Date: 10/17/2023
    # # Last Edited Coder: Ayah
    # # NOTE (10/13/2023): Ayah edited check  
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['raw_qvalue'] > 1.1) | ((logger['raw_qvalue']!=-88) & (logger['raw_qvalue'] < 0))].tmp_row.tolist(),
    #     "badcolumn": "raw_qvalue",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your raw_qvalue is out of range. Must be within 0-1.1."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_mdot_data - qvalue")
    # print("...End minidot data checks.")
    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ----------------------------------------------- End of Minidot Check --------------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ###################################################################################################################### 

    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ----------------------------------------------- Begin of CTD Check ----------------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ###################################################################################################################### 
          
    # # Need to add not null checks for the measurement columns
    # print("Begin CTD data checks...")

    # print('Begin check 7')
    # # Description: Range for raw_conductivity must be between [0, 10] or -88 when raw_conductiviy_units is mscm
    # # Created Coder: NA
    # # Created Date: NA
    # # Last Edited Date: 10/17/2023
    # # Last Edited Coder: Ayah
    # # NOTE (10/13/2023): Ayah edited check  
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(((logger['raw_conductivity'] < 0) & (logger['raw_conductivity'] != -88)) | (logger['raw_conductivity'] > 100)) & \
    #                           (logger['raw_conductivity_unit'] == "mS/cm")].tmp_row.tolist(),
    #     "badcolumn": "raw_conductivity",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your conductivity_mscm value is out of range. Value must be within 0-10. If no value to provide, enter -88."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_ctd_data - conductivity_mscm")

    # print('Begin check 9')
    # # Description: Range for salinity_ppt must be nonnegative or-88
    # # Created Coder: NA
    # # Created Date: NA
    # # Last Edited Date: 10/17/2023
    # # Last Edited Coder: Ayah
    # # NOTE (10/13/2023): Ayah edited check 
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['raw_salinity'] < 0) & (logger['raw_salinity'] != -88)].tmp_row.tolist(),
    #     "badcolumn": "raw_salinity",
    #     "error_type" : "Negative value",
    #     "error_message" : "Your raw_salinity value is less than 0. Value should be nonnegative. If no value to provide, enter -88."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_ctd_data - raw_salinity")

    # print("...End CTD data checks.")
    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------ End of CTD Check ------------------------------------------------ #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################

    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------ Begin of Troll Check -------------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################
    
    
    # # print('Begin check 12')
    # # # Description: If sensortype is CTD, then raw_pressure should be filled and raw_pressure_unit should be cmh2o
    # # # Created Coder: NA
    # # # Created Date: NA
    # # # Last Edited Date: 10/17/2023
    # # # Last Edited Coder: Ayah
    # # # NOTE (10/13/2023): Ayah edited check 
    # # lu_pressure_unit = pd.read_sql("SELECT * FROM lu_pressure_unit;",g.eng)
    # # args.update({
    # #     "dataframe": logger,
    # #     "tablename": "tbl_wq_logger_raw",
    # #     "badrows":logger[
    # #         (logger['sensortype'] == 'troll') & (logger['raw_pressure'].isna() | ~logger['raw_pressure_unit'].isin(lu_pressure_unit))
    # #     ].tmp_row.tolist(),
    # #     "badcolumn": "sensortype,raw_pressure,raw_pressure_unit",
    # #     "error_type" : "Unknown Error",
    # #     "error_message" : 'Since sensortype is Troll, raw_pressure and raw_pressure_unit should contain a value'
    # # })
    # # errs = [*errs, checkData(**args)]

    # # print("check ran - wqlogger - pressure_cmh2o")
    # # print("...End troll  data checks.")

    # # Need to add not null checks for the measurement columns

    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------- End of Troll Check --------------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################
    
    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------- Begin of Tidbit Check ------------------------------------------ #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################
    # print("Begin Tidbit data checks...")
    # #Only check for tidbit was for h2otemp which is already coded
    # print("...End Tidbit data checks.")

    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------- Begin of Other  Check ------------------------------------------ #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################
    # print("Begin Other data checks...")

    # print('Begin check 14')
    # # Description: Range for pH must be between [1, 14] or -88
    # # Created Coder: NA
    # # Created Date: NA
    # # Last Edited Date: 10/17/2023
    # # Last Edited Coder: Ayah
    # # NOTE (10/13/2023): Ayah edited check 
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['raw_ph'] < 1) | (logger['raw_ph'] > 14)].tmp_row.tolist(),
    #     "badcolumn": "raw_ph",
    #     "error_type" : "Value out of range",
    #     "error_message" : "pH value is out of range. Value should be between 1 and 14. If no value to provide, enter -88."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_other_data - pH")


    # print('Begin check 15')
    # # Description: Range for turbidity_ntu must be between [0, 3000] or -88
    # # Created Coder: NA
    # # Created Date: NA
    # # Last Edited Date: 10/17/2023
    # # Last Edited Coder: Ayah
    # # NOTE (10/13/2023): Ayah edited check 
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['raw_turbidity_unit']=="NTU") & (((logger['raw_turbidity'] < 0) & (logger['raw_turbidity'] != -88)) | (logger['raw_turbidity'] > 3000))].tmp_row.tolist(),
    #     "badcolumn": "raw_turbidity",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Turbidity_NTU value is out of range. Value should be within 0-3000. If no value to provide, enter -88."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_other_data - turbidity_ntu")

    # print('Begin check 16')
    # # Description: Range for do_percent must be between [-1, 300] or -88
    # # Created Coder: NA
    # # Created Date: NA
    # # Last Edited Date: 10/17/2023
    # # Last Edited Coder: Ayah
    # # NOTE (10/13/2023): Ayah edited check 
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['sensortype']!='minidot') & ((logger['raw_do_pct'] < -1) & (logger['raw_do_pct'] != -88)) | (logger['raw_do_pct'] > 300)].tmp_row.tolist(),
    #     "badcolumn": "raw_do_pct",
    #     "error_type" : "Value out of range",
    #     "error_message" : "raw_do_pct is out of range. Value must be within 0-300. If no value, enter -88 or leave blank."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_other_data - do_percent")
    
    # print('Begin check 17')
    # # Description: Range for raw_orp must be between [999,-999] when raw_orp_unit is mv
    # # Created Coder: NA
    # # Created Date: NA
    # # Last Edited Date: 10/17/2023
    # # Last Edited Coder: Ayah
    # # NOTE (10/13/2023): Ayah edited check 
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[(logger['raw_orp_unit']=='mV') & ((logger['raw_orp'] < -999) | (logger['raw_orp'] > 999))].tmp_row.tolist(),
    #     "badcolumn": "raw_orp",
    #     "error_type" : "Value out of range",
    #     "error_message" : "ORP_mV is out of range. Value must be within -999 to 999."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - logger_other_data - orp_mv")

    # print('Begin check 18')
    # # Description: Range for raw-pressure must be between [50,6000] when raw_pressure_unit is cmh
    # # Created Coder: NA
    # # Created Date: NA
    # # Last Edited Date: 11/02/2023
    # # Last Edited Coder: Ayah
    # # NOTE (10/13/2023): Ayah edited check
    # args.update({
    #     "dataframe": logger,
    #     "tablename": "tbl_wq_logger_raw",
    #     "badrows":logger[((logger['raw_pressure'] < 50) | (logger['raw_pressure'] > 6000)) & (logger['raw_pressure_unit'] == 'cmH2O') ].tmp_row.tolist(),
    #     "badcolumn": "raw_pressure",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your raw_pressure values are out of range. The values must be in centimeters."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - wqlogger - pressure_cmh2o")
