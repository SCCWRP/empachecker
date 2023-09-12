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
    ######################################################################################################################    print("Begin WQ Logic Checks...")
    
    print("START CHECK  1a")
    #Description: Each metadata must include corresponding data (wq_metdata records not found in wq_data)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    watermeta_pkey = get_primary_key('tbl_waterquality_metadata',g.eng)
    waterdata_pkey = get_primary_key('tbl_waterquality_data',g.eng)
    watermeta_waterdata_shared_pkey = list(set(watermeta_pkey).intersection(set(waterdata_pkey)))
    
    args.update({
        "dataframe": watermeta,
        "tablename": "tbl_waterquality_metadata",
        "badrows": mismatch(watermeta, waterdata, watermeta_waterdata_shared_pkey),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplecollectiontime, profile, depth_m, projectid",
        "error_type": "Logic Error",
        "error_message": "Each record in WQ_metadata must have a corresponding record in WQ_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - wq_metadata records not found in wq_data")
    print("END CHECK check 1a")


    print("START CHECK 1b")
    #Description: Each data must include corresponding metadata(wq_metadata records missing for records provided by wq_data)
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": "tbl_waterquality_data",
        "badrows": mismatch(waterdata, watermeta, watermeta_waterdata_shared_pkey), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplecollectiontime, profile, depth_m",
        "error_type": "Logic Error",
        "error_message": "Records in WQ_data must have a corresponding record in WQ_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("END CHECK 1b")
    
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
    
    print('START CHECK 2')
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
    print('END CHECK 2')
    
    
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
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard

    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[            
            (waterdata['conductivity'] != -88) &
            (waterdata['conductivity_units'] == 'uS/cm') & 
            (~waterdata['conductivity'].between(0,100))
        ].tmp_row.tolist(), 
        "badcolumn": "conductivity",
        "error_type": "Value out of range",
        "error_message" : "If the conductivity unit is uS/cm, then conductivity values must be between 0 and 100."
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
    print('END CHECK  9')

    print('BEGIN CHECK  10')
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
    
    print('START CHECK  11')
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
    # Description:  Range for do, with do_units as mg/l, must be within [0, 20] or -88
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['do_mgl'] != -88) & 
            (waterdata['do_units'].str.lower() == 'mg/l') &
            (~waterdata['do_mgl'].between(0, 20))
        ].tmp_row.tolist(),
        "badcolumn": "do_mgl",
        "error_type": "Value Out of range",
        "error_message" : "do_mgl values must be between 0 and 20."
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 12')

    print('START CHECK 13')
    # Description:  Range for airtemp, with airtemp_units as C, must be between [0, 50] or -88
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
            (~waterdata['airtemp'].between(0, 50))
        ].tmp_row.tolist(),
        "badcolumn": "airtemp",
        "error_type": "Value Out of range",
        "error_message" : "If airtemp_unit is C, airtemp must be between 0 and 50."
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 13')

    print('BEGIN CHECK  14')
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
    print('END CHECK  14')

    
    print('BEGIN CHECK 15')
    # Description:  If H2OTemp is reported, then H2OTemp_Units cannot be 'Not Recorded'
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['h2otemp'].notna()) &
            (waterdata['h2otemp_units'] == 'Not Recorded')
        ].tmp_row.tolist(),
        "badcolumn": "h2otemp_units",
        "error_type": "Bad Value",
        "error_message" : " If H2OTemp is reported, then H2OTemp_Units cannot be 'Not Recorded'"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 15')    

    
    print('START CHECK 16')
    # Description: If AirTemp is reported, then AirTemp_Units cannot be 'Not Recorded'
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['airtemp'].notna()) &
            (waterdata['airtemp_units'] == 'Not Recorded')
        ].tmp_row.tolist(),
        "badcolumn": "airtemp_units",
        "error_type": "Bad Value",
        "error_message" : " If AirTemp is reported, then AirTemp_Units cannot be 'Not Recorded'"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 16')  

    print('BEGIN CHECK 17')
    # Description: If DO_mgL is reported, then DO_Units cannot be 'Not Recorded'
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['do_mgl'].notna()) &
            (waterdata['do_units'] == 'Not Recorded')
        ].tmp_row.tolist(),
        "badcolumn": "do_units",
        "error_type": "Bad Value",
        "error_message" : "If DO_mgL is reported, then DO_Units cannot be 'Not Recorded'"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 17')   

    print('START CHECK 18')
    # Description:  If Salinity is reported, then Salinity_Units cannot be 'Not Recorded'    
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['salinity'].notna()) &
            (waterdata['salinity_units'] == 'Not Recorded')
        ].tmp_row.tolist(),
        "badcolumn": "Salinity_Units",
        "error_type": "Bad Value",
        "error_message" : "If Salinity is reported, then Salinity_Units cannot be 'Not Recorded'"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 18') 

    print('START CHECK 19')
    # Description:  If TDS_ppt is reported, then TDS_Units cannot be 'Not Recorded'
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['tds'].notna()) &
            (waterdata['tds_units'] == 'Not Recorded')
        ].tmp_row.tolist(),
        "badcolumn": "tds_units",
        "error_type": "Bad Value",
        "error_message" : " If TDS is reported, then TDS_Units cannot be 'Not Recorded'"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 19')   

    
    print('START CHECK 20')
    # Description:  If Conductivity is reported, then Conductivity_Units cannot be 'Not Recorded'
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 09/05/2023
    # Last Edited Coder: Ayah Halabi
    # NOTE (09/05/2023): Ayah adjusted format so it follows the coding standard
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['conductivity'].notna()) &
            (waterdata['conductivity_units'] == 'Not Recorded')
        ].tmp_row.tolist(),
        "badcolumn": "Conductivity_Units",
        "error_type": "Bad Value",
        "error_message" : "If Conductivity is reported, then Conductivity_Units cannot be 'Not Recorded'"
    })
    errs = [*errs, checkData(**args)]
    print('END CHECK 20')  

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END WQ Data Custom Checks -------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("-------------End of Discrete WQ Custom Check ----------")
    
    
    
    
    return {'errors': errs, 'warnings': warnings}