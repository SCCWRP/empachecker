# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, mismatch

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

    ############################### --Start of Logic Checks -- #############################################################
    print("Begin WQ Logic Checks...")
    # Logic Check 1: wq_metadata & wq_data
    # Logic Check 1a: Each metadata must include corresponding data (wq_metdata records not found in wq_data)
    print("Start logic check 1a")




    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplecollectiontime', 'profile', 'depth_m', 'projectid']
    args.update({
        "dataframe": watermeta,
        "tablename": "tbl_waterquality_metadata",
        "badrows": mismatch(watermeta, waterdata, groupcols),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplecollectiontime, profile, depth_m, 'projectid'",
        "error_type": "Logic Error",
        "error_message": "Each record in WQ_metadata must have a corresponding record in WQ_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - wq_metadata records not found in wq_data")
    print("End logic check 1a")

    # Logic Check 1b:Each data must include corresponding metadata(wq_metadata records missing for records provided by wq_data)
    print("Start logic check 1b")
    args.update({
        "dataframe": waterdata,
        "tablename": "tbl_waterquality_data",
        "badrows": mismatch(waterdata, watermeta, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplecollectiontime, profile, depth_m",
        "error_type": "Logic Error",
        "error_message": "Records in WQ_data must have a corresponding record in WQ_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - wq_metadata records missing for records provided by wq_data") #testing
    print("End logic check 1b")
    print("End WQ Logic Checks...")

    # End Discrete WQ Logic Checks
    ############################## -- End of logic checks --####################################################################
    
    ############################### --Start of WaterQuality Metadata Checks -- #############################################################
    print("Begin WaterQuality Metadata Checks...")

    #check 2: Depth_m must be greater than or equal to 0
    print("Start of WQ Metadata custom checks:")
    print('begin discretewq-custom-check 2')
    args.update({
        "dataframe": watermeta,
        "tablename": 'tbl_waterquality_metadata',
        "badrows": watermeta[(watermeta['depth_m'] != -88) & (watermeta['depth_m'] < 0)].tmp_row.tolist(),
        "badcolumn": "depth_m",
        "error_type" : "Value Out of Range",
        "error_message" : "Your depth value must be larger than or equal to 0."
    })
    errs = [*errs, checkData(**args)]
    print('begin discretewq-custom-check 2')
    
    # #check 3: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Time_Ele is required // 
    # # NOTE @Duy/Robert I am assuming Time_Ele is elevation_time since there is no Time_Ele field -Aria      
    # print('begin discretewq-custom-check 3')
    # args.update({
    #     "dataframe": watermeta,
    #     "tablename": 'tbl_waterquality_metadata',
    #     "badrows": watermeta[            
    #         ((watermeta['elevation_ellipsoid'].notna()) | (watermeta['elevation_orthometric'].notna())) & (watermeta['elevation_time'].isna())
    #     ].tmp_row.tolist(), 
    #     "badcolumn": "elevation_time",
    #     "error_type": "Missing Value",
    #     "error_message" : "If Elevation_Ellipsoid or Elevation_Orthometric is reported, then elevation_time is required"
    # })
    # errs = [*errs, checkData(**args)]
    # print('end discretewq-custom-check 3')

    # #check 4: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required     
    # print('begin discretewq-custom-check 4')
    # args.update({
    #     "dataframe": watermeta,
    #     "tablename": 'tbl_waterquality_metadata',
    #     "badrows": watermeta[            
    #         ((watermeta['elevation_ellipsoid'].notna()) | (watermeta['elevation_orthometric'].notna())) & (watermeta['elevation_units'].isna())
    #     ].tmp_row.tolist(), 
    #     "badcolumn": "elevation_units",
    #     "error_type": "Missing Value",
    #     "error_message" : "If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required"
    # })
    # errs = [*errs, checkData(**args)]
    # print('end discretewq-custom-check 4')

    # #check 5: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Corr is required
    # print('begin discretewq-custom-check 5')
    # args.update({
    #     "dataframe": watermeta,
    #     "tablename": 'tbl_waterquality_metadata',
    #     "badrows": watermeta[            
    #         ((watermeta['elevation_ellipsoid'].notna()) | (watermeta['elevation_orthometric'].notna())) & (watermeta['elevation_corr'].isna())
    #     ].tmp_row.tolist(), 
    #     "badcolumn": "elevation_corr",
    #     "error_type": "Missing Value",
    #     "error_message" : "If Elevation_Ellipsoid or Elevation_Orthometric is reported, then elevation_corr is required"
    # })
    # errs = [*errs, checkData(**args)]
    # print('end discretewq-custom-check 5')    
    
    # #check 6: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Datum is required
    # print('begin discretewq-custom-check 6')
    # args.update({
    #     "dataframe": watermeta,
    #     "tablename": 'tbl_waterquality_metadata',
    #     "badrows": watermeta[            
    #         ((watermeta['elevation_ellipsoid'].notna()) | (watermeta['elevation_orthometric'].notna())) & (watermeta['elevation_datum'].isna())
    #     ].tmp_row.tolist(), 
    #     "badcolumn": "elevation_datum",
    #     "error_type": "Missing Value",
    #     "error_message" : "If Elevation_Ellipsoid or Elevation_Orthometric is reported, then elevation_datum is required"
    # })
    # errs = [*errs, checkData(**args)]
    # print('end discretewq-custom-check 6')    
     
    print("End WaterQuality Metadata Checks...")

    # End WaterQuality Metadata Checks
    ############################## -- End of WaterQuality Metadata checks --####################################################################
    
    ###################################################################################
    print("Start of WaterQuality data custom checks: ")

    # Check 7: Range for conductivity, with conductivity_units as uS/cm, must be between [0, 100] or -88
    print('begin discretewq-custom-check 7')
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
    print('end discretewq-custom-check 7')

    # Check 8: Range for tds, with tds_units as ppt, must be between [0, 100] or -88
    print('begin discretewq-custom-check 8')
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
    print('end discretewq-custom-check 8')

    # Check 9: Range for ph_teststrip must be within [1, 14] or -88
    print('begin discretewq-custom-check 9')
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
    print('end discretewq-custom-check 9')

    # Check 10: Range for ph_probe must be within [1, 14] or -88
    print('begin discretewq-custom-check 10')
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
    print('end discretewq-custom-check 10')

    # Check 11: Range for salinity, with salinity_units as ppt, must be between [0, 100] or -88 
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
    print('end discretewq-custom-check 11')

    # Check 12: Range for do, with do_units as mg/l, must be within [0, 20] or -88
    print('begin discretewq-custom-check 12')
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['do_mgl'] != -88) & 
            (waterdata['do_units'] == 'mg/l') &
            (~waterdata['do_mgl'].between(0, 20))
        ].tmp_row.tolist(),
        "badcolumn": "do_mgl",
        "error_type": "Value Out of range",
        "error_message" : "do_mgl values must be between 0 and 20."
    })
    errs = [*errs, checkData(**args)]
    print('check discretewq-custom-check 12')

    # Check 13: Range for airtemp, with airtemp_units as C, must be between [0, 50] or -88
    print('begin discretewq-custom-check 13')
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['airtemp'] != -88) &
            (waterdata['airtemp_units'] == 'C') &
            (~waterdata['airtemp'].between(0, 50))
        ].tmp_row.tolist(),
        "badcolumn": "airtemp",
        "error_type": "Value Out of range",
        "error_message" : "If airtemp_unit is C, airtemp must be between 0 and 50."
    })
    errs = [*errs, checkData(**args)]
    print('end discretewq-custom-check 13')

    # Check 14: Range for h2otemp, with h2otemp_units as C, must be between [0, 50] or -88
    print('begin discretewq-custom-check 14')
    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[
            (waterdata['h2otemp'] != -88) &
            (waterdata['h2otemp_units'] == 'C') &
            (~waterdata['h2otemp'].between(0, 50))
        ].tmp_row.tolist(),
        "badcolumn": "h2otemp",
        "error_type": "Value Out of Range",
        "error_message" : "If h2otemp_unit is C, h2otemp values must be between 0 and 50."
    })
    errs = [*errs, checkData(**args)]
    print('end discretewq-custom-check 14')

    #check 15: If H2OTemp is reported, then H2OTemp_Units cannot be 'Not Recorded'
    print('begin discretewq-custom-check 15')
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
    print('end discretewq-custom-check 15')    

    #check 16: If AirTemp is reported, then AirTemp_Units cannot be 'Not Recorded'
    print('begin discretewq-custom-check 16')
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
    print('end discretewq-custom-check 16')  

    #check 17: If DO_mgL is reported, then DO_Units cannot be 'Not Recorded'
    print('begin discretewq-custom-check 17')
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
    print('end discretewq-custom-check 17')   

    #check 18: If Salinity is reported, then Salinity_Units cannot be 'Not Recorded'    
    print('begin discretewq-custom-check 18')
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
    print('end discretewq-custom-check 18') 

    #check 19: If TDS_ppt is reported, then TDS_Units cannot be 'Not Recorded'
    print('begin discretewq-custom-check 19')
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
    print('end discretewq-custom-check 19')   

    #check 20: If Conductivity is reported, then Conductivity_Units cannot be 'Not Recorded'
    print('begin discretewq-custom-check 20')
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
    print('end discretewq-custom-check 20')  

    print("End of WaterQuality data custom checks! ")
    ###################################################################################

    print("-------------done custom check----------")
    
    
    
    
    return {'errors': errs, 'warnings': warnings}