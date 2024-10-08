# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from pandas import DataFrame
from .functions import checkData, checkLogic, mismatch

def nutrients_lab(all_dfs):
    
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

    nutrilab = all_dfs['tbl_nutrients_labbatch_data']
    nutridata= all_dfs['tbl_nutrients_data']

    nutrilab['tmp_row'] = nutrilab.index
    nutridata['tmp_row'] = nutridata.index
    
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

    print("Begin Logic Checks...")
    eng = g.eng
    sql = eng.execute("SELECT * FROM tbl_nutrients_metadata")
    sql_df = DataFrame(sql.fetchall())
    sql_df.columns = sql.keys()
    nutrimeta = sql_df
    del sql_df
    # Logic Check 1: nutrients_metadata (db) & nutrients_labbatch_data (submission),nutrients metadata records do not exist in database
    print("Start of Logic check 1")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplecollectiontime', 'matrix', 'nutrientreplicate', 'sampleid']
    args.update({
        "dataframe": nutrilab,
        "tablename": "tbl_nutrients_labbatch_data",
        "badrows": mismatch(nutrilab, nutrimeta, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, matrix, nutrientreplicate, sampleid",
        "error_type": "Logic Error",
        "error_message": "Field submission for nutrients labdata is missing. Please verify that the nutrients field data has been previously submitted."
    })
    errs = [*errs, checkData(**args)]
    print("End of Logic check 1")
    print("check ran - logic - nutrients metadata records do not exist in database for nutrilab submission")

    # Logic Check 2: nutrients_labbatch_data & nutrients_data must have corresponding records within session submission
    #This is the grouped columns needed for check 2a and check2b which is slightly different from the groupedcolumns needed in check 1
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate','samplecollectiontime', 'matrix', 'nutrientreplicate', 'sampleid', 'preparationbatchid']

    # Logic Check 2a: nutrients_data missing records provided by nutrients_labbatch_data
    print("Start of Logic check 2a")
    args.update({
        "dataframe": nutrilab,
        "tablename": "tbl_nutrients_labbatch_data",
        "badrows": mismatch(nutrilab, nutridata, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, matrix, nutrientreplicate, sampleid, preparationbatchid",
        "error_type": "Logic Error",
        "error_message": "Records in nutrients_labbatch_data must have corresponding records in nutrients_data. Missing records in nutrients_data."
    })
    errs = [*errs, checkData(**args)]
    print("Start of Logic check 2a")
    print("check ran - logic - missing nutrients_data records")
    
    # Logic Check 2b: nutrients_labbatch_data missing records provided by nutrients_data
    print("Start of Logic check 2b")
    args.update({
        "dataframe": nutridata,
        "tablename": "tbl_nutrients_data",
        "badrows": mismatch(nutridata, nutrilab, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, matrix, nutrientreplicate, sampleid, preparationbatchid",
        "error_type": "Logic Error",
        "error_message": "Records in nutrients_data must have corresponding records in nutrients_labbatch_data. Missing records in nutrients_labbatch_data."
    })
    errs = [*errs, checkData(**args)]
    print("Start of Logic check 2b")
    print("check ran - logic - missing nutrients_labbatch_data records")

    print("End Nutrients Lab Logic Checks...")

    print("-----------CUSTOM CHECKS-------------")
    #check 4: If there is a value in result column, mdl cannot be empty
    print("Start of custom check 4")

    # args.update({
    #     "dataframe": nutridata,
    #     "tablename": "tbl_nutrients_data",
    #     "badrows":nutridata['analysisdate'].apply(lambda x: pd.Timestamp(str(x)).strftime('%Y-%m-%d') if not (pd.isnull(x) or x == 'Not recorded') else "00:00:00").index.tolist(),
    #     "badcolumn": "analysisdate",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your time input is out of range."
    # })
    # #errs = [*warnings, checkData(**args)]
    
    # args.update({
    #     "dataframe": nutrilab,
    #     "tablename": "tbl_nutrients_labbatch_data",
    #     "badrows":nutrilab['preparationtime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M:%S') if not (pd.isnull(x) or x == 'Not recorded') else "00:00:00").index.tolist(),
    #     "badcolumn": "preparationtime",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your time input is out of range."
    # })
    # #errs = [*errs, checkData(**args)]
    
    # args.update({
    #     "dataframe": nutridata,
    #     "tablename": "tbl_nutrients_data",
    #     "badrows":nutridata['samplecollectiontime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M:%S') if not (pd.isnull(x) or x == 'Not recorded') else "00:00:00").index.tolist(),
    #     "badcolumn": "samplecollectiontime",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Your time input is out of range."
    # })
    #errs = [*errs, checkData(**args)]
    
    print("End of custom check 4")
    
    return {'errors': errs, 'warnings': warnings}


def nutrients_field(all_dfs):
    
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

    nutrimeta = all_dfs['tbl_nutrients_metadata']
    
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