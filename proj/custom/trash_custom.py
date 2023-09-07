# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import re
import pandas as pd
import datetime as dt
import time

def trash(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # define errors and warnings list
    errs = []
    warnings = []


    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # This data type should only have tbl_example
    # example = all_dfs['tbl_example']

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    # args = {
    #     "dataframe": example,
    #     "tablename": 'tbl_example',
    #     "badrows": [],
    #     "badcolumn": "",
    #     "error_type": "",
    #     "is_core_error": False,
    #     "error_message": ""
    # }

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": df[df.temperature != 'asdf'].index.tolist(),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    # return {'errors': errs, 'warnings': warnings}

    trashsiteinfo = all_dfs['tbl_trashsiteinfo']
    trashsiteinfo['tmp_row'] = trashsiteinfo.index
    trashtally = all_dfs['tbl_trashquadrattally']
    trashtally['tmp_row'] = trashtally.index
    trashvisualassessment = all_dfs['tbl_trashvisualassessment']
    trashvisualassessment['tmp_row'] = trashvisualassessment.index
    trashphotodoc = all_dfs['tbl_trashphotodoc']
    trashphotodoc['tmp_row'] = trashphotodoc.index

    # removed this due the following error: length of values (1) does not match length of index (55) -- when dropping a data file 
    # not sure why this happened but the tmp_row returned an issue after this length mismatch was no longer an issue
    # likely has to do with assignin tmp_row column -- the following lines did not run/populate new column
    # trashsiteinfo = all_dfs['tbl_trashsiteinfo'].assign(tmp_row = all_dfs['tbl_trashsiteinfo'].index)
    # trashtally = all_dfs['tbl_trashtally'].assign(tmp_row = all_dfs['tbl_trashsiteinfo'].index)
    # trashvisualassessment = all_dfs['tbl_trashvisualassessment'].assign(tmp_row = all_dfs['tbl_trashvisualassessment'])
    # trashphotodoc = all_dfs['tbl_trashphotodoc'].assign(tmp_row = all_dfs['tbl_trashphotodoc'])


    trashsiteinfo_args = {
        "dataframe": trashsiteinfo,
        "tablename": 'tbl_trashsiteinfo',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trashtally_args = {
        "dataframe": trashtally,
        "tablename": 'tbl_trashquadrattally',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trashvisualassessment_args = {
        "dataframe": trashvisualassessment,
        "tablename": 'tbl_trashvisualassessment',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trashphotodoc_args = {
        "dataframe": trashphotodoc,
        "tablename": 'tbl_trashphotodoc',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    
    print(" after args")

    
    ######################################################################################################################
    #--------------------------------------------------------------------------------------------------------------------#
    #--------------------------------------------- SITE INFORMATION CHECKS ----------------------------------------------#
    #--------------------------------------------------------------------------------------------------------------------#
    ###################################################################################################################### 

    print('# SKIPPING CHECK 1: no datum field in tbl_trashsiteinfo')
    # print("# CHECK - 1")    
    # # Description: If datum is 'Other (comment required)', then comment is required for trashsiteinfo.
    # # Created Coder: Unknown
    # # Created Date: Unknown
    # # Last Edited Date: 08/24/23
    # # Last Edited Coder: Caspian Thackeray
    # # NOTE (08/23/23): Copied from SMC and added formatting comments
    # # NOTE (08/24/23): Skipped because tbl_trashsiteinfo doesn't have a datum field


    # errs.append(
    #     checkData(
    #         'tbl_trashsiteinfo',
    #         trashsiteinfo[(trashsiteinfo.datum == 'Other (comment required)') & (trashsiteinfo.comments.isna())].tmp_row.tolist(),
    #         'comments',
    #         'Undefined Error',
    #         'Datum field is Other (comment required). Comments field is required.'
    #         )
    # )
    # print("# END OF CHECK - 1")
 

    print("# CHECK - 2")
    # Description: StartTime/EndTime needs to be in the format HH:MM, and they need to be in the 24-hour range(0-24:0-59)
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments


    # time_regex = re.compile("^(2[0-3]|[01]?[0-9]):([0-5]?[0-9])$")
    correct_time_format = r'^(0?[0-9]|1\d|2[0-3]):([0-5]\d)$' 
   
    # StartTime Check
    errs.append(
        checkData(
            tablename     = 'tbl_trashsiteinfo',
            badrows       = trashsiteinfo[trashsiteinfo['starttime'].apply(lambda x: not bool(re.match(correct_time_format, x)))].tmp_row.tolist(),
            badcolumn     = 'starttime',
            error_type    = 'Time Formatting Error',
            error_message = 'starttime needs to be in the format HH:MM, and they need to be in the 24-hour range military time'
        )
    )
    
    # EndTime Check
    errs.append(
        checkData(
            tablename     = 'tbl_trashsiteinfo',
            badrows       = trashsiteinfo[trashsiteinfo['endtime'].apply(lambda x: not bool(re.match(correct_time_format, x)))].tmp_row.tolist(),
            badcolumn     = 'endtime',
            error_type    = 'Time Formatting Error',
            error_message = 'EndTime needs to be in the format HH:MM, and they need to be in the 24-hour range'
        )
    )  

    # #Start Time Checker
    # errs.append(
    #     checkData(
    #         'tbl_trashsiteinfo',
    #         trashsiteinfo[trashsiteinfo.starttime.apply(lambda x: not bool(re.match(time_regex, x)))].tmp_row.tolist(),
    #         'starttime',
    #         'Time Formatting Error ',
    #         'starttime needs to be in the format HH:MM, and they need to be in the 24-hour range military time'
    #     )
    # )
    
    # #End Time Checker 
    # errs.append(
    #     checkData(
    #         'tbl_trashsiteinfo',
    #         trashsiteinfo[(trashsiteinfo["endtime"].apply(lambda x: not bool(re.match(time_regex, x))))].tmp_row.tolist(),
    #         'endtime',
    #         'Undefined Error',
    #         'EndTime needs to be in the format HH:MM, and they need to be in the 24-hour range'
    #     )
    # )

    print("# END OF CHECK - 2")
    
    
    print("# CHECK - 3")
    # Description: Start Time needs to be before end time
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments

    
    if (
        all(
            [
                trashsiteinfo['starttime'].apply(lambda x: bool(re.match(correct_time_format, x))).all(), 
                trashsiteinfo['endtime'].apply(lambda x: bool(re.match(correct_time_format, x))).all()
            ]
        )
    ):
        
        trashsiteinfo['starttime'] = pd.to_datetime(trashsiteinfo['starttime'], format='%H:%M').dt.time
        trashsiteinfo['endtime'] = pd.to_datetime(trashsiteinfo['endtime'], format='%H:%M').dt.time

        errs.append(
            checkData(
                tablename     = 'tbl_trashsiteinfo',
                badrows       = trashsiteinfo[(trashsiteinfo["starttime"] > trashsiteinfo["endtime"])].tmp_row.tolist(),
                badcolumn     = 'starttime',
                error_type    = 'Undefined Error',
                error_message = 'StartTime must be before EndTime'
            )
        )

    print("# END CHECK - 3")

    ######################################################################################################################
    #--------------------------------------------------------------------------------------------------------------------#
    #----------------------------------------------- TRASH TALLY CHECKS -------------------------------------------------#
    #--------------------------------------------------------------------------------------------------------------------#
    ###################################################################################################################### 

    print("# CHECK - 4")
    # Description: If debriscategory contains Other then comment is required
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments
    
    errs.append(
        checkData(
            tablename     = 'tbl_trashquadrattally',
            badrows       = trashtally[(trashtally.debriscategory == 'Other') & (trashtally.comments.isna())].tmp_row.tolist(),
            badcolumn     = 'comments',
            error_type    = 'Undefined Error',
            error_message = 'debriscategory field is Other (comment required). Comments field is required.'
            )
    )

    print("# END CHECK - 4")

   
    print("# CHECK - 5")
    # Description: Plastic debris entered must match a value in lu_trashplastics
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments
    # NOTE (08/23/23): Fixed check highlighting wrong column in excel sheet and wrong error message

    lu_trashplastic = pd.read_sql("SELECT plastic FROM lu_trashplastic",g.eng).plastic.tolist()

    errs.append(
        checkData(
            tablename     = 'tbl_trashquadrattally',
            badrows       = trashtally[(trashtally.debriscategory == 'Plastic') & (~trashtally.debrisitem.isin(lu_trashplastic))].tmp_row.tolist(),
            badcolumn     = 'debrisitem',
            error_type    = 'Undefined Error',
            error_message = 'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashplastic>lu_trashplastic</a>'
            )
    )

    print("# END CHECK - 5")
    
    
    print("# CHECK - 6")
    # Description: Fabric or cloth entered must match a value in lu_trashfabricandcloth
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments
    # NOTE (08/23/23): Fixed check highlighting wrong column in excel sheet and wrong error message 

    lu_trashfabricandcloth = pd.read_sql("SELECT fabricandcloth FROM lu_trashfabricandcloth",g.eng).fabricandcloth.tolist()

    errs.append(
        checkData(
            tablename     = 'tbl_trashquadrattally',
            badrows       = trashtally[(trashtally.debriscategory.str.lower() == 'fabric_cloth') & (~trashtally.debrisitem.isin(lu_trashfabricandcloth))].tmp_row.tolist(),
            badcolumn     = 'debrisitem',
            error_type    = 'Undefined Error',
            error_message = 'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashfabricandcloth>lu_trashfabricandcloth</a>'
            )
    )

    print("# END CHECK - 6")


    print("# CHECK - 7")
    # Description: trash value entered must match a value in lu_trashlarge
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments
    # NOTE (08/23/23): Fixed check highlighting wrong column in excel sheet and wrong error message

    lu_trashlarge = pd.read_sql("SELECT large FROM lu_trashlarge",g.eng).large.tolist()

    errs.append(
        checkData(
            tablename     = 'tbl_trashquadrattally',
            badrows       = trashtally[(trashtally.debriscategory.str.lower() == 'large') & (~trashtally.debrisitem.isin(lu_trashlarge))].tmp_row.tolist(),
            badcolumn     = 'debrisitem',
            error_type    = 'Undefined Error',
            error_message = 'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashlarge>lu_trashlarge</a>'
            )
    )
    print("# END CHECK - 7")


    print("# CHECK - 8")
    # Description: trash value entered must match a value in lu_trashbiodegradable
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments
    # NOTE (08/23/23): Fixed check highlighting wrong column in excel sheet and wrong error message

    lu_biodegradable = pd.read_sql("SELECT biodegradable FROM lu_trashbiodegradable",g.eng).biodegradable.tolist()

    errs.append(
        checkData(
            tablename     = 'tbl_trashquadrattally',
            badrows       = trashtally[(trashtally.debriscategory.str.lower() == 'biodegradable') & (~trashtally.debrisitem.isin(lu_biodegradable))].tmp_row.tolist(),
            badcolumn     = 'debrisitem',
            error_type    = 'Undefined Error',
            error_message = 'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashbiodegradable>lu_trashbiodegradable</a>'
            )
    )
    print("# END CHECK - 8")


    print("# CHECK - 9")
    # Description: trash value entered must match a value in lu_biohazard
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments
    # NOTE (08/23/23): Fixed check highlighting wrong column in excel sheet and wrong error message
    
    lu_biohazard = pd.read_sql("SELECT biohazard FROM lu_biohazard",g.eng).biohazard.tolist()
    errs.append(
        checkData(
            tablename     = 'tbl_trashquadrattally',
            badrows       = trashtally[(trashtally.debriscategory.str.lower() == 'biohazard') & (~trashtally.debrisitem.isin(lu_biohazard))].tmp_row.tolist(),
            badcolumn     = 'debrisitem',
            error_type    = 'Undefined Error',
            error_message = 'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_biohazard>lu_biohazard</a>'
            )
    )
    print("# END CHECK - 9")


    print("# CHECK - 10")
    # Description: trash value entered must match a value in lu_trashconstruction
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments
    # NOTE (08/23/23): Fixed check highlighting wrong column in excel sheet and wrong error message

    lu_construction = pd.read_sql("SELECT construction FROM lu_trashconstruction",g.eng).construction.tolist()
    errs.append(
        checkData(
            tablename     = 'tbl_trashquadrattally',
            badrows       = trashtally[(trashtally.debriscategory.str.lower() == 'construction') & (~trashtally.debrisitem.isin(lu_construction))].tmp_row.tolist(),
            badcolumn     = 'debrisitem',
            error_type    = 'Undefined Error',
            error_message = 'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashconstruction>lu_trashconstruction</a>'
            )
    )
    print("# END CHECK - 10")


    print("# CHECK - 11")
    # Description: trash value entered must match a value in lu_trashglass
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments
    # NOTE (08/23/23): Fixed check highlighting wrong column in excel sheet and wrong error message

    lu_glass = pd.read_sql("SELECT glass FROM lu_trashglass",g.eng).glass.tolist()
    errs.append(
        checkData(
            tablename     = 'tbl_trashquadrattally',
            badrows       = trashtally[(trashtally.debriscategory.str.lower() == 'glass') & (~trashtally.debrisitem.isin(lu_glass))].tmp_row.tolist(),
            badcolumn     = 'debrisitem',
            error_type    = 'Undefined Error',
            error_message = 'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashglass>lu_trashglass</a>'
            )
    )
    print("# END CHECK - 11")


    print("# CHECK - 12")
    # Description: trash value entered must match a value in lu_trashmetal
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments
    # NOTE (08/23/23): Fixed check highlighting wrong column in excel sheet and wrong error message

    lu_metal = pd.read_sql("SELECT metal FROM lu_trashmetal",g.eng).metal.tolist()
    errs.append(
        checkData(
            tablename     = 'tbl_trashquadrattally',
            badrows       = trashtally[(trashtally.debriscategory.str.lower() == 'metal') & (~trashtally.debrisitem.isin(lu_metal))].tmp_row.tolist(),
            badcolumn     = 'debrisitem',
            error_type    = 'Undefined Error',
            error_message = 'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashmetal>lu_trashmetal</a>'
            )
    )
    print("# END CHECK - 12")


    print("# CHECK - 13")
    # Description: trash value entered must match a value in lu_trashmiscellaneous
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments
    # NOTE (08/23/23): Fixed check highlighting wrong column in excel sheet and wrong error message

    lu_miscellaneous = pd.read_sql("SELECT miscellaneous FROM lu_trashmiscellaneous",g.eng).miscellaneous.tolist()
    errs.append(
        checkData(
            tablename     = 'tbl_trashquadrattally',
            badrows       = trashtally[(trashtally.debriscategory.str.lower() == 'miscellaneous') & (~trashtally.debrisitem.isin(lu_miscellaneous))].tmp_row.tolist(),
            badcolumn     = 'debrisitem',
            error_type    = 'Undefined Error',
            error_message = 'The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashmiscellaneous>lu_trashmiscellaneous</a>'
            )
    )
    print("# END CHECK - 12")


    print("# CHECK - 14")
    # Description: If debriscategory is None then debrisitem must be 'No Trash Present'
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 08/23/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/23/23): Copied from SMC and added formatting comments

    errs.append( 
        checkData(
            tablename     = 'tbl_trashquadrattally',
            badrows       = trashtally[(trashtally.debriscategory == 'None') & (trashtally.debrisitem != 'No Trash Present')].tmp_row.tolist(),
            badcolumn     = 'debrisitem',
            error_type    = 'Undefined Error',
            error_message = "If debriscategory is None then debrisitem must be 'No Trash Present'"
            )
    )
    print("# END CHECK - 14")
    
    return {'errors': errs, 'warnings': warnings}
    
