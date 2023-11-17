# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, check_consecutiveness,get_primary_key
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

    #aria - i added this in since there was no df for tbl_trashsamplearea
    trashsamplearea = all_dfs['tbl_trashsamplearea']
    trashsamplearea['tmp_row'] = trashsamplearea.index

    trashsiteinfo_pkey = get_primary_key('tbl_trashsiteinfo', g.eng)
    trashsamplearea_pkey = get_primary_key('tbl_trashsamplearea', g.eng)
    trashtally_pkey = get_primary_key('tbl_trashquadrattally', g.eng)
    site_area_shared_pkey =  [x for x in trashsiteinfo_pkey if x in trashsamplearea_pkey]


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

    #Aria - I added this in since there was no df for tbl_trashsamplearea
    trashsamplearea_args = {
        "dataframe": trashsamplearea,
        "tablename": 'tbl_trashsamplearea',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # trashphotodoc_args = {
    #     "dataframe": trashphotodoc,
    #     "tablename": 'tbl_trashphotodoc',
    #     "badrows": [],
    #     "badcolumn": "",
    #     "error_type": "",
    #     "is_core_error": False,
    #     "error_message": ""
    # }
    
    print(" after args")

    ######################################################################################################################
    #--------------------------------------------------------------------------------------------------------------------#
    #--------------------------------------------- Logic Checks ---------------------------------------------------------#
    #--------------------------------------------------------------------------------------------------------------------#
    ###################################################################################################################### 

    print("# LOGIC CHECK - 2")
    # Description: The number of quadrats listed in trashsiteinfo should have corresponding row in trashsamplearea
    # Created Coder: Aria Askaryar  
    # Created Date: 11/15/2023
    # Last Edited Date: 11/15/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (11/15/2023): Aria - Created logic check 2

    # print("Aria start here ")
    # print(trashsiteinfo.columns)
    # #trashsiteinfo columns: ['projectid', 'siteid', 'estuaryname', 'sampledate', 'stationno','starttime', 'endtime', 'numberofquadrats', 'transect','transectlength', 'fieldcrew', 'comments',
    # print(trashsamplearea.columns)
    # #trashsamplearea columns: ['projectid', 'siteid', 'estuaryname', 'stationno', 'sampledate','transect', 'crossrackline', 'quadrat', 'trash', 'habitat', 'latitude','longitude', 'datum_latlon', 'comments']

    # shared_pk = ["projectid","siteid","stationno","sampledate","transect"]
    # max_quadrats = trashsamplearea.groupby(shared_pk)['quadrat'].max().reset_index(name='max_quadrat')

    # # Merge this with trashsiteinfo
    # check_df = pd.merge(
    #     trashsiteinfo,
    #     max_quadrats,
    #     on=shared_pk + ['transect'],
    #     how='left',
    #     suffixes=('', '_max')
    # )

    # # Check for mismatches where numberofquadrats in trashsiteinfo doesn't match the counted quadrats in trashsamplearea
    # check_df['mismatch'] = check_df['numberofquadrats'] != check_df['max_quadrats']
    # mismatched_rows = check_df[check_df['mismatch']]

    # errs.append( 
    #     checkData(
    #         tablename='tbl_trashsiteinfo',
    #         badrows= mismatched_rows.tmp_row.tolist(),
    #         badcolumn='numberofquadrats',
    #         error_type='Undefined Error',
    #         error_message="The number of quadrats listed in trashsiteinfo should have corresponding row in trashsamplearea"
    #         )
    # )
    print("# END LOGIC CHECK - 2")

    print("# LOGIC CHECK - 3")
    # Description: quadrat must be consecutive within primary keys ('projectid','siteid','sampledate','quadrat','stationno','estuaryname','transect')
    # Created Coder: Ayah Halabi  
    # Created Date: 11/16/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    trashsamplearea_pkey_filter = [x for x in trashsiteinfo_pkey if x !='quadrat']
    errs.append(
        checkData(
            tablename='tbl_trashsamplearea',
            badrows= check_consecutiveness(trashsamplearea,trashsamplearea_pkey_filter,'quadrat'),
            badcolumn='comments',
            error_type='Undefined Error',
            error_message=f"quadrat values must be consecutive in tbl_trashsamplearea. Records are grouped by {','.join(trashsamplearea_pkey)}"
        )
    )
    

    print("# END LOGIC CHECK - 3")

    print("# LOGIC CHECK - 4")
    # Description: If trash is 'Yes' in trashsamplearea, then information should exist in trashquadrattally – at least one row
    # Created Coder: Aria Askaryar
    # Created Date: 11/14/23
    # Last Edited Date: 11/16/23
    # Last Edited Coder: Ayah Halabi
    # NOTE (11/14/23): Aria - wrote logic check 3
    # NOTE (11/16/23): Ayah - primary keys were hardcoded, so I changed their format to be more dynamic
    
    ids_trashsamplearea_yes = set(trashsamplearea[trashsamplearea['trash'] == 'Yes'][trashsamplearea_pkey].apply(tuple, axis=1))
    ids_trashquadrattally = set(trashtally[trashtally_pkey].apply(tuple, axis=1))
    missing_entries = ids_trashsamplearea_yes - ids_trashquadrattally
    
    errs.append(
        checkData(
            tablename='tbl_trashsamplearea',
            badrows= trashsamplearea[trashsamplearea[trashsamplearea_pkey].apply(tuple, axis=1).isin(missing_entries)].tmp_row.tolist(),
            badcolumn='trash',
            error_type='Undefined Error',
            error_message='If trash is "Yes" in trashsamplearea, then information should exist in trashquadrattally.'
        )
    )

    print("# END LOGIC CHECK - 4")

    ######################################################################################################################
    #--------------------------------------------------------------------------------------------------------------------#
    #---------------------------------------------END OF Logic Checks 00000----------------------------------------------#
    #--------------------------------------------------------------------------------------------------------------------#
    ###################################################################################################################### 



    
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
 

    ######################################################################################################################
    #--------------------------------------------------------------------------------------------------------------------#
    #----------------------------------------------- TRASH TALLY CHECKS -------------------------------------------------#
    #--------------------------------------------------------------------------------------------------------------------#
    ###################################################################################################################### 

    print("# CHECK - 4")
    # Description: If debriscategory contains Other then comment is required
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 11/14/23
    # Last Edited Coder: Aria Askaryar
    # NOTE (08/23/23): Copied from SMC and added formatting comments
    # NOTE (11/14/23): Aria -QA'ed check
   
    errs.append(
        checkData(
            tablename='tbl_trashquadrattally',
            badrows=trashtally[(trashtally.debriscategory == 'Other') & (trashtally.comments.isna())].tmp_row.tolist(),
            badcolumn='comments',
            error_type='Undefined Error',
            error_message='debriscategory field is Other (comment required). Comments field is required.'
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
            tablename='tbl_trashquadrattally',
            badrows=trashtally[(trashtally.debriscategory == 'Plastic') & (~trashtally.debrisitem.isin(lu_trashplastic))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashplastic target="_blank">lu_trashplastic</a>'
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
            tablename='tbl_trashquadrattally',
            badrows=trashtally[(trashtally.debriscategory.str.lower() == 'fabric_cloth') & (~trashtally.debrisitem.isin(lu_trashfabricandcloth))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashfabricandcloth target="_blank">lu_trashfabricandcloth</a>'
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
            tablename='tbl_trashquadrattally',
            badrows=trashtally[(trashtally.debriscategory.str.lower() == 'large') & (~trashtally.debrisitem.isin(lu_trashlarge))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashlarge target="_blank">lu_trashlarge</a>'
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
            tablename='tbl_trashquadrattally',
            badrows=trashtally[(trashtally.debriscategory.str.lower() == 'biodegradable') & (~trashtally.debrisitem.isin(lu_biodegradable))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashbiodegradable target="_blank">lu_trashbiodegradable</a>'
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
            tablename='tbl_trashquadrattally',
            badrows=trashtally[(trashtally.debriscategory.str.lower() == 'biohazard') & (~trashtally.debrisitem.isin(lu_biohazard))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_biohazard target="_blank">lu_biohazard</a>'
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
            tablename='tbl_trashquadrattally',
            badrows=trashtally[(trashtally.debriscategory.str.lower() == 'construction') & (~trashtally.debrisitem.isin(lu_construction))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashconstruction target="_blank">lu_trashconstruction</a>'
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
            tablename='tbl_trashquadrattally',
            badrows=trashtally[(trashtally.debriscategory.str.lower() == 'glass') & (~trashtally.debrisitem.isin(lu_glass))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashglass target="_blank">lu_trashglass</a>'
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
            tablename='tbl_trashquadrattally',
            badrows=trashtally[(trashtally.debriscategory.str.lower() == 'metal') & (~trashtally.debrisitem.isin(lu_metal))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashmetal target="_blank">lu_trashmetal</a>'
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
            tablename='tbl_trashquadrattally',
            badrows=trashtally[(trashtally.debriscategory.str.lower() == 'miscellaneous') & (~trashtally.debrisitem.isin(lu_miscellaneous))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashmiscellaneous target="_blank">lu_trashmiscellaneous</a>'
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
            tablename='tbl_trashquadrattally',
            badrows=trashtally[(trashtally.debriscategory == 'None') & (trashtally.debrisitem != 'No Trash Present')].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message="If debriscategory is None then debrisitem must be 'No Trash Present'"
            )
    )
    print("# END CHECK - 14")

    return {'errors': errs, 'warnings': warnings}
    
