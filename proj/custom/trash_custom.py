# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, check_consecutiveness,get_primary_key,mismatch
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

    trashtimesearchtally = all_dfs['tbl_trashtimesearchtally']
    trashtimesearchtally['tmp_row'] = trashtimesearchtally.index

    trashsamplearea = all_dfs['tbl_trashsamplearea']
    trashsamplearea['tmp_row'] = trashsamplearea.index

    trashsiteinfo_pkey = get_primary_key('tbl_trashsiteinfo', g.eng)
    trashsamplearea_pkey = get_primary_key('tbl_trashsamplearea', g.eng)
    trashtally_pkey = get_primary_key('tbl_trashquadrattally', g.eng)
    trashtimesearchtally_pkey = get_primary_key('tbl_trashtimesearchtally',g.eng)
    trashvisualassessment_pkey = get_primary_key('tbl_trashvisualassessment',g.eng)

    site_area_shared_pkey =  [x for x in trashsiteinfo_pkey if x in trashsamplearea_pkey]
    visualassessment_searchtally_shared_pkey = [x for x in trashtimesearchtally_pkey if x in trashvisualassessment_pkey]
    trashtally_trashsamplearea_shared_pkey =  [x for x in trashtally_pkey if x in trashsamplearea_pkey]
    
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
    print("# LOGIC CHECK - 1")
        # Description: Recs in trashtimesearchtally must have matching records in trashvisualassessment and vise versa on shared primary keys
        # Created Coder: Ayah Halabi
        # Created Date: 11/21/2023
        # Last Edited Date: 

    errs.append( 
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows= mismatch(trashtimesearchtally,trashvisualassessment,visualassessment_searchtally_shared_pkey),
            badcolumn=','.join(visualassessment_searchtally_shared_pkey),
            error_type='Undefined Error',
            error_message=f"Records in the trashtimesearchtally should have the corresponding records in the trashvisualassessment based on these columns  {','.join(visualassessment_searchtally_shared_pkey)}"
            )
    )

    print("# END LOGIC CHECK - 1")

    print("# LOGIC CHECK - 16")
        # Description: Recs in trashtimesearchtally must have matching records in trashvisualassessment and vise versa on shared primary keys
        # Created Coder: Ayah Halabi
        # Created Date: 11/21/2023
        # Last Edited Date:
    
    errs.append( 
        checkData(
            tablename='tbl_trashvisualassessment',
            badrows= mismatch(trashvisualassessment,trashtimesearchtally,visualassessment_searchtally_shared_pkey),
            badcolumn=','.join(visualassessment_searchtally_shared_pkey),
            error_type='Undefined Error',
            error_message=f"Records in the trashvisualassessment should have the corresponding records in the trashtimesearchtally based on these columns  {','.join(visualassessment_searchtally_shared_pkey)}"
            )
    )

    print("# END LOGIC CHECK - 16")

    print("# LOGIC CHECK - 2")
    # Description: The number of quadrats listed in trashsiteinfo should have corresponding row in trashsamplearea
    # Created Coder: Aria Askaryar  
    # Created Date: 11/15/2023
    # Last Edited Date: 11/17/2023
    # Last Edited Coder: Ayah
    # NOTE (11/15/2023): Aria - Created logic check 2
    # NOTE (11/17/2023): Ayah - Finished logic Check 2

    merged_df = pd.merge(trashsiteinfo,trashsamplearea, on= site_area_shared_pkey)
    #grouped_df contains:
    # 1.what the number of quadrats should be ('numberofquadrats':'max') <- I put max becasue they are all the same value 
    # 2.how many rows are in each group ('quadrat': 'count') , 
    # 3. the index of table I am checking ('tmp_row_y': list)
    grouped_df = merged_df.groupby(site_area_shared_pkey).agg({'numberofquadrats':'max', 'quadrat': 'count', 'tmp_row_y': list}).reset_index()
    #The output I get from grouped_df is in this from [[],[],[]] so I need to change it to [   ]
    badrows_list = grouped_df[grouped_df['numberofquadrats'] != grouped_df['quadrat']].tmp_row_y.to_list() 
    badrows = []
    for i in badrows_list: 
        badrows = badrows + i
    badrows = sorted(badrows)

    errs.append( 
        checkData(
            tablename='tbl_trashsamplearea',
            badrows= badrows,
            badcolumn='quadrat',
            error_type='Undefined Error',
            error_message=f"The number of quadrats listed in trashsamplearea does not match numberofquadrats in trashsiteinfor for group based on these columns {','.join(site_area_shared_pkey)}"
            )
    )


    grouped_df = merged_df.groupby(site_area_shared_pkey).agg({'numberofquadrats':'max', 'quadrat': 'count', 'tmp_row_x': 'max'}).reset_index()
    badrows = grouped_df[grouped_df['numberofquadrats'] != grouped_df['quadrat']].tmp_row_x.to_list()
    badrows = sorted(badrows)

    errs.append( 
        checkData(
            tablename='tbl_trashsiteinfo',
            badrows= badrows,
            badcolumn='numberofquadrats',
            error_type='Undefined Error',
            error_message=f"The value of numberofquadrats in trashsiteinfor does not match the total quadrats in trashsamplearea for the groups based on these columns {','.join(site_area_shared_pkey)}"
            )
    )

    
    print("# END LOGIC CHECK - 2")

    print("# LOGIC CHECK - 3")

    #Aria's code
    # Description: If trash is 'Yes' in trashsamplearea, then information should exist in trashquadrattally – at least one row
    # Created Coder: Aria Askaryar
    # Created Date: 11/14/23
    # Last Edited Date: 11/16/23
    # Last Edited Coder: Ayah Halabi
    # NOTE (11/16/2023) Pkeys were hardcoded so I changed them to the dynamic form - Ayah 
   
    # ids_trashsamplearea_yes = set(trashsamplearea[trashsamplearea['trash'] == 'Yes'][trashsamplearea_pkey].apply(tuple, axis=1))
    # ids_trashquadrattally = set(trashtally[trashtally_pkey].apply(tuple, axis=1))
    # missing_entries = ids_trashsamplearea_yes - ids_trashquadrattally

    # errs.append(
    #     checkData(
    #         tablename='tbl_trashsamplearea',
    #         badrows= trashsamplearea[trashsamplearea[trashsamplearea_pkey].apply(tuple, axis=1).isin(missing_entries)].tmp_row.tolist(),
    #         badcolumn='trash',
    #         error_type='Undefined Error',
    #         error_message='If trash is "Yes" in trashsamplearea, then information should exist in trashquadrattally.'          
    #     )
    # )

    #I will ask duy about this on 11/28/2023
    ## Description: trashsamplearea should have matching records in trashquadrattally
    ## Created Coder: Ayah Halabi
    ## Created Date: 11/21/2023
    ## Last Edited Date:

    errs.append( 
        checkData(
            tablename='tbl_trashsamplearea',
            badrows= mismatch(trashsamplearea,trashtally,trashtally_trashsamplearea_shared_pkey ),
            badcolumn=','.join(trashtally_trashsamplearea_shared_pkey ),
            error_type='Undefined Error',
            error_message=f"Records in the trashsamplearea should have the corresponding records in the trashquadrattally based on these columns  {','.join(trashtally_trashsamplearea_shared_pkey )}"
            )
    )

    print("# END LOGIC CHECK - 3")    



    print("# LOGIC CHECK - 17")
    # Description: if trash is 'No' in trashsamplearea, then the corresponding record should have None in trashdebriscategory and debrisitem should say 'No Trash present'
    # Created Coder:  Ayah Halabi
    # Created Date: 11/20/23
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (11/14/23): Aria - wrote logic check 3
    # NOTE (11/16/23): Ayah - primary keys were hardcoded, so I changed their format to be more dynamic
    # NOTE (11/22/2023): Ayah - check changed completely so had to be redone. 
    
    merged_tally_area = pd.merge(trashtally,trashsamplearea, on= trashtally_trashsamplearea_shared_pkey)
    badrows = merged_tally_area[(merged_tally_area['trash'] == 'No') \
                                & (merged_tally_area['debrisitem'] != 'No Trash Present') \
                                    & (merged_tally_area['debriscategory'] != 'None')].tmp_row_x.tolist()
    errs.append(
        checkData(
            tablename='tbl_trashquadrattally',
            badrows= badrows,
            badcolumn='debriscategory,debrisitem',
            error_type='Undefined Error',
            error_message=f"Trash is 'No' in trashsamplearea for these record, however trashdebriscategory is not 'None' and debrisitem is not 'No Trash present' for these rows based on {','.join(trashtally_trashsamplearea_shared_pkey)}."
        )
    )
    badrows = merged_tally_area[(merged_tally_area['trash'] == 'No') \
                                & (merged_tally_area['debrisitem'] != 'No Trash Present') \
                                    & (merged_tally_area['debriscategory'] != 'None')].tmp_row_y.tolist()
    errs.append(
        checkData(
            tablename='tbl_trashsamplearea',
            badrows= badrows,
            badcolumn='trash',
            error_type='Undefined Error',
            error_message=f"Trash is 'No' in trashsamplearea for these record, however trashdebriscategory is not 'None' and debrisitem is not 'No Trash present' for these rows based on {','.join(trashtally_trashsamplearea_shared_pkey)}."
        )
    )

    print("# END LOGIC CHECK - 17")

    print("# LOGIC CHECK - 18")
    # Description: quadrat must be consecutive within primary keys ('projectid','siteid','sampledate','quadrat','stationno','estuaryname','transect')
    # Created Coder: Ayah Halabi  
    # Created Date: 11/16/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    
    if not trashsamplearea.empty:
        groupby_cols = ['projectid','estuaryname','sampledate','siteid','stationno','transect']
        trashsamplearea['tmp_row'] = trashsamplearea.index
        errs.append(
            checkData(
                tablename='tbl_trashsamplearea',
                badrows= check_consecutiveness(trashsamplearea, groupby_cols, 'quadrat'),
                badcolumn='quadrat',
                error_type='Undefined Error',
                error_message=f"quadrat values must be consecutive for each transect. Records are grouped by {','.join(groupby_cols)}"
            )
        )
        

    print("# END LOGIC CHECK - 18")

    ######################################################################################################################
    #--------------------------------------------------------------------------------------------------------------------#
    #--------------------------------------------- END OF Logic Checks --------------------------------------------------#
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
    print(trashtally[(trashtally.debriscategory == 'Other') & (trashtally.comments.isna())].tmp_row.tolist())
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
    print("# END CHECK - 13")


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

    ######################################################################################################################
    #--------------------------------------------------------------------------------------------------------------------#
    #----------------------------------------------- Trash Time Search Tally --------------------------------------------#
    #--------------------------------------------------------------------------------------------------------------------#
    ######################################################################################################################
    # Description: Either resulttotal or resulttotaltext needs to be filled in
    # Created Coder: Ayah
    # Created Date: 11/22/2023
    # Last Edited Date: 
    # Last Edited Coder:
    print("# END CHECK - 19")
    errs.append( 
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally['resulttotal'].isna()) & (trashtimesearchtally['resulttotaltext'].isna())].tmp_row.tolist(),
            badcolumn='resulttotal, resulttotaltext',
            error_type='Missing value Error',
            error_message=" Both resulttotal and resulttotaltext cannot be empty, please indicate a value in one or the other"
            )
    )
    print("# END CHECK - 19")
    
    # Description: Either resulttotal or resulttotaltext needs to be filled in
    # Created Coder: Ayah
    # Created Date: 11/22/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    print("# END CHECK - 20")
    errs.append( 
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally['width'] <0) &  (trashtimesearchtally['width']!= -88)].tmp_row.tolist(),
            badcolumn='width',
            error_type='Sign Error',
            error_message=" Width must be non-negative"
            )
    )
    print("# END CHECK - 20")
    
    print("# CHECK - 20")
    # Description: Plastic debris entered must match a value in lu_trashplastics
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 04/02/24
    # Last Edited Coder: Ayah Halabi
    # NOTE (08/23/23):Copied from trashtally checks
    errs.append(
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally.debriscategory == 'Plastic') & (~trashtimesearchtally.debrisitem.isin(lu_trashplastic))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashplastic target="_blank">lu_trashplastic</a>'
        )
    )

    print("# END CHECK - 20")
    
    
    print("# CHECK -21")
    # Description: Fabric or cloth entered must match a value in lu_trashfabricandcloth
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 04/02/24
    # Last Edited Coder: Ayah Halabi
    # NOTE (08/23/23):Copied from trashtally checks

    errs.append(
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally.debriscategory.str.lower() == 'fabric_cloth') & (~trashtimesearchtally.debrisitem.isin(lu_trashfabricandcloth))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashfabricandcloth target="_blank">lu_trashfabricandcloth</a>'
        )
    )

    print("# END CHECK - 21")


    print("# CHECK - 22")
    # Description: trash value entered must match a value in lu_trashlarge
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 04/02/24
    # Last Edited Coder: Ayah Halabi
    # NOTE (08/23/23):Copied from trashtally checks
    
    errs.append(
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally.debriscategory.str.lower() == 'large') & (~trashtimesearchtally.debrisitem.isin(lu_trashlarge))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashlarge target="_blank">lu_trashlarge</a>'
            )
    )
    print("# END CHECK - 22")


    print("# CHECK - 23")
    # Description: trash value entered must match a value in lu_trashbiodegradable
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 04/02/24
    # Last Edited Coder: Ayah Halabi
    # NOTE (08/23/23):Copied from trashtally checks
    
    errs.append(
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally.debriscategory.str.lower() == 'biodegradable') & (~trashtimesearchtally.debrisitem.isin(lu_biodegradable))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashbiodegradable target="_blank">lu_trashbiodegradable</a>'
        )
    )
    print("# END CHECK - 23")


    print("# CHECK - 24")
    # Description: trash value entered must match a value in lu_biohazard
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 04/02/24
    # Last Edited Coder: Ayah Halabi
    # NOTE (08/23/23):Copied from trashtally checks
    
    
    errs.append(
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally.debriscategory.str.lower() == 'biohazard') & (~trashtimesearchtally.debrisitem.isin(lu_biohazard))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_biohazard target="_blank">lu_biohazard</a>'
        )
    )
    print("# END CHECK - 24")


    print("# CHECK - 25")
    # Description: trash value entered must match a value in lu_trashconstruction
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 04/02/24
    # Last Edited Coder: Ayah Halabi
    # NOTE (08/23/23):Copied from trashtally checks
    
    lu_construction = pd.read_sql("SELECT construction FROM lu_trashconstruction",g.eng).construction.tolist()
    errs.append(
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally.debriscategory.str.lower() == 'construction') & (~trashtimesearchtally.debrisitem.isin(lu_construction))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashconstruction target="_blank">lu_trashconstruction</a>'
        )
    )
    print("# END CHECK - 25")


    print("# CHECK - 26")
    # Description: trash value entered must match a value in lu_trashglass
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 04/02/24
    # Last Edited Coder: Ayah Halabi
    # NOTE (08/23/23):Copied from trashtally checks
    
    errs.append(
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally.debriscategory.str.lower() == 'glass') & (~trashtimesearchtally.debrisitem.isin(lu_glass))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashglass target="_blank">lu_trashglass</a>'
            )
    )
    print("# END CHECK - 26")


    print("# CHECK - 27")
    # Description: trash value entered must match a value in lu_trashmetal
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 04/02/24
    # Last Edited Coder: Ayah Halabi
    # NOTE (08/23/23):Copied from trashtally checks
    
    errs.append(
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally.debriscategory.str.lower() == 'metal') & (~trashtimesearchtally.debrisitem.isin(lu_metal))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashmetal target="_blank">lu_trashmetal</a>'
        )
    )
    print("# END CHECK - 27")


    print("# CHECK - 28")
    # Description: trash value entered must match a value in lu_trashmiscellaneous
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 04/02/24
    # Last Edited Coder: Ayah Halabi
    # NOTE (08/23/23):Copied from trashtally checks
    

    errs.append(
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally.debriscategory.str.lower() == 'miscellaneous') & (~trashtimesearchtally.debrisitem.isin(lu_miscellaneous))].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message='The value you entered does not match the lookup list <a href=scraper?action=help&layer=lu_trashmiscellaneous target="_blank">lu_trashmiscellaneous</a>'
        )
    )
    print("# END CHECK - 28")


    print("# CHECK - 29")
    # Description: If debriscategory is None then debrisitem must be 'No Trash Present'
    # Created Coder: Unknown
    # Created Date: Unknown
    # Last Edited Date: 04/02/24
    # Last Edited Coder: Ayah Halabi
    # NOTE (08/23/23):Copied from trashtally checks

    errs.append( 
        checkData(
            tablename='tbl_trashtimesearchtally',
            badrows=trashtimesearchtally[(trashtimesearchtally.debriscategory == 'None') & (trashtimesearchtally.debrisitem != 'No Trash Present')].tmp_row.tolist(),
            badcolumn='debrisitem',
            error_type='Undefined Error',
            error_message="If debriscategory is None then debrisitem must be 'No Trash Present'"
            )
    )
    print("# END CHECK - 29")


    return {'errors': errs, 'warnings': warnings}
    
