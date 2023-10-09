# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from datetime import date
from .functions import checkData,get_primary_key, mismatch, multicol_lookup_check

def edna_field(all_dfs):
    
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
    edna_metadata = all_dfs['tbl_edna_metadata']

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    # args = {
    #     "dataframe": edna_metadata,
    #     "tablename": 'tbl_edna_metadata',
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

    # Example of how to document a custom check
    #print("# CHECK - ")
    # Description:
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    #print("# END OF CHECK - ")

    return {'errors': errs, 'warnings': warnings}

def edna_lab(all_dfs):
    
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

    # These are the dataframes that got submitted for edna

    ednased = all_dfs['tbl_edna_sed_labbatch_data']
    ednawater = all_dfs['tbl_edna_water_labbatch_data']
    ednadata= all_dfs['tbl_edna_data']

    grabdeets = pd.read_sql("SELECT * FROM tbl_grabevent_details", g.eng)
    grabdeets_pkey = get_primary_key('tbl_grabevent_details', g.eng)

    ednased['tmp_row'] = ednased.index
    ednawater['tmp_row'] = ednawater.index
    ednadata['tmp_row'] = ednadata.index

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
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- EDNA Logic Checks ---------------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    sed_labbatch_pkey = get_primary_key('tbl_edna_sed_labbatch_data', g.eng)
    water_labbatch_pkey = get_primary_key('tbl_edna_water_labbatch_data', g.eng)
    data_pkey = get_primary_key('tbl_edna_data', g.eng)

    sed_labbatch_grabdeets_shared_pkey = list(set(sed_labbatch_pkey).intersection(set(grabdeets_pkey)))
    water_labbatch_grabdeets_shared_pkey = list(set(water_labbatch_pkey).intersection(set(grabdeets_pkey)))
    
    sed_labbatch_data_shared_pkey = list(set(sed_labbatch_pkey).intersection(set(data_pkey)))
    water_labbatch_data_shared_pkey = list(set(water_labbatch_pkey).intersection(set(data_pkey)))

    data_grabdeets_shared_pkey = list(set(data_pkey).intersection(set(grabdeets_pkey)))
    
    print("# CHECK - 1")
    # Description: Each labbatch data must correspond to grabeventdetails in database
    # Created Coder: Ayah H
    # Created Date: 09/08/2023
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/14/2023): Check updated to match other similar checks
    # NOTE (10/05/2023): Aria revised the error message

    args.update({
        "dataframe": ednased,
        "tablename": 'tbl_edna_sed_labbatch_data',
        "badrows": mismatch(ednased, grabdeets, sed_labbatch_grabdeets_shared_pkey),
        "badcolumn": ','.join(sed_labbatch_grabdeets_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each sed lab batch record must have corresponding record in the grab event details table in database.  Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(sed_labbatch_grabdeets_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": ednawater,
        "tablename": 'tbl_edna_sed_labbatch_data',
        "badrows": mismatch(ednawater, grabdeets, water_labbatch_grabdeets_shared_pkey),
        "badcolumn": ','.join(water_labbatch_grabdeets_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each water lab batch record must have corresponding record in the grab event details table in database.  Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(water_labbatch_grabdeets_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 1")



    print("# CHECK - 2")
    # Description: Each data must correspond to grabeventdetails in database
    # Created Coder: Caspian Thackeray
    # Created Date: 09/14/23
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/14/2023): Check written
    # NOTE (10/05/2023): Aria revised the error message

    args.update({
        "dataframe": ednadata,
        "tablename": 'tbl_edna_data',
        "badrows": mismatch(ednadata, grabdeets, data_grabdeets_shared_pkey),
        "badcolumn": ','.join(data_grabdeets_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each eDNA data record must have corresponding record in the grab event details table in database. Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(data_grabdeets_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 2")
    
    
    
    print("# CHECK - 3")
    # Description: Each labbatch data must correspond to data within submission
    # Created Coder: Caspian Thackeray
    # Created Date: 09/14/23
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/14/2023): Check written
    # NOTE (10/05/2023): Aria revised the error message
    
    args.update({
        "dataframe": ednased,
        "tablename": 'tbl_sed_labbatch_data',
        "badrows": mismatch(ednased, ednadata, sed_labbatch_data_shared_pkey),
        "badcolumn": ','.join(sed_labbatch_data_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each sed lab batch record must have corresponding eDNA data record. Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(sed_labbatch_data_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    
    args.update({
        "dataframe": ednawater,
        "tablename": 'tbl_water_labbatch_data',
        "badrows": mismatch(ednawater, ednadata, water_labbatch_data_shared_pkey),
        "badcolumn": ','.join(water_labbatch_data_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each water lab batch record must have corresponding eDNA data record. Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(water_labbatch_data_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    
    print("# END OF CHECK - 3")
    
    
    
    print("# CHECK - 4")
    # Description: Each data must correspond to labbatch data within submission
    # Created Coder: Caspian Thackeray
    # Created Date: 09/14/23
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/14/2023): Check written
    # NOTE (10/05/2023): Aria revised the error message

    args.update({
        "dataframe": ednadata,
        "tablename": 'tbl_edna_data',
        "badrows": mismatch(ednadata, ednawater, sed_labbatch_data_shared_pkey),
        "badcolumn": ','.join(sed_labbatch_data_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each eDNA data record must have corresponding sed lab batch record. Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(sed_labbatch_data_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": ednadata,
        "tablename": 'tbl_edna_data',
        "badrows": mismatch(ednadata, ednawater, water_labbatch_data_shared_pkey),
        "badcolumn": ','.join(water_labbatch_data_shared_pkey),
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each eDNA data record must have corresponding water lab batch record. Please submit the metadata for these records first based on these columns: {}".format(
            ','.join(water_labbatch_data_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 4")



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END OF EDNA Logic Checks --------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- Sed Labbatch Data Checks --------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    correct_time_format = r'^(0?[0-9]|1\d|2[0-3]):([0-5]\d)$'

    print("# CHECK - 5")
    # Description: Samplecollectiondate should be before preparationdate
    # Created Coder: Caspian Thackeray
    # Created Date: 09/14/23
    # Last Edited Date: 09/14/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE 09/14/23: Check written
    
    # formatting dates
    ednased['preparationdate'] = pd.to_datetime(ednased['preparationdate'], format='%d/%m/%Y').dt.date
    ednased['samplecollectiondate'] = pd.to_datetime(ednased['samplecollectiondate'], format='%d/%m/%Y').dt.date
    
    
    args.update({
        "dataframe": ednased,
        "tablename": 'tbl_edna_sed_labbatch_data',
        "badrows": ednased[(ednased["preparationdate"] > ednased["samplecollectiondate"])].tmp_row.tolist(),
        "badcolumn": "preparationdate",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Preparation date should be before sample collection date"
    })
    errs = [*errs, checkData(**args)]
    
    print("# END OF CHECK - 5")
    
    
    print("# CHECK - 6")
    # Description: Preparationtime should be before samplecollectiontime
    # Created Coder: Caspian Thackeray
    # Created Date: 09/14/23
    # Last Edited Date: 09/14/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE 09/14/23: Check written

    if (
        all(
            [
                ednased['preparationtime'].apply(lambda x: bool(re.match(correct_time_format, x))).all(), 
                ednased['samplecollectiontime'].apply(lambda x: bool(re.match(correct_time_format, x))).all()
            ]
        )
    ):
        
        ednased['preparationtime'] = pd.to_datetime(ednased['preparationtime'], format='%H:%M').dt.time
        ednased['samplecollectiontime'] = pd.to_datetime(ednased['samplecollectiontime'], format='%H:%M').dt.time

        args.update({
            "dataframe": ednased,
            "tablename": 'tbl_edna_sed_labbatch_data',
            "badrows": ednased[(ednased["preparationtime"] > ednased["samplecollectiontime"])].tmp_row.tolist(),
            "badcolumn": 'preparationtime',
            "error_type": 'Undefined Error',
            "is_core_error": False,
            "error_message": 'Preparation time should be before sample collection time'
        })
        errs = [*errs, checkData(**args)]
    
    print("# END OF CHECK - 6")
    
    
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END OF Sed Labbatch Data Checks -------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    
    
    
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- Water Labbatch Data Checks ------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 7")
    # Description: Preparationtime should be before samplecollectiontime
    # Created Coder: Caspian Thackeray
    # Created Date: 09/14/23
    # Last Edited Date: 09/14/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE 09/14/23: Check written

    if (
        all(
            [
                ednawater['preparationtime'].apply(lambda x: bool(re.match(correct_time_format, x))).all(), 
                ednawater['samplecollectiontime'].apply(lambda x: bool(re.match(correct_time_format, x))).all()
            ]
        )
    ):
        
        ednawater['preparationtime'] = pd.to_datetime(ednawater['preparationtime'], format='%H:%M').dt.time
        ednawater['samplecollectiontime'] = pd.to_datetime(ednawater['samplecollectiontime'], format='%H:%M').dt.time

        args.update({
            "dataframe": ednawater,
            "tablename": 'tbl_edna_water_labbatch_data',
            "badrows": ednawater[(ednawater["preparationtime"] > ednawater["samplecollectiontime"])].tmp_row.tolist(),
            "badcolumn": 'preparationtime',
            "error_type": 'Undefined Error',
            "is_core_error": False,
            "error_message": 'Preparation time should be before sample collection time'
        })
        errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 7")

    print("# CHECK - 8")
    # Description: Preparationdate should be before samplecollectiondate
    # Created Coder: Caspian Thackeray
    # Created Date: 09/14/23
    # Last Edited Date: 09/14/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE 09/14/23: Check written

    ednawater['preparationdate'] = pd.to_datetime(ednawater['preparationdate'], format='%d/%m/%Y').dt.date
    ednawater['samplecollectiondate'] = pd.to_datetime(ednawater['samplecollectiondate'], format='%d/%m/%Y').dt.date
    
    
    args.update({
        "dataframe": ednawater,
        "tablename": 'tbl_edna_water_labbatch_data',
        "badrows": ednawater[(ednawater["preparationdate"] > ednawater["samplecollectiondate"])].tmp_row.tolist(),
        "badcolumn": "preparationdate",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Preparation date should be before sample collection date"
    })
    errs = [*errs, checkData(**args)]
    
    print("# END OF CHECK - 8")

    
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END OF Water Labbatch Data Checks -----------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    
    
    
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- Edna Data Checks ----------------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 9")
    # Description: labreplicate must be consecutive within primary keys
    # Created Coder: Caspian Thackeray
    # Created Date: 09/14/23
    # Last Edited Date: 09/14/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE 09/14/23: Check written
    # NOTE 09/14/23: Error message will need to be re-written with the actual primary keys

    badrows = []
    for _, subdf in ednadata.groupby([x for x in data_pkey if x != 'labreplicate']):
        df = subdf.filter(items=[*data_pkey,*['tmp_row']])
        df = df.sort_values(by='labreplicate').fillna(0)
        rep_diff = df['labreplicate'].diff().dropna()
        all_values_are_one = (rep_diff == 1).all()
        if not all_values_are_one:
            badrows = [*badrows, *df.tmp_row.tolist()]

    args.update({
        "dataframe": ednadata,
        "tablename": "tbl_edna_data",
        "badrows": badrows,
        "badcolumn": "labreplicate",
        "error_type": "Custom Error",
        "error_message": "Replicate must be consecutive within a primary key group"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 9")




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # --------------------------------------------- END OF Edna Data Checks ---------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    return {'errors': errs, 'warnings': warnings}

