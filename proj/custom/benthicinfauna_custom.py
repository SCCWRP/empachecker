# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, mismatch, get_primary_key
import re


def benthicinfauna_lab(all_dfs):
    
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

    # benthicmeta = all_dfs['tbl_benthicinfauna_metadata']
    benthiclabbatch = all_dfs['tbl_benthicinfauna_labbatch']
    benthicabundance = all_dfs['tbl_benthicinfauna_abundance']
    benthicbiomass = all_dfs['tbl_benthicinfauna_biomass']
    grabevent_details = pd.read_sql("SELECT * FROM tbl_grabevent_details WHERE sampletype = 'infauna' ",g.eng)
    
    benthiclabbatch['tmp_row'] = benthiclabbatch.index
    benthicabundance['tmp_row'] = benthicabundance.index
    benthicbiomass['tmp_row'] = benthicbiomass.index
    grabevent_details['tmp_row'] = grabevent_details.index

    errs = []
    warnings = []

    args = {
        "dataframe": pd.DataFrame({}),
        "tablename": '',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Benthic Infauna Logic Checks ------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    benthiclabbatch_pkey = get_primary_key('tbl_benthicinfauna_labbatch', g.eng)
    benthicabundance_pkey = get_primary_key('tbl_benthicinfauna_abundance', g.eng)
    benthicbiomass_pkey = get_primary_key('tbl_benthicinfauna_biomass', g.eng)
    grabeventdet_pkey = get_primary_key('tbl_grabevent_details', g.eng)

    abundance_labbatch_shared_pkey = [x for x in benthicabundance_pkey if x in benthiclabbatch_pkey]
    abundance_biomass_shared_pkey = [x for x in benthicbiomass_pkey if x in benthicabundance_pkey]
    labbatch_abundance_shared_pkey = [x for x in benthiclabbatch_pkey if x in benthicabundance_pkey]
    labbatch_grabeventdet_shared_pkey = [x for x in benthiclabbatch_pkey if x in grabeventdet_pkey]
    labbatch_biomass_shared_pkey = [x for x in benthiclabbatch_pkey if x in benthicbiomass_pkey]


    print("# CHECK - 1")
    # Description: Each labbatch data must include corresponding records in grabevent_details
    # Created Coder: Duy
    # Created Date: 2/22/24
    # Last Edited Date:  NA
    # Last Edited Coder: NA
    # NOTE (09/27/23): Duy created the check, QA'ed
    # NOTE (2/22/24): Make sure that samplecollectiondate have the same format, so later when we do astype(str), it doesn't randomly add 00:00:00 to the date

    if 'samplecollectiondate' in benthiclabbatch.columns:
        benthiclabbatch['samplecollectiondate'] = pd.to_datetime(benthiclabbatch['samplecollectiondate'])
    if 'samplecollectiondate' in grabevent_details.columns:
        grabevent_details['samplecollectiondate'] = pd.to_datetime(grabevent_details['samplecollectiondate'])

    args.update({
        "dataframe": benthiclabbatch,
        "tablename": "tbl_benthicinfauna_labbatch",
        "badrows": mismatch(benthiclabbatch, grabevent_details, labbatch_grabeventdet_shared_pkey), 
        "badcolumn": ','.join(labbatch_grabeventdet_shared_pkey),
        "error_type": "Logic Error",
        "error_message": 
            "Records in benthicinfauna_labbatch must have corresponding field records. " +\
            "Please submit the field records using this template. " +\
            "<a href='/checker/templater?datatype=grab_field' target='_blank'>Field Template</a>."
    })
    errs = [*errs, checkData(**args)]  
    print("# END OF CHECK - 1")

    # CHECK - 2
    print("# CHECK - 2")
    # Description: Each record in benthicinfauna_labbatch must include corresponding record in benthicinfauna_abundance when abundance_recorded = 'yes'
    # Created Coder: Duy
    # Created Date: 12/22/24
    # Last Edited Date: NA
    # Last Edited Coder: NA

    if 'abundance_recorded' in benthiclabbatch.columns:
        args.update({
            "dataframe": benthiclabbatch[benthiclabbatch['abundance_recorded'].str.lower() == 'yes'],
            "tablename": "tbl_benthicinfauna_labbatch",
            "badrows": mismatch(
                benthiclabbatch[benthiclabbatch['abundance_recorded'].str.lower() == 'yes'], 
                benthicabundance, 
                abundance_labbatch_shared_pkey  # Correct parameter
            ),
            "badcolumn": ','.join(abundance_labbatch_shared_pkey),
            "error_type": "Logic Error",
            "error_message": 
                "Each record in benthicinfauna_labbatch with abundance_recorded = 'yes' must have a corresponding record in benthicinfauna_abundance."
        })
        errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")

    # CHECK - 3
    print("# CHECK - 3")
    # Description: Each record in benthicinfauna_labbatch must include corresponding record in benthicinfauna_biomass when biomass_recorded = 'yes'
    # Created Coder: Duy
    # Created Date: 12/22/24
    # Last Edited Date: NA
    # Last Edited Coder: NA

    if 'biomass_recorded' in benthiclabbatch.columns:
        args.update({
            "dataframe": benthiclabbatch[benthiclabbatch['biomass_recorded'].str.lower() == 'yes'],
            "tablename": "tbl_benthicinfauna_labbatch",
            "badrows": mismatch(
                benthiclabbatch[benthiclabbatch['biomass_recorded'].str.lower() == 'yes'], 
                benthicbiomass, 
                labbatch_biomass_shared_pkey  # Correct parameter
            ),
            "badcolumn": ','.join(labbatch_biomass_shared_pkey),
            "error_type": "Logic Error",
            "error_message": 
                "Each record in benthicinfauna_labbatch with biomass_recorded = 'yes' must have a corresponding record in benthicinfauna_biomass."
        })
        errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")

    # CHECK - 4
    print("# CHECK - 4")
    # Description: Each record in benthicinfauna_abundance must include corresponding record in benthicinfauna_labbatch
    # Created Coder: Duy
    # Created Date: 12/22/24
    # Last Edited Date: NA
    # Last Edited Coder: NA

    args.update({
        "dataframe": benthicabundance,
        "tablename": "tbl_benthicinfauna_abundance",
        "badrows": mismatch(
            benthicabundance, 
            benthiclabbatch, 
            abundance_labbatch_shared_pkey  # Correct parameter
        ),
        "badcolumn": ','.join(abundance_labbatch_shared_pkey),
        "error_type": "Logic Error",
        "error_message": 
            "Each record in benthicinfauna_abundance must have a corresponding record in benthicinfauna_labbatch."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 4")

    # CHECK - 5
    print("# CHECK - 5")
    # Description: Each record in benthicinfauna_biomass must include corresponding record in benthicinfauna_labbatch
    # Created Coder: Duy
    # Created Date: 12/22/24
    # Last Edited Date: NA
    # Last Edited Coder: NA

    args.update({
        "dataframe": benthicbiomass,
        "tablename": "tbl_benthicinfauna_biomass",
        "badrows": mismatch(
            benthicbiomass, 
            benthiclabbatch, 
            labbatch_biomass_shared_pkey  # Correct parameter
        ),
        "badcolumn": ','.join(labbatch_biomass_shared_pkey),
        "error_type": "Logic Error",
        "error_message": 
            "Each record in benthicinfauna_biomass must have a corresponding record in benthicinfauna_labbatch."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 5")


   


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Benthic Infauna Logic Checks ----------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Fish Logic Checks ----------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ----------------------------------------------- Biomasss Checks -------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
 
    print("# CHECK - 6")
    # Description: Biomass_g must be greater than or equal to 0. You can put -88 for missing values.
    # Created Coder: Caspian
    # Created Date: 9/27/23
    # Last Edited Date:  1/4/24
    # Last Edited Coder: Duy
    # NOTE (1/4/24): somehow this benthicbiomass[benthicbiomass['biomass_g'].apply(lambda x: (x < 0) & pd.notnull(x))] returns an empty
    # dataframe with no columns. So I changed the code's logic
    args.update({
        "dataframe": benthicbiomass,
        "tablename": "tbl_benthicinfauna_biomass",
        "badrows":  benthicbiomass[
                (benthicbiomass['biomass_g'] != -88) &
                (benthicbiomass['biomass_g'] < 0) &
                (~benthicbiomass['biomass_g'].isnull())
            ].tmp_row.tolist(),
        "badcolumn": 'biomass_g',
        "error_type" : "Logic Warning",
        "error_message" : "Biomass_g in benthicinfauna_biomass must be greater than or equal to 0. You can put -88 for missing values."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 6")
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # -------------------------------------------- END OF Biomasss Checks ---------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    return {'errors': errs, 'warnings': warnings}
