# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData
import re

def benthicinfauna_field(all_dfs):
    
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

    benthicmeta = all_dfs['tbl_benthicinfauna_metadata']
    #benthiclabbatch = all_dfs['tbl_benthicinfauna_labbatch']
    #benthicabundance = all_dfs['tbl_benthicinfauna_abundance']
    #benthicbiomass = all_dfs['tbl_benthicinfauna_biomass']
    

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    '''
    args = {
        "dataframe": df,
        "tablename": tbl,
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    '''
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename": '',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # Check: coresizediameter_cm corresponds to sample location == subtidal
    args = {
        "dataframe": benthicmeta,
        "tablename": 'tbl_benthicinfauna_metadata',
        "badrows": benthicmeta[(benthicmeta['samplelocation'] == 'subtidal') & (benthicmeta['coresizediameter_cm'] != 10)].index.to_list(),
        "badcolumn": "coresizediameter_cm",
        "error_type": "Unexpected Value",
        "is_core_error": False,
        "error_message": "The value for coresizediameter_cm where samplelocation recorded as 'subtidal' is expected to be 10."
    }
    warnings = [*warnings, checkData(**args)]
    print("check ran - benthicinfauna_metadata - coresizediamter_cm") # tested
    
    # Check: coresizedepth_cm corresponds to sample location == subtidal
    args = {
        "dataframe": benthicmeta,
        "tablename": 'tbl_benthicinfauna_metadata',
        "badrows": benthicmeta[(benthicmeta['samplelocation'] == 'subtidal') & (benthicmeta['coresizedepth_cm'] != 10)].index.to_list(),
        "badcolumn": "coresizediameter_cm",
        "error_type": "Unexpected Value",
        "is_core_error": False,
        "error_message": "The value for coresizedepth_cm where samplelocation recorded as 'subtidal' is expected to be 10."
    }
    warnings = [*warnings, checkData(**args)]
    print("check ran - benthicinfauna_metadata - coresizedepth_cm") # tested

    # Check: coresizediameter_cm corresponds to sample location == intertidal
    args = {
        "dataframe": benthicmeta,
        "tablename": 'tbl_benthicinfauna_metadata',
        "badrows": benthicmeta[(benthicmeta['samplelocation'] == 'intertidal') & (benthicmeta['coresizediameter_cm'] != 10)].index.to_list(),
        "badcolumn": "coresizediameter_cm",
        "error_type": "Unexpected Value",
        "is_core_error": False,
        "error_message": "The value for coresizediameter_cm where samplelocation recorded as 'intertidal' is expected to be 10."
    }
    warnings = [*warnings, checkData(**args)]
    print("check ran - benthicinfauna_metadata - coresizediamter_cm") # tested

    # Check: coresizedepth_cm corresponds to sample location == intertidal
    args = {
        "dataframe": benthicmeta,
        "tablename": 'tbl_benthicinfauna_metadata',
        "badrows": benthicmeta[(benthicmeta['samplelocation'] == 'intertidal') & (benthicmeta['coresizedepth_cm'] != 2)].index.to_list(),
        "badcolumn": "coresizedepth_cm",
        "error_type": "Unexpected Value",
        "is_core_error": False,
        "error_message": "The value for coresizedepth_cm where samplelocation recorded as 'intertidal' is expected to be 2."
    }
    warnings = [*warnings, checkData(**args)]
    print("check ran - benthicinfauna_metadata - coresizedepth_cm") # tested
    

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]
    
    return {'errors': errs, 'warnings': warnings}

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
    
    benthiclabbatch['tmp_row'] = benthiclabbatch.index
    benthicabundance['tmp_row'] = benthicabundance.index
    benthicbiomass['tmp_row'] = benthicbiomass.index

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    '''
    args = {
        "dataframe": df,
        "tablename": tbl,
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    '''
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename": '',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # # Logic Checks
    # eng = g.eng
    # sql = eng.execute("SELECT * FROM tbl_benthicinfauna_metadata")
    # sql_df = DataFrame(sql.fetchall())
    # sql_df.columns = sql.keys()
    # protocolmeta = sql_df
    # del sql_df

    # Start Benthicinfauna LAB Logic Checks
    ############################## -- Start of LAB logic checks --####################################################################
    # NOTE the values in groupcols might be wrong 
    #check 1: Each labbatch data must correspond to metadata in database
    # print("Enter logic check 1: Each labbatch data must correspond to metadata in database")
    # groupcols = ["siteid, estuaryname, stationno, samplecollectiondate, matrix, samplelocation",]
    # args = {
    #     "dataframe": benthiclabbatch,
    #     "tablename": 'tbl_benthicinfauna_labbatch',
    #     "badrows": mismatch(benthiclabbatch, protocolmeta, groupcols),
    #     "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, matrix, samplelocation",
    #     "error_type": "Logic Error",
    #     "error_message": "Field submission for benthicinfauna labbatch data is missing. Please verify that the benthicinfauna field data has been previously submitted."
    # }
    # errs = [*errs, checkData(**args)]
    # print("check 1 ran - logic - Each labbatch data must correspond to metadata in database")
    
    #check 2: Each labbatch data must include corresponding abundance data within session submission
    print("begin check 2")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplelocation', 'fieldreplicate', 'projectid']
    args.update({
        "dataframe": benthiclabbatch,
        "tablename": "tbl_benthicinfauna_labbatch",
        "badrows": mismatch(benthiclabbatch, benthicabundance, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplelocation, fieldreplicate, projectid",
        "error_type": "Logic Error",
        "error_message": "Records in tbl_benthicinfauna_labbatch must have corresponding records in benthicabundance. Missing records in benthicabundance."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - missing benthicabundance records")
    print("end check 2")

    #check 3: Each abundance data must include corresponding labbatch data within session submission
    print("begin check 3")
    groupcols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplelocation', 'scientificname', 'fieldreplicate', 'projectid']
    args.update({
        "dataframe": benthicabundance,
        "tablename": "tbl_benthicinfauna_abundance",
        "badrows": mismatch(benthicabundance, benthiclabbatch, groupcols), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplelocation, scientificname, fieldreplicate, projectid",
        "error_type": "Logic Error",
        "error_message": "Records in benthicabundance must have corresponding records in benthiclabbatch. Missing records in benthiclabbatch."
    })
    errs = [*errs, checkData(**args)]
    print("check 3 ran - logic - missing benthiclabbatch records")
    print("end check 3")

    #check 4: If there are values in biomass, they must correspond with metadata in database

    # End Benthicinfauna Logic Checks
    ############################## -- End of logic checks --####################################################################
    
    
    #NOTE THIS SHOULD BE A CORE CHECK
    # Check: preparationtime format validation
    # timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    # '''
    # badrows_preptime = benthiclabbatch[
    #     benthiclabbatch['preparationtime'].apply(
    #         lambda x: not bool(re.match(timeregex, ":".join([i for i in x.split(":")[:-1]]) )) 
    #         if str(x) != 'Not recorded' 
    #         else False
    #     )
    # ].index.tolist()
    # '''
    # # This badrows_preptime is working.
    # badrows_preptime = benthiclabbatch[
    #     benthiclabbatch['preparationtime'].apply(
    #         lambda x: not bool(re.match(timeregex, str(x))) if ((str(x) != 'Not Recorded') or (str(x) != '') or (not pd.isnull(x))) else False)
    #         ].index.tolist()
    # args.update({
    #     "dataframe": benthiclabbatch,
    #     "tablename": "tbl_benthicinfauna_labbatch",
    #     "badrows": badrows_preptime,
    #     "badcolumn": "preparationtime",
    #     "error_type" : "Time Format Error",
    #     "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    # })
    # errs = [*errs, checkData(**args)]
    # print("check ran - benthicinfauna_labbatch - preparationtime") # tested

    ###############################################################################################################
    #BIOMASS CUSTOM CHECKS START HERE
    print("Begining Benthic Infauna Custom checks for Biomass and Abundance")

    #check 5: Biomass_g must be greater than or equal to 0
    print('Begin custom check 5: Biomass_g must be greater than or equal to 0 ')
    args.update({
        "dataframe": benthicbiomass,
        "tablename": "tbl_benthicinfauna_biomass",
        "badrows":benthicbiomass[(benthicbiomass['biomass_g'] < 0)].tmp_row.tolist(),
        "badcolumn": "biomass_g",
        "error_type" : "Value is out of range.",
        "error_message" : "Biomass must be greater than 0"
    })
    errs = [*errs, checkData(**args)]
    print('End Custom check 5: Biomass_g must be greater than or equal to 0')

    #check 7: Scientificname/commoname pair for species must match lookup
    print('Begin custom check 7: Scientificname/commoname pair for species must match lookup --benthicabundance ')

    def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        
        for c in check_cols:
            df_to_check[c] = df_to_check[c].apply(lambda x: str(x).lower().strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())

        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)

    lookup_sql = f"SELECT * FROM lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(benthicabundance, lu_species, check_cols, lookup_cols)
    
    # Check: multicolumn lookup for species - benthicabundance
    args.update({
        "dataframe": benthicabundance,
        "tablename": "tbl_benthicinfauna_abundance",
        "badrows": badrows,
        "badcolumn":"commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>.'
    })
    errs = [*errs, checkData(**args)]
    print("check 7 ran - benthicinfauna_abundance - multicolumn lookup for species") 

    badrows = multicol_lookup_check(benthicbiomass, lu_species, check_cols, lookup_cols) #tested
    
    #check 6: Scientificname/commoname pair for species must match lookup
    # Check: multicolumn lookup for species - benthicbiomass
    print('Begin custom check 6: Scientificname/commoname pair for species must match lookup --benthicbiomass ')
    args.update({
        "dataframe": benthicbiomass,
        "tablename": "tbl_benthicinfauna_biomass",
        "badrows": badrows,
        "badcolumn":"commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>.'
    })
    errs = [*errs, checkData(**args)]
    print("check 6 ran - benthicinfauna_biomass - multicolumn lookup for species") #tested

    return {'errors': errs, 'warnings': warnings}