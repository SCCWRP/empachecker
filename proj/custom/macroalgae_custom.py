# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch,get_primary_key, check_consecutiveness
import itertools


def macroalgae(all_dfs):
    
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

    algaemeta = all_dfs['tbl_macroalgae_sample_metadata']
    algaecover = all_dfs['tbl_algaecover_data']
    algaefloating = all_dfs['tbl_floating_data']

    algaemeta['tmp_row'] = algaemeta.index
    algaecover['tmp_row'] = algaecover.index
    algaefloating['tmp_row'] = algaefloating.index

    algaemeta_pkey = get_primary_key('tbl_macroalgae_sample_metadata',g.eng)
    algaecover_pkey = get_primary_key('tbl_algaecover_data',g.eng)
    algaefloating_pkey = get_primary_key('tbl_floating_data',g.eng)

    algaemeta_algaecover_shared_pkey = [x for x in algaemeta_pkey if x in algaecover_pkey]
    algaemeta_algaefloating_shared_pkey = [x for x in algaemeta_pkey if x in algaefloating_pkey]

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
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Macroalgae Logic Checks ----------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 1")
    # Description: Each metadata must include a corresponding coverdata
    # Created Coder: Ayah
    # Created Date:09/15/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/15/2023): Ayah wrote check
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": mismatch(algaemeta, algaecover, algaemeta_algaecover_shared_pkey), 
        "badcolumn":  ','.join(algaemeta_algaecover_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Records in Algae metadata should have corresponding records in AlgeaCover data in the database. Please submit the metadata for these records first based on these columns: {}".format(
            ', '.join(algaemeta_algaecover_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: Each cover data must include a corresponding metadata
    # Created Coder:
    # Created Coder: Ayah
    # Created Date:09/15/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/15/2023): Ayah wrote check
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": mismatch(algaecover, algaemeta, algaemeta_algaecover_shared_pkey), 
        "badcolumn":  ','.join(algaemeta_algaecover_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Records in AlgeaCover data  should have corresponding records in Algae metadata. Please submit the metadata for these records first based on these columns: {}".format(
            ', '.join(algaemeta_algaecover_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")

    print("# CHECK - 3")
    # Description: Each metadata must include a corresponding floating data
    # Created Coder:
    # Created Coder: Ayah
    # Created Date:09/15/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/15/2023): Ayah wrote check
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": mismatch(algaemeta, algaefloating, algaemeta_algaefloating_shared_pkey), 
        "badcolumn":  ','.join(algaemeta_algaefloating_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Records in Algae metadata should have corresponding records in Algae Floating data. Please submit the metadata for these records first based on these columns: {}".format(
            ', '.join(algaemeta_algaefloating_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")

    print("# CHECK - 4")
    # Description: Each floating data must include a corresponding metadata
    # Created Coder: Ayah
    # Created Date:09/15/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/15/2023): Ayah wrote check
    args.update({
        "dataframe": algaefloating,
        "tablename": "tbl_floating_data",
        "badrows": mismatch(algaefloating, algaemeta, algaemeta_algaefloating_shared_pkey), 
        "badcolumn":  ','.join(algaemeta_algaefloating_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Records in AlgaeFloating should have corresponding records in AlgeaCover data. Please submit the data for these records first based on these columns: {}".format(
            ', '.join(algaemeta_algaefloating_shared_pkey)
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 4")




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------END OF Macroalgae Logic Checks ----------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################








    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Sample Metadata Checks ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 5")
    # Description: Transectreplicate must be greater than 0
    # Created Coder: Ayah
    # Created Date:09/15/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/15/2023): Ayah wrote check
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": algaemeta[(algaemeta['transectreplicate'] <= 0) & (algaemeta['transectreplicate'] != -88)].tmp_row.tolist(),
        "badcolumn": "transectreplicate",
        "error_type" : "Value Error",
        "error_message" : "TransectReplicate must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    #print("# END OF CHECK - 5")

    print("# CHECK - 6")
    # Description: Transectlength_m must be greater than 0
    # Created Coder: Ayah
    # Created Date:09/15/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/15/2023): Ayah wrote check
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": algaemeta[algaemeta['transect_length_m'] <= 0].tmp_row.tolist(),
        "badcolumn": "transect_length_m",
        "error_type" : "Value out of range",
        "error_message" : "Transect length must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 6")

    print("# CHECK - 7")
    # Description: Transectreplicate must be consecutive within primary keys
    # Created Coder: Aria Askaryar
    # Created Date: 09/28/2023
    # Last Edited Date:  09/28/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE ( 09/28/2023): Aria wrote the check, it has not been tested yet
    
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows" : check_consecutiveness(algaemeta, [x for x in algaemeta_pkey if x != 'transectreplicate'], 'transectreplicate'),
        "badcolumn": "transectreplicate",
        "error_type": "Replicate Error",
        "error_message": f"transectreplicate values must be consecutive."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 7")


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------END OF  Sample Metadata Checks ----------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Cover Data Checks ----------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 8")
    # Description: Transectreplicate must be greater than 0
    # Created Coder: NA
    # Created Date:NA
    # Last Edited Date: 09/15/2023
    # Last Edited Coder: Ayah
    # NOTE (09/15/2023): Ayah ajusted code to match coding standard
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": algaecover[(algaecover['transectreplicate'] <= 0) & (algaecover['transectreplicate'] != -88)].tmp_row.tolist(),
        "badcolumn": "transectreplicate",
        "error_type" : "Value Error",
        "error_message" : "TransectReplicate must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    #print("# END OF CHECK - 8")

    #print("# CHECK - 9")
    # Description: Plotreplicate must be greater than 0
    # Created Coder: NA
    # Created Date:NA
    # Last Edited Date: 09/15/2023
    # Last Edited Coder: Ayah
    # NOTE (09/15/2023): Ayah ajusted code to match coding standard
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": algaecover[(algaecover['plotreplicate'] <= 0) & (algaecover['plotreplicate'] != -88)].tmp_row.tolist(),
        "badcolumn": "plotreplicate",
        "error_type" : "Value Error",
        "error_message" : "PlotReplicate must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    
    
    print("# END OF CHECK - 9")

    print("# CHECK - 10")
    # Description: If covertype is "plant" then scientificname cannot be "Not recorded"
    # Created Coder: NA
    # Created Date:NA
    # Last Edited Date: 09/15/2023
    # Last Edited Coder: Ayah
    # NOTE (09/15/2023): Ayah ajusted code to match coding standard
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": algaecover[(algaecover['covertype'] == 'plant') & (algaecover['scientificname'] == 'Not recorded')].tmp_row.tolist(), 
        "badcolumn": "covertype, scientificname",
        "error_type": "Value Error",
        "error_message": "CoverType is 'plant' so the ScientificName must be a value other than 'Not recorded'."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 10")

    print("# CHECK - 11")
    # Description: Plotreplicate must be consecutive within primary keys
    # Created Coder: Aria Askaryar
    # Created Date: 09/28/2023
    # Last Edited Date:  09/28/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE ( 09/28/2023): Aria wrote the check, it has not been tested yet
    groupby_cols = ['projectid','siteid','estuaryname','stationno','samplecollectiondate','transectreplicate','covertype','scientificname','replicate']
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows" : check_consecutiveness(algaecover, groupby_cols, 'plotreplicate'),
        "badcolumn": "plotreplicate",
        "error_type": "Replicate Error",
        "error_message": f"plotreplicate values must be consecutive."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 11")

    print("# CHECK - 13")
    # Description: For every plot – total cover and open cover required in cover type
    # Created Coder: Duy
    # Created Date: 10/05/23
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (10/05/23): Duy created the check

    badrows = list(
        itertools.chain.from_iterable(
            algaecover.groupby(
                ['projectid','siteid','estuaryname','stationno','samplecollectiondate','transectreplicate','plotreplicate']
            ).apply(
                lambda subdf: subdf.tmp_row.tolist() 
                if any(['open' not in subdf['covertype'].tolist(), 'total' not in subdf['covertype'].tolist()])
                else []
            )
        )
    )
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows" : badrows,
        "badcolumn": "covertype",
        "error_type": "Value Error",
        "error_message": f"For every plotreplicate - total cover and open cover are required in covertype column"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 13")


    print("# CHECK - 14")
    # Description: estimatedcover for 'open cover' in covertype + estimatedcover for 'total cover' must be 100
    # Created Coder: Duy
    # Created Date: 10/05/23
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (10/05/23): Duy created the check

    badrows = list(
        itertools.chain.from_iterable(
            algaecover.groupby(
                ['projectid','siteid','estuaryname','stationno','samplecollectiondate','transectreplicate','plotreplicate']
            ).apply(
                lambda subdf: subdf.tmp_row.tolist() 
                if sum(subdf[subdf['covertype'].isin(['open','cover'])]['estimatedcover']) != 100
                else []
            )
        )
    )
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows" : badrows,
        "badcolumn": "covertype, estimatedcover",
        "error_type": "Value Error",
        "error_message": f"estimatedcover for 'open cover' in covertype + estimatedcover for 'total cover' must be 100"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 14")


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------END OF Cover Data Checks ----------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################









    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Floating Data Checks -------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 12")
    # Description: If estimatedcover is 0 then scientificname must be "Not recorded"
    # Created Coder:
    # Created Date:
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (Date):
    args.update({
        "dataframe": algaefloating,
        "tablename": "tbl_floating_data",
        "badrows": algaefloating[(algaefloating['estimatedcover'] == 0) & (algaefloating['scientificname'] != 'Not recorded')].tmp_row.tolist(), 
        "badcolumn": "estimatedcover, scientificname",
        "error_type": "Value Error",
        "error_message": "EstimatedCover is 0. The ScientificName MUST be 'Not recorded'."
    })
    errs = [*errs, checkData(**args)]
    #print("# END OF CHECK - 12")


    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Floating Data Checks ------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    return {'errors': errs, 'warnings': warnings}
