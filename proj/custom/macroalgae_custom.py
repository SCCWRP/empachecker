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

    errs = []
    warnings = []

    site_metadata = all_dfs['tbl_macroalgae_site_meta']
    transect_metadata = all_dfs['tbl_macroalgae_transect_meta']
    transect_cover = all_dfs['tbl_floating_data']
    floating = all_dfs['tbl_floating_data']

    site_metadata['tmp_row'] = site_metadata.index
    transect_metadata['tmp_row'] = transect_metadata.index
    transect_cover['tmp_row'] = transect_cover.index
    floating['tmp_row'] = floating.index

    site_metadata_pkey = get_primary_key('tbl_macroalgae_site_meta', g.eng)
    transect_metadata_pkey = get_primary_key('tbl_macroalgae_transect_meta', g.eng)
    transect_cover_pkey = get_primary_key('tbl_floating_data', g.eng)
    floating_pkey = get_primary_key('tbl_floating_data', g.eng)

    sitemetadata_transectmeta_shared_pkey = [x for x in site_metadata_pkey if x in transect_metadata_pkey]
    transectmeta_transectcover_shared_pkey = [x for x in transect_metadata_pkey if x in transect_cover_pkey]
    transectcover_floating_shared_pkey = [x for x in transect_cover_pkey if x in floating_pkey]




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

    # CHECK - 1
    print("# CHECK - 1")
    args.update({
        "dataframe": site_metadata[site_metadata['transectmethod_surveyed'].str.lower() == 'yes'],
        "tablename": "tbl_macroalgae_site_meta",
        "badrows": mismatch(
            site_metadata[site_metadata['transectmethod_surveyed'].str.lower() == 'yes'],
            transect_metadata,
            sitemetadata_transectmeta_shared_pkey
        ),
        "badcolumn": ','.join(sitemetadata_transectmeta_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in macroalgae_site_metadata with transectmethod_surveyed = 'yes' must include a corresponding record in macroalgae_transect_meta."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 1")

    # CHECK - 2
    print("# CHECK - 2")
    args.update({
        "dataframe": site_metadata[site_metadata['floatingmethod_surveyed'].str.lower() == 'yes'],
        "tablename": "tbl_macroalgae_site_meta",
        "badrows": mismatch(
            site_metadata[site_metadata['floatingmethod_surveyed'].str.lower() == 'yes'],
            floating,
            transectcover_floating_shared_pkey
        ),
        "badcolumn": ','.join(transectcover_floating_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in macroalgae_site_metadata with floatingmethod_surveyed = 'yes' must include a corresponding record in macroalgae_floating."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")

    # CHECK - 3
    print("# CHECK - 3")
    args.update({
        "dataframe": transect_cover,
        "tablename": "tbl_floating_data",
        "badrows": mismatch(transect_cover, site_metadata, sitemetadata_transectmeta_shared_pkey),
        "badcolumn": ','.join(sitemetadata_transectmeta_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in macroalgae_transect_cover must include a corresponding record in macroalgae_site_meta."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")

    # CHECK - 4
    print("# CHECK - 4")
    args.update({
        "dataframe": transect_cover,
        "tablename": "tbl_floating_data",
        "badrows": mismatch(transect_cover, transect_metadata, transectmeta_transectcover_shared_pkey),
        "badcolumn": ','.join(transectmeta_transectcover_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in macroalgae_transect_cover must include a corresponding record in macroalgae_transect_meta."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 4")

    # CHECK - 5
    print("# CHECK - 5")
    args.update({
        "dataframe": floating,
        "tablename": "tbl_floating_data",
        "badrows": mismatch(floating, site_metadata, sitemetadata_transectmeta_shared_pkey),
        "badcolumn": ','.join(sitemetadata_transectmeta_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in macroalgae_floating must include a corresponding record in macroalgae_site_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 5")

    # CHECK - 6
    print("# CHECK - 6")
    transect_cover_merged = pd.merge(
        transect_metadata[['plot_replicate', 'total_algae_cover', 'tmp_row']],
        transect_cover[transect_cover['covertype'].str.lower() == 'algae'][['plot_replicate', 'estimatedcover', 'tmp_row']],
        on='plot_replicate',
        how='left'
    )
    check_6_bad_rows = transect_cover_merged[
        transect_cover_merged['total_algae_cover'] != transect_cover_merged['estimatedcover']
    ]['tmp_row_x'].tolist()

    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": check_6_bad_rows,
        "badcolumn": "plot_replicate,total_algae_cover,estimatedcover",
        "error_type": "Logic Error",
        "error_message": "For each plot replicate in transect_meta, total_algae_cover must equal estimatedcover when covertype = algae in transect_cover."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 6")

    # CHECK - 7
    print("# CHECK - 7")
    check_7_bad_rows = []
    for _, group in transect_metadata.iterrows():
        if group['non_algae_cover'] == 100:
            corresponding_records = transect_cover[
                (transect_cover['plot_replicate'] == group['plot_replicate']) &
                (transect_cover['covertype'].str.lower() == 'algae') &
                (transect_cover['estimatedcover'] == 0)
            ]
            if corresponding_records.empty:
                check_7_bad_rows.append(group['tmp_row'])

    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": check_7_bad_rows,
        "badcolumn": "plot_replicate,non_algae_cover,covertype,estimatedcover",
        "error_type": "Logic Error",
        "error_message": "For each plot replicate in transect_meta with non_algae_cover = 100, there must be one record with covertype = algae and estimatedcover = 0 in transect_cover."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 7")

    # CHECK - 8
    print("# CHECK - 8")
    args.update({
        "dataframe": site_metadata,
        "tablename": "tbl_macroalgae_site_meta",
        "badrows": site_metadata[
            (site_metadata['waterclarity_length_cm'] < 0) | 
            (site_metadata['waterclarity_length_cm'] > 300)
        ]['tmp_row'].tolist(),
        "badcolumn": "waterclarity_length_cm",
        "error_type": "Range Error",
        "error_message": "waterclarity_length_cm must be between 0 and 300."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 8")



    # CHECK - 9
    print("# CHECK - 9")
    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": transect_metadata[transect_metadata['transectreplicate'] <= 0]['tmp_row'].tolist(),
        "badcolumn": "transectreplicate",
        "error_type": "Value Error",
        "error_message": "transectreplicate must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 9")

    # CHECK - 10
    print("# CHECK - 10")
    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": transect_metadata[transect_metadata['transectlength_m'] <= 0]['tmp_row'].tolist(),
        "badcolumn": "transectlength_m",
        "error_type": "Value Error",
        "error_message": "transectlength_m must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 10")

    # CHECK - 11
    print("# CHECK - 11")
    transect_metadata_sorted = transect_metadata.sort_values(
        by=['projectid', 'siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate']
    )
    transect_metadata_sorted['expected_replicate'] = transect_metadata_sorted.groupby(
        ['projectid', 'siteid', 'estuaryname', 'stationno', 'samplecollectiondate']
    )['transectreplicate'].rank().astype(int)

    badrows_11 = transect_metadata_sorted[
        transect_metadata_sorted['transectreplicate'] != transect_metadata_sorted['expected_replicate']
    ]['tmp_row'].tolist()

    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": badrows_11,
        "badcolumn": "transectreplicate",
        "error_type": "Logic Error",
        "error_message": "transectreplicate must be consecutive within primary keys (projectid, siteid, estuaryname, stationno, samplecollectiondate)."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 11")

    # CHECK - 12
    print("# CHECK - 12")
    transect_metadata_sorted['expected_plotreplicate'] = transect_metadata_sorted.groupby(
        ['projectid', 'siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate']
    )['plotreplicate'].rank().astype(int)

    badrows_12 = transect_metadata_sorted[
        transect_metadata_sorted['plotreplicate'] != transect_metadata_sorted['expected_plotreplicate']
    ]['tmp_row'].tolist()

    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": badrows_12,
        "badcolumn": "plotreplicate",
        "error_type": "Logic Error",
        "error_message": "plotreplicate must be consecutive within 'projectid', 'siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate'."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 12")

    # CHECK - 13
    print("# CHECK - 13")
    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": transect_metadata[transect_metadata['plotreplicate'] <= 0]['tmp_row'].tolist(),
        "badcolumn": "plotreplicate",
        "error_type": "Value Error",
        "error_message": "plotreplicate must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 13")

    # CHECK - 14
    print("# CHECK - 14")
    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": transect_metadata[
            (transect_metadata['total_algae_cover'] + transect_metadata['non_algae_cover']) != 100
        ]['tmp_row'].tolist(),
        "badcolumn": "total_algae_cover,non_algae_cover",
        "error_type": "Logic Error",
        "error_message": "For every plot, total_algae_cover + non_algae_cover must equal 100."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 14")


    # CHECK - 15
    print("# CHECK - 15")
    args.update({
        "dataframe": transect_cover,
        "tablename": "tbl_floating_data",
        "badrows": transect_cover[
            (transect_cover['estimatedcover'] == 0) & (transect_cover['scientificname'].str.lower() != "not recorded")
        ]['tmp_row'].tolist(),
        "badcolumn": "estimatedcover,scientificname",
        "error_type": "Logic Error",
        "error_message": "If estimatedcover is 0, then scientificname must be 'Not recorded'."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 15")

    # CHECK - 16
    print("# CHECK - 16")
    args.update({
        "dataframe": transect_cover,
        "tablename": "tbl_floating_data",
        "badrows": transect_cover[
            (transect_cover['covertype'].str.lower() == "plant") & (transect_cover['scientificname'].str.lower() == "not recorded")
        ]['tmp_row'].tolist(),
        "badcolumn": "covertype,scientificname",
        "error_type": "Logic Error",
        "error_message": "If covertype is 'plant', then scientificname cannot be 'Not recorded'."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 16")




    ########################################################### LEGACY CHECKS ###########################################################
    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------ Macroalgae Logic Checks ----------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################

    # print("# CHECK - 1")
    # # Description: Each metadata must include a corresponding coverdata
    # # Created Coder: Ayah
    # # Created Date:09/15/2023
    # # Last Edited Date: 
    # # Last Edited Coder: 
    # # NOTE (09/15/2023): Ayah wrote check
    # args.update({
    #     "dataframe": algaemeta,
    #     "tablename": "tbl_macroalgae_sample_metadata",
    #     "badrows": mismatch(algaemeta, algaecover, algaemeta_algaecover_shared_pkey), 
    #     "badcolumn":  ','.join(algaemeta_algaecover_shared_pkey),
    #     "error_type": "Logic Error",
    #     "error_message": "Each record in macroalgae_sample_metadata must include a corresponding record in algaecover_data. "+\
    #         "Records are matched based on these columns: {}".format(
    #         ', '.join(algaemeta_algaecover_shared_pkey)
    #     )
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 1")

    # print("# CHECK - 2")
    # # Description: Each cover data must include a corresponding metadata
    # # Created Coder:
    # # Created Coder: Ayah
    # # Created Date:09/15/2023
    # # Last Edited Date: 
    # # Last Edited Coder: 
    # # NOTE (09/15/2023): Ayah wrote check
    # args.update({
    #     "dataframe": algaecover,
    #     "tablename": "tbl_algaecover_data",
    #     "badrows": mismatch(algaecover, algaemeta, algaemeta_algaecover_shared_pkey), 
    #     "badcolumn":  ','.join(algaemeta_algaecover_shared_pkey),
    #     "error_type": "Logic Error",
    #     "error_message": "Each record in algaecover_data must include a corresponding record in macroalgae_sample_metadata. "+\
    #         "Records are matched based on these columns: {}".format(
    #         ', '.join(algaemeta_algaecover_shared_pkey)
    #     )
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 2")

    # print("# CHECK - 3")
    # # Description: Each metadata must include a corresponding floating data
    # # Created Coder:
    # # Created Coder: Ayah
    # # Created Date:09/15/2023
    # # Last Edited Date: 
    # # Last Edited Coder: 
    # # NOTE (09/15/2023): Ayah wrote check
    # args.update({
    #     "dataframe": algaemeta,
    #     "tablename": "tbl_macroalgae_sample_metadata",
    #     "badrows": mismatch(algaemeta, algaefloating, algaemeta_algaefloating_shared_pkey), 
    #     "badcolumn":  ','.join(algaemeta_algaefloating_shared_pkey),
    #     "error_type": "Logic Error",
    #     "error_message": "Each record in macroalgae_sample_metadata must include a corresponding record in floating_data. "+\
    #         "Records are matched based on these columns: {}".format(
    #         ', '.join(algaemeta_algaefloating_shared_pkey)
    #     )
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 3")

    # print("# CHECK - 4")
    # # Description: Each floating data must include a corresponding metadata
    # # Created Coder: Ayah
    # # Created Date:09/15/2023
    # # Last Edited Date: 
    # # Last Edited Coder: 
    # # NOTE (09/15/2023): Ayah wrote check
    # args.update({
    #     "dataframe": algaefloating,
    #     "tablename": "tbl_floating_data",
    #     "badrows": mismatch(algaefloating, algaemeta, algaemeta_algaefloating_shared_pkey), 
    #     "badcolumn":  ','.join(algaemeta_algaefloating_shared_pkey),
    #     "error_type": "Logic Error",
    #     "error_message": "Each record in floating_data must include a corresponding record in macroalgae_sample_metadata "+\
    #         "Records are matched based on these columns: {}".format(
    #         ', '.join(algaemeta_algaefloating_shared_pkey)
    #     )
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 4")




    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------END OF Macroalgae Logic Checks ----------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################








    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------ Sample Metadata Checks ------------------------------------------ #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################

    # print("# CHECK - 5")
    # # Description: Transectreplicate must be greater than 0
    # # Created Coder: Ayah
    # # Created Date:09/15/2023
    # # Last Edited Date: 
    # # Last Edited Coder: 
    # # NOTE (09/15/2023): Ayah wrote check
    # args.update({
    #     "dataframe": algaemeta,
    #     "tablename": "tbl_macroalgae_sample_metadata",
    #     "badrows": algaemeta[(algaemeta['transectreplicate'] <= 0) & (algaemeta['transectreplicate'] != -88)].tmp_row.tolist(),
    #     "badcolumn": "transectreplicate",
    #     "error_type" : "Value Error",
    #     "error_message" : "TransectReplicate must be greater than 0."
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 5")

    # print("# CHECK - 6")
    # # Description: Transectlength_m must be greater than 0
    # # Created Coder: Ayah
    # # Created Date:09/15/2023
    # # Last Edited Date: 
    # # Last Edited Coder: 
    # # NOTE (09/15/2023): Ayah wrote check
    # args.update({
    #     "dataframe": algaemeta,
    #     "tablename": "tbl_macroalgae_sample_metadata",
    #     "badrows": algaemeta[algaemeta['transect_length_m'] <= 0].tmp_row.tolist(),
    #     "badcolumn": "transect_length_m",
    #     "error_type" : "Value out of range",
    #     "error_message" : "Transect length must be greater than 0."
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 6")

    # print("# CHECK - 7")
    # # Description: Transectreplicate must be consecutive within primary keys
    # # Created Coder: Aria Askaryar
    # # Created Date: 09/28/2023
    # # Last Edited Date:  09/28/2023
    # # Last Edited Coder: Aria Askaryar
    # # NOTE ( 09/28/2023): Aria wrote the check, it has not been tested yet
    # groupby_cols = [x for x in algaemeta_pkey if x != 'transectreplicate']
    # args.update({
    #     "dataframe": algaemeta,
    #     "tablename": "tbl_macroalgae_sample_metadata",
    #     "badrows" : check_consecutiveness(algaemeta, groupby_cols, 'transectreplicate'),
    #     "badcolumn": "transectreplicate",
    #     "error_type": "Replicate Error",
    #     "error_message": f"transectreplicate values must be consecutive. Records are grouped by {','.join(groupby_cols)}"
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 7")


    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------END OF  Sample Metadata Checks ----------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################




    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------ Cover Data Checks ----------------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################

    # #print("# CHECK - 8")
    # # Description: Transectreplicate must be greater than 0
    # # Created Coder: NA
    # # Created Date:NA
    # # Last Edited Date: 09/15/2023
    # # Last Edited Coder: Ayah
    # # NOTE (09/15/2023): Ayah ajusted code to match coding standard
    # args.update({
    #     "dataframe": algaecover,
    #     "tablename": "tbl_algaecover_data",
    #     "badrows": algaecover[(algaecover['transectreplicate'] <= 0) & (algaecover['transectreplicate'] != -88)].tmp_row.tolist(),
    #     "badcolumn": "transectreplicate",
    #     "error_type" : "Value Error",
    #     "error_message" : "TransectReplicate must be greater than 0."
    # })
    # errs = [*errs, checkData(**args)]
    # #print("# END OF CHECK - 8")

    # #print("# CHECK - 9")
    # # Description: Plotreplicate must be greater than 0
    # # Created Coder: NA
    # # Created Date:NA
    # # Last Edited Date: 09/15/2023
    # # Last Edited Coder: Ayah
    # # NOTE (09/15/2023): Ayah ajusted code to match coding standard
    # args.update({
    #     "dataframe": algaecover,
    #     "tablename": "tbl_algaecover_data",
    #     "badrows": algaecover[(algaecover['plotreplicate'] <= 0) & (algaecover['plotreplicate'] != -88)].tmp_row.tolist(),
    #     "badcolumn": "plotreplicate",
    #     "error_type" : "Value Error",
    #     "error_message" : "PlotReplicate must be greater than 0."
    # })
    # errs = [*errs, checkData(**args)]
    
    
    # print("# END OF CHECK - 9")

    # print("# CHECK - 10")
    # # Description: If covertype is "plant" then scientificname cannot be "Not recorded"
    # # Created Coder: NA
    # # Created Date:NA
    # # Last Edited Date: 09/15/2023
    # # Last Edited Coder: Ayah
    # # NOTE (09/15/2023): Ayah ajusted code to match coding standard
    # args.update({
    #     "dataframe": algaecover,
    #     "tablename": "tbl_algaecover_data",
    #     "badrows": algaecover[(algaecover['covertype'] == 'plant') & (algaecover['scientificname'] == 'Not recorded')].tmp_row.tolist(), 
    #     "badcolumn": "covertype, scientificname",
    #     "error_type": "Value Error",
    #     "error_message": "CoverType is 'plant' so the ScientificName must be a value other than 'Not recorded'."
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 10")

    # print("# CHECK - 11")
    # # Description: Plotreplicate must be consecutive within primary keys
    # # Created Coder: Aria Askaryar
    # # Created Date: 09/28/2023
    # # Last Edited Date:  09/28/2023
    # # Last Edited Coder: Aria Askaryar
    # # NOTE ( 09/28/2023): Aria wrote the check, it has not been tested yet
    # # NOTE ( 02/13/2024): Robert updated grouping columns to:
    # #                    'projectid','siteid','estuaryname','stationno','samplecollectiondate','transectreplicate'
    # #                    per Jan's instruction
    
    # groupby_cols = ['projectid','siteid','estuaryname','stationno','samplecollectiondate','transectreplicate']
    # args.update({
    #     "dataframe": algaecover,
    #     "tablename": "tbl_algaecover_data",
    #     "badrows" : check_consecutiveness(algaecover, groupby_cols, 'plotreplicate'),
    #     "badcolumn": "plotreplicate",
    #     "error_type": "Replicate Error",
    #     "error_message": f"plotreplicate values must be consecutive. Records are grouped by {','.join(groupby_cols)}"
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 11")

    # print("# CHECK - 13")
    # # Description: For every plot – total cover and open cover required in cover type
    # # Created Coder: Duy
    # # Created Date: 10/05/23
    # # Last Edited Date: 
    # # Last Edited Coder: 
    # # NOTE (10/05/23): Duy created the check

    # badrows = list(
    #     itertools.chain.from_iterable(
    #         algaecover.groupby(
    #             ['projectid','siteid','estuaryname','stationno','samplecollectiondate','transectreplicate','plotreplicate']
    #         ).apply(
    #             lambda subdf: subdf.tmp_row.tolist() 
    #             if any(['open' not in subdf['covertype'].tolist(), 'total' not in subdf['covertype'].tolist()])
    #             else []
    #         )
    #     )
    # )
    # args.update({
    #     "dataframe": algaecover,
    #     "tablename": "tbl_algaecover_data",
    #     "badrows" : badrows,
    #     "badcolumn": "covertype",
    #     "error_type": "Value Error",
    #     "error_message": f"For every plotreplicate - total cover and open cover are required in covertype column"
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 13")


    # print("# CHECK - 14")
    # # Description: estimatedcover for 'open cover' in covertype + estimatedcover for 'total cover' must be 100
    # # Created Coder: Duy
    # # Created Date: 10/05/23
    # # Last Edited Date: 
    # # Last Edited Coder: 
    # # NOTE (10/05/23): Duy created the check
    # # NOTE (2/13/24): Robert edited the two covertypes to be "open" and "total" rather than "open" and "cover"
    # #                 Jan told me in Feb 2024 that these were the two covertypes that needed to have the estimated cover add to 100 

    # badrows = list(
    #     itertools.chain.from_iterable(
    #         algaecover.groupby(
    #             ['projectid','siteid','estuaryname','stationno','samplecollectiondate','transectreplicate','plotreplicate']
    #         ).apply(
    #             lambda subdf: subdf.tmp_row.tolist() 
    #             if sum(subdf[subdf['covertype'].isin(['open','total'])]['estimatedcover']) != 100
    #             else []
    #         )
    #     )
    # )
    # args.update({
    #     "dataframe": algaecover,
    #     "tablename": "tbl_algaecover_data",
    #     "badrows" : badrows,
    #     "badcolumn": "covertype, estimatedcover",
    #     "error_type": "Value Error",
    #     "error_message": f"For every plotreplicate - total cover and open cover are required in covertype column, and estimatedcover for 'open' in covertype + estimatedcover for 'total' must be 100"
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 14")


    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------END OF Cover Data Checks ----------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################









    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------ Floating Data Checks -------------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################

    # print("# CHECK - 12")
    # # Description: If estimatedcover is 0 then scientificname must be "Not recorded"
    # # Created Coder: Caspian
    # # Created Date: 4/10/2023
    # # Last Edited Date: 
    # # Last Edited Coder: 
    # # NOTE (Date):
    # args.update({
    #     "dataframe": algaefloating,
    #     "tablename": "tbl_floating_data",
    #     "badrows": algaefloating[(algaefloating['estimatedcover'] == 0) & (algaefloating['scientificname'] != 'Not recorded')].tmp_row.tolist(), 
    #     "badcolumn": "estimatedcover, scientificname",
    #     "error_type": "Value Error",
    #     "error_message": "EstimatedCover is 0. The ScientificName MUST be 'Not recorded'."
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END OF CHECK - 12")

    # print("# CHECK - 15")
    # # Description: transectlocation needs to match with associated values in the metadata table for a station
    # # Created Coder: Ayah
    # # Created Date: 05/22/2024
    # # Last Edited Date: 
    # # Last Edited Coder: 
    # # NOTE (Date):

    # algaemeta["present"] = 'yes'
    # group_col = ['projectid','siteid','estuaryname','samplecollectiondate','stationno','transectlocation']

    # merge_df = pd.merge(algaemeta,algaefloating, on = group_col, how = "outer") 

    # badrows = merge_df[(merge_df.present.isnull())].tmp_row_y.tolist()
    # badrows

    # args.update({
    #     "dataframe": algaefloating,
    #     "tablename": "tbl_floating_data",
    #     "badrows": badrows, 
    #     "badcolumn": "transectlocation",
    #     "error_type": "Value Error",
    #     "error_message": "Transectlocation needs to match with associated values in the metadata table for a station."
    # })
    # errs = [*errs, checkData(**args)]
    # print("# END CHECK - 15")

    # ######################################################################################################################
    # # ------------------------------------------------------------------------------------------------------------------ #
    # # ------------------------------------------------ END OF Floating Data Checks ------------------------------------- #
    # # ------------------------------------------------------------------------------------------------------------------ #
    # ######################################################################################################################

    return {'errors': errs, 'warnings': warnings}
