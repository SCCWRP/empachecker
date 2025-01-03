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
    transect_cover = all_dfs['tbl_macroalgae_transect_cover']
    floating = all_dfs['tbl_macroalgae_floating']

    site_metadata['tmp_row'] = site_metadata.index
    transect_metadata['tmp_row'] = transect_metadata.index
    transect_cover['tmp_row'] = transect_cover.index
    floating['tmp_row'] = floating.index

    site_metadata_pkey = get_primary_key('tbl_macroalgae_site_meta', g.eng)
    transect_metadata_pkey = get_primary_key('tbl_macroalgae_transect_meta', g.eng)
    transect_cover_pkey = get_primary_key('tbl_macroalgae_transect_cover', g.eng)
    floating_pkey = get_primary_key('tbl_macroalgae_floating', g.eng)

    sitemetadata_transectmeta_shared_pkey = [x for x in site_metadata_pkey if x in transect_metadata_pkey]
    sitemetadata_floating_shared_pkey = [x for x in site_metadata_pkey if x in floating_pkey]
    transectmeta_transectcover_shared_pkey = [x for x in transect_metadata_pkey if x in transect_cover_pkey]


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
    # Description: Each sample metadata must include corresponding abundance data (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

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
    # Description: Each sample metadata must include corresponding floating method data (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

    args.update({
        "dataframe": site_metadata[site_metadata['floatingmethod_surveyed'].str.lower() == 'yes'],
        "tablename": "tbl_macroalgae_site_meta",
        "badrows": mismatch(
            site_metadata[site_metadata['floatingmethod_surveyed'].str.lower() == 'yes'],
            floating,
            sitemetadata_floating_shared_pkey
        ),
        "badcolumn": ','.join(sitemetadata_floating_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in macroalgae_site_metadata with floatingmethod_surveyed = 'yes' must include a corresponding record in macroalgae_floating."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 2")

    # CHECK - 3
    print("# CHECK - 3")
    # Description: Each transect cover record must include a corresponding site metadata record (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

    args.update({
        "dataframe": transect_cover,
        "tablename": "tbl_macroalgae_transect_cover",
        "badrows": mismatch(transect_cover, site_metadata, sitemetadata_transectmeta_shared_pkey),
        "badcolumn": ','.join(sitemetadata_transectmeta_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in macroalgae_transect_cover must include a corresponding record in macroalgae_site_meta."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")

    # CHECK - 4
    print("# CHECK - 4")
    # Description: Each transect cover record must include a corresponding transect metadata record (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

    args.update({
        "dataframe": transect_cover,
        "tablename": "tbl_macroalgae_transect_cover",
        "badrows": mismatch(transect_cover, transect_metadata, transectmeta_transectcover_shared_pkey),
        "badcolumn": ','.join(transectmeta_transectcover_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in macroalgae_transect_cover must include a corresponding record in macroalgae_transect_meta."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 4")

    # CHECK - 5
    print("# CHECK - 5")
    # Description: Each floating data record must include a corresponding site metadata record (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

    args.update({
        "dataframe": floating,
        "tablename": "tbl_macroalgae_floating",
        "badrows": mismatch(floating, site_metadata, sitemetadata_transectmeta_shared_pkey),
        "badcolumn": ','.join(sitemetadata_transectmeta_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in macroalgae_floating must include a corresponding record in macroalgae_site_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 5")

    # CHECK - 6
    print("# CHECK - 6")
    # Description: For each plot replicate in transect_meta, if total_algae_cover > 0, 
    # there must be at least one record with covertype = 'algae', 
    # and total_algae_cover must equal the summed estimatedcover for covertype = 'algae' in transect_cover.
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: [Your Edit Date]
    # Last Edited Coder: [Your Name]

    pkey_cols = ['projectid', 'estuaryname', 'siteid', 'samplecollectiondate', 'stationno', 'transectreplicate', 'plotreplicate']

    # Extract relevant columns for metadata and cover
    metadata_cols = [*pkey_cols, 'total_algae_cover', 'tmp_row']
    cover_cols = [*pkey_cols, 'covertype', 'estimatedcover']

    # Filter transect_cover for algae and group by primary keys to sum estimatedcover
    transect_cover_aggregated = (
        transect_cover[transect_cover['covertype'].str.lower() == 'algae']
        .groupby(pkey_cols, as_index=False)
        .agg({'estimatedcover': 'sum'})
    )

    # Merge aggregated cover data with transect_metadata
    transect_cover_merged = pd.merge(
        transect_metadata[metadata_cols],
        transect_cover_aggregated,
        on=pkey_cols,
        how='left'
    )

    # Add a flag to check if covertype = 'algae' exists when total_algae_cover > 0
    transect_cover_merged['algae_record_exists'] = (
        transect_cover[pkey_cols + ['covertype']]
        .query("covertype.str.lower() == 'algae'", engine='python')
        .drop_duplicates(subset=pkey_cols)
        .assign(algae_record_exists=True)
        .set_index(pkey_cols)['algae_record_exists']
        .reindex(transect_metadata.set_index(pkey_cols).index, fill_value=False)
        .reset_index(drop=True)
    )

    # Identify bad rows
    check_6_bad_rows = transect_cover_merged[
        (
            (transect_cover_merged['total_algae_cover'] > 0) & 
            (~transect_cover_merged['algae_record_exists'])  # Missing algae record
        ) | (
            (transect_cover_merged['total_algae_cover'] != transect_cover_merged['estimatedcover']) & 
            (~transect_cover_merged['total_algae_cover'].isnull()) & 
            (~transect_cover_merged['estimatedcover'].isnull())  # Mismatch in total cover
        )
    ]['tmp_row'].tolist()

    # Update error log
    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": check_6_bad_rows,
        "badcolumn": "projectid,estuaryname,siteid,samplecollectiondate,stationno,transectreplicate,plotreplicate",
        "error_type": "Logic Error",
        "error_message": (
            "For each plot replicate in transect_meta, if total_algae_cover > 0, there must be at least one record "
            "with covertype = 'algae' in transect_cover. Additionally, total_algae_cover must equal the summed "
            "estimatedcover for all such records."
        )
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 6")


    # CHECK - 7
    print("# CHECK - 7")
    # Description: For each plot replicate in transect_meta with non_algae_cover = 100, there must be one record with covertype = algae and estimatedcover = 0 in transect_cover (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Revised to properly validate records across two connected tables.

    pkey_cols = ['projectid', 'estuaryname', 'siteid', 'samplecollectiondate', 'stationno', 'transectreplicate', 'plotreplicate']

    # Filter transect_metadata where non_algae_cover = 100
    metadata_filtered = transect_metadata[transect_metadata['non_algae_cover'] == 100][pkey_cols + ['tmp_row']]

    # Filter transect_cover for records with covertype = algae and estimatedcover = 0
    cover_filtered = transect_cover[
        (transect_cover['covertype'].str.lower() == 'algae') &
        (transect_cover['estimatedcover'] == 0)
    ][pkey_cols]

    # Perform a left join to find metadata rows with no matching cover rows
    metadata_with_cover_check = pd.merge(
        metadata_filtered,
        cover_filtered,
        on=pkey_cols,
        how='left',
        indicator=True  # Add merge indicator to detect unmatched rows
    )

    # Find rows in metadata where there is no match in transect_cover
    check_7_bad_rows = metadata_with_cover_check[
        metadata_with_cover_check['_merge'] == 'left_only'
    ]['tmp_row'].tolist()

    # Update the error log
    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": check_7_bad_rows,
        "badcolumn": "projectid, estuaryname, siteid, samplecollectiondate, stationno, transectreplicate, plotreplicate",
        "error_type": "Logic Error",
        "error_message": "For each plot replicate in transect_meta with non_algae_cover = 100, there must be one record with covertype = algae and estimatedcover = 0 in transect_cover."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 7")

    # CHECK - 8
    print("# CHECK - 8")
    # Description: waterclarity_length_cm must be between 0 and 300 (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

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
    # Description: transectreplicate must be greater than 0 (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

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
    # Description: transectlength_m must be greater than 0 (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": transect_metadata[transect_metadata['transect_length_m'] <= 0]['tmp_row'].tolist(),
        "badcolumn": "transect_length_m",
        "error_type": "Value Error",
        "error_message": "transectlength_m must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 10")


    # CHECK - 11
    print("# CHECK - 11")
    # Description: transectreplicate must be consecutive within primary keys (projectid, siteid, estuaryname, stationno, samplecollectiondate) (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.
    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": check_consecutiveness(transect_metadata, ['projectid','siteid','estuaryname','samplecollectiondate','stationno'], 'transectreplicate'), 
        "badcolumn": 'transectreplicate',
        "error_type": "Custom Error",
        "error_message": " transectreplicate must be consecutive within a projectid,siteid,estuaryname,samplecollectiondate,stationno group"
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 11")

    # CHECK - 12
    print("# CHECK - 12")
    # Description: plotreplicate must be consecutive within 'projectid', 'siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate' (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.
    args.update({
        "dataframe": transect_metadata,
        "tablename": "tbl_macroalgae_transect_meta",
        "badrows": check_consecutiveness(transect_metadata, ['projectid','siteid','estuaryname','samplecollectiondate','stationno','transectreplicate'], 'plotreplicate'), 
        "badcolumn": 'plotreplicate',
        "error_type": "Custom Error",
        "error_message": "plotreplicate must be consecutive within a projectid,siteid,estuaryname,samplecollectiondate,stationno,transectreplicate group"
    })
    errs = [*errs, checkData(**args)]   
    print("# END OF CHECK - 12")

    # CHECK - 13
    print("# CHECK - 13")
    # Description: plotreplicate must be greater than 0 (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

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
    # Description: For every plot, total_algae_cover + non_algae_cover must equal 100 (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

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
    # Description: If estimatedcover is 0, then scientificname must be 'Not recorded' (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

    args.update({
        "dataframe": transect_cover,
        "tablename": "tbl_macroalgae_transect_cover",
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
    # Description: If covertype is 'plant', then scientificname cannot be 'Not recorded' (🛑 ERROR 🛑)
    # Created Coder: Duy Nguyen
    # Created Date: 12/30/2024
    # Last Edited Date: 12/30/2024
    # Last Edited Coder: Duy Nguyen
    # NOTE (12/30/2024): Adjusted to follow the coding standard.

    args.update({
        "dataframe": transect_cover,
        "tablename": "tbl_macroalgae_transect_cover",
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
