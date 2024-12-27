# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, get_primary_key, mismatch
import pandas as pd

def benthiclarge(all_dfs):
    
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

    benthiclargemeta = all_dfs['tbl_benthiclarge_metadata']
    benthiclargeabundance= all_dfs['tbl_benthiclarge_abundance']

    benthiclargemeta['tmp_row'] = benthiclargemeta.index
    benthiclargeabundance['tmp_row'] = benthiclargeabundance.index

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
    # ------------------------------------------------ benthiclarge Logic Checks --------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    benthiclargemeta_pkey = get_primary_key('tbl_benthiclarge_metadata', g.eng)
    benthiclargeabundance_pkey = get_primary_key('tbl_benthiclarge_abundance', g.eng)

    benthiclargemeta_benthiclargeabundance_shared_pkey = [x for x in benthiclargemeta_pkey if x in benthiclargeabundance_pkey]

    print("# CHECK - 1")
    # Description: Each record in benthiclarge_metadata must include corresponding record in benthiclarge_abundance (🛑 ERROR 🛑)
    # Created Coder: Duy
    # Created Date: 9/28/2023
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (9/28/2023): Duy created the check, has not QA'ed yet. 
    # NOTE (10/05/2023): Aria revised the error message 
    args.update({
        "dataframe": benthiclargemeta,
        "tablename": "tbl_benthiclarge_metadata",
        "badrows": mismatch(benthiclargemeta, benthiclargeabundance, benthiclargemeta_benthiclargeabundance_shared_pkey), 
        "badcolumn": ','.join(benthiclargemeta_benthiclargeabundance_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in benthiclarge_metadata must include a corresponding record in benthiclarge_abundance. "+\
            "Records are matched based on these columns: {}".format(','.join(benthiclargemeta_benthiclargeabundance_shared_pkey))
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 1")

    print("# CHECK - 2")
    # Description: Each record in benthiclarge_abundance must include corresponding record in benthiclarge_metadata (🛑 ERROR 🛑)
    # Created Coder: Duy
    # Created Date: 9/28/2023
    # Last Edited Date: 10/05/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (9/28/2023): Duy created the check, has not QA'ed yet. 
    # NOTE (10/05/2023): Aria revised the error message 
    args.update({
        "dataframe": benthiclargeabundance,
        "tablename": "tbl_benthiclarge_abundance",
        "badrows": mismatch(benthiclargeabundance, benthiclargemeta, benthiclargemeta_benthiclargeabundance_shared_pkey), 
        "badcolumn": ','.join(benthiclargemeta_benthiclargeabundance_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in benthiclarge_abundance must include a corresponding record in benthiclarge_metadata. "+\
            "Records are matched based on these columns: {}".format(','.join(benthiclargemeta_benthiclargeabundance_shared_pkey))
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 2")
    
    print("# CHECK - 3")
    # Description: Each record in benthiclarge_abundance must include corresponding record in benthiclarge_length when
    # shell_type = whole, live_dead = live, and scientificname does not equal Nereididae (🛑 ERROR 🛑)
    # Created Coder: Duy
    # Created Date: 12/27/2024

    # Get primary keys for benthiclarge_length and benthiclarge_abundance
    benthiclarge_length = all_dfs['tbl_benthiclarge_length']
    benthiclarge_length['tmp_row'] = benthiclarge_length.index

    benthiclargeabundance_pkey = get_primary_key('tbl_benthiclarge_abundance', g.eng)
    benthiclargelength_pkey = get_primary_key('tbl_benthiclarge_length', g.eng)

    abundance_length_shared_pkey = [x for x in benthiclargeabundance_pkey if x in benthiclargelength_pkey]

    # Filter abundance records based on the conditions
    filtered_abundance = benthiclargeabundance[
        (benthiclargeabundance['shell_type'].str.lower() == 'whole') &
        (benthiclargeabundance['live_dead'].str.lower() == 'live') &
        (benthiclargeabundance['scientificname'].str.lower() != 'nereididae')
    ]

    # Perform the mismatch check
    args.update({
        "dataframe": filtered_abundance,
        "tablename": "tbl_benthiclarge_abundance",
        "badrows": mismatch(filtered_abundance, benthiclarge_length, abundance_length_shared_pkey),
        "badcolumn": ','.join(abundance_length_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in benthiclarge_abundance must include a corresponding record in benthiclarge_length "+
                        "when shell_type = whole, live_dead = live, and scientificname does not equal Nereididae. "+
                        "Records are matched based on these columns: {}".format(','.join(abundance_length_shared_pkey))
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 3")

    print("# CHECK - 4")
    # Description: Each record in benthiclarge_length must include corresponding record in benthiclarge_abundance (🛑 ERROR 🛑)
    # Created Coder: Duy
    # Created Date: 12/27/2024

    args.update({
        "dataframe": benthiclarge_length,
        "tablename": "tbl_benthiclarge_length",
        "badrows": mismatch(benthiclarge_length, benthiclargeabundance, abundance_length_shared_pkey),
        "badcolumn": ','.join(abundance_length_shared_pkey),
        "error_type": "Logic Error",
        "error_message": "Each record in benthiclarge_length must include a corresponding record in benthiclarge_abundance. "+
                        "Records are matched based on these columns: {}".format(','.join(abundance_length_shared_pkey))
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 4")

    
    print("# CHECK - 5")
    # Description: If scientificname contains the word 'unknown', then unknown_replicate must have a numeric value (🛑 ERROR 🛑)
    # Created Coder: Duy
    # Created Date: 12/27/2024

    # Filter rows where scientificname contains 'unknown' but unknown_replicate is not numeric
    check_5_bad_rows = benthiclargeabundance[
        benthiclargeabundance['scientificname'].str.contains('unknown', case=False, na=False) &
        ~benthiclargeabundance['unknown_replicate'].apply(lambda x: pd.to_numeric(x, errors='coerce')).notnull()
    ].tmp_row.tolist()

    args.update({
        "dataframe": benthiclargeabundance,
        "tablename": "tbl_benthiclarge_abundance",
        "badrows": check_5_bad_rows,
        "badcolumn": "scientificname,unknown_replicate",
        "error_type": "Logic Error",
        "error_message": "If scientificname contains the word 'unknown', then unknown_replicate must have a numeric value."
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 5")

    print("# CHECK - 6")
    # Description: Species_replicate should match total abundance for each unique record. If abundance > 10, then just need 10 records in length (🛑 ERROR 🛑)
    # Created Coder: Duy
    # Created Date: 12/27/2024

    # Define the unique key fields for grouping
    unique_key_fields = [
        'projectid', 'siteid', 'estuaryname', 'stationno',
        'samplecollectiondate', 'samplelocation', 'fieldreplicate',
        'substrate', 'scientificname', 'live_dead'
    ]

    # Group by the unique record definition and check conditions
    check_6_bad_rows = []
    for _, group in benthiclargeabundance.groupby(unique_key_fields):
        total_abundance = group['abundance'].sum()
        species_replicates = group['species_replicate'].count()

        # Condition 1: Species_replicate count must match abundance for abundance <= 10
        if total_abundance <= 10 and species_replicates != total_abundance:
            check_6_bad_rows.extend(group['tmp_row'].tolist())

        # Condition 2: Species_replicate count must be 10 for abundance > 10
        elif total_abundance > 10 and species_replicates != 10:
            check_6_bad_rows.extend(group['tmp_row'].tolist())

    args.update({
        "dataframe": benthiclargeabundance,
        "tablename": "tbl_benthiclarge_abundance",
        "badrows": check_6_bad_rows,
        "badcolumn": ','.join(unique_key_fields + ['species_replicate']),
        "error_type": "Logic Error",
        "error_message": "Species_replicate must match total abundance for each unique record. If abundance > 10, only 10 records in length are required. "+
                        "Unique records are defined by these fields: {}".format(', '.join(unique_key_fields))
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 6")




    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------end of benthiclarge Logic Checks --------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################



    return {'errors': errs, 'warnings': warnings}