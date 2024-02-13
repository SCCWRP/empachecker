from inspect import currentframe
from flask import current_app, g
from .functions import checkData,mismatch,get_primary_key,check_consecutiveness
import pandas as pd

def grab_field(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    lu_list_script_root = current_app.script_root

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

    grabeventdet = all_dfs['tbl_grabevent_details']
    grabevent = all_dfs['tbl_grabevent']

    grabeventdet['tmp_row'] = grabeventdet.index
    grabevent['tmp_row'] = grabevent.index 

    grabeventdet_pkey = get_primary_key('tbl_grabevent_details',g.eng)
    grabevent_pkey = get_primary_key('tbl_grabevent',g.eng)
    grabevent_grabeventdet_shared_pkey = [x for x in grabevent_pkey if x in grabeventdet_pkey]


    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    args = {
        "dataframe":pd.DataFrame({}),
        "tablename":'',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ Logic Checks ---------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    print("# CHECK - 1")
    # Description: Records in the grabevent should have the corresponding records in the grabeventdetails when the sampletype identifying column is yes (for example: toxicity column in grabevent)
    # Created Coder: Ayah
    # Created Date: NA
    # Last Edited Date: 09/25/2023
    # Last Edited Coder: Duy
    # NOTE (09/12/2023): Ayah wrote logic check. Since we are only checking records when any sampletype is yea, I created a filtered df containing all the rows where one or more sampletype identifier is "Yes"
    
    # NOTE (09/22/2023): Got a critical error submitting Prop50 data - "list index out of range" - Robert
    #                    Remember that we need a matching record such that when a sampletype column is "Yes" there has to be a matching record in 
    #                    Grab Event Details such that the sampletype is the same as the column name in Grab Event
    #                    For this reason, i'll need to adjust this
    
    # lowercase the sampletypes 
    # and put in alphabetical order, just because
    # NOTE (09/25/2023): Duy changed the code so it marks the badrows in grabevent, added the sampletype column to badcolumns, clarified the error message
    sampletypes = pd.read_sql("SELECT DISTINCT LOWER(sampletype) AS sampletype FROM lu_sampletype ORDER BY 1",g.eng).sampletype.tolist()

    # If there is a sampletype that is not in the grabevent dataframe columns, then we set something up incorrectly
    assert \
        set(sampletypes).issubset(set(grabevent.columns)), \
        f"Sampletypes {set(sampletypes) - set(grabevent.columns)} not found in columns of grabevent table"
    
    # Loop through the sampletypes and check data accordingly
    for sampletype in sampletypes:
        filtered_grabevent = grabevent[ grabevent[sampletype] == 'Yes' ]
        filtered_grabdetail = grabeventdet[grabeventdet['sampletype'] == sampletype]
        args.update({
            "dataframe":grabevent,
            "tablename":'tbl_grabevent',
            "badrows":mismatch(filtered_grabevent, filtered_grabdetail, grabevent_grabeventdet_shared_pkey),
            "badcolumn": ','.join([*grabevent_grabeventdet_shared_pkey, *[sampletype]]),
            "error_type": "Logic Error",
            "is_core_error": False,
            "error_message": 
                "If you indicate that you collect data for a datatype (for example: you input 'Yes' in nutrients column in grab_event), "+\
                "then you should have the corresponding records for that datatype in the grabevent_details based on these columns {}, "+\
                "and the value in sampletype column should match".format(','.join(grabevent_grabeventdet_shared_pkey))
        })
        errs = [*errs, checkData(**args)] 
    print("# END OF CHECK - 1")
    
    
    print("# CHECK - 2")
    # Description: Records in the grabevent_details should have the corresponding records in the grabevent based on the shared pkeys
    # Created Coder: Ayah
    # Created Date: NA
    # Last Edited Date: 09/14/2023
    # Last Edited Coder: Ayah
    # NOTE (09/12/2023): Ayah wrote logic check
    args.update({
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows":mismatch(grabevent,grabeventdet,grabevent_grabeventdet_shared_pkey),
        "badcolumn": ','.join(grabevent_grabeventdet_shared_pkey),
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "Records in the grabevent_details should have the corresponding records in the grabevent based on these columns {}".format(
            ','.join(grabevent_grabeventdet_shared_pkey))
    })
    errs = [*errs, checkData(**args)] 
    print("# END OF CHECK - 2")
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF Logic Checks --------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################






    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ GrabEvent Checks ------------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    #print("# CHECK - 12")
    # Description: At least one of the datatype identifiers (toxicity,grainsize,infauna,chemistry,nutrients,edna,microplastic) needed to be yes
    # Created Coder: Duy
    # Created Date: 10/02/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (10/02/2023): Duy created the check.
    badrows = grabevent[
        grabevent.apply(
            lambda row: all(
                [
                    row['infauna'].lower() == 'no',    
                    row['chemistry'].lower() == 'no',    
                    row['toxicity'].lower() == 'no',    
                    row['grainsize'].lower() == 'no',    
                    row['microplastics'].lower() == 'no',    
                    row['pfas'].lower() == 'no',    
                    row['pfasfieldblank'].lower() == 'no',    
                    row['microplasticsfieldblank'].lower() == 'no',    
                    row['equipmentblank'].lower() == 'no',    
                    row['nutrients'].lower() == 'no',    
                ]
            ),
            axis=1    
        )
    ].tmp_row.tolist()
    args.update({
        "dataframe": grabevent,
        "tablename":'tbl_grabevent',
        "badrows": badrows,
        "badcolumn": 'infauna,chemistry,toxicity,grainsize,microplastics,pfas,pfasfieldblank,microplasticsfieldblank,equipmentblank,nutrients',
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "At least one of the datatype identifiers (toxicity,grainsize,infauna,chemistry,nutrients,edna,microplastic) needed to be yes"
    })
    errs = [*errs, checkData(**args)] 



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------END OF GrabEvent Checks ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################






    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ GrabEventDetail Checks ------------------------------------------ #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# CHECK - 3")
    # Description: If matrix is 'sediment' AND collectionmethod is 'sediment core', then coresizediameter MUST be a numeric value (CANNOT BE -88) 
    # Created Coder: Ayah
    # Created Date: NA
    # Last Edited Date: 01/05/2024
    # Last Edited Coder: Zaib Quraishi
    # NOTE (09/12/2023): Ayah adjusted the format so it follows the coding standard
    # NOTE (09/25/2023): Aria adjusted error message and changed code since coresizediamerter is numeric so -88 is the numeric equivalent to "Not Recorded"
    # NOTE (09/26/2023): Aria changed the logic of this check (changed from Not recorded to -88 since coresizediameter is a numeric column).
    # NOTE (01/05/2024): Zaib updated description with specific criteria provided by Jan. QA done.

    args.update({
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows":grabeventdet[ 
            (grabeventdet['matrix'] == 'sediment') & 
            (grabeventdet['collectionmethod'] == 'sediment core') &
            (grabeventdet['coresizediameter'] == -88)
        ].tmp_row.tolist(),
        "badcolumn": "coresizediameter",
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "If matrix is sediment and collectionmethod is sediment core, then coresizediameter MUST be a numeric value (CANNOT be -88)."
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 3")

    print("# CHECK - 4")
    # Description: If matrix is "sediment" and collectionmethod is "sediment core", then coresizedepth MUST be a numeric value (CANNOT be -88)
    # Created Coder: Ayah 
    # Created Date: NA
    # Last Edited Date: 01/05/2024
    # Last Edited Coder: Zaib Quraishi
    # NOTE (09/12/2023): Ayah adjusted the format so it follows the coding standard
    # NOTE (09/25/2023): Aria adjusted error message and changed code since coresizedepth is numeric so -88 is the numeric equivalent to "Not Recorded"
    # NOTE (09/26/2023): Aria changed the logic of this check (changed from Not recorded to -88 since coresizediameter is a numeric column).
    # NOTE (01/05/2024): Zaib updated description and check based on specified criteria provided by Jan. QA done.

    args.update({
            "dataframe":grabeventdet,
            "tablename":'tbl_grabevent_details',
            "badrows":grabeventdet[
                (grabeventdet['matrix'] == 'sediment') &
                (grabeventdet['collectionmethod'] == 'sediment core') &
                (grabeventdet['coresizedepth'] == -88) 
            ].tmp_row.tolist(),
            "badcolumn": "coresizedepth",
            "error_type": "empty value",
            "is_core_error": False,
            "error_message": "If matrix is sediment and collectionmethod is sediment core, then coresizedepth MUST be a numeric value (CANNOT be -88)."
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 4")

    print("# CHECK - 5")
    # Description: If the sample type is benthic infauna and the matrix is sediment then sieve_or_depth is required 
    # Created Coder: Ayah 
    # Created Date: NA
    # Last Edited Date: 09/14/2023
    # Last Edited Coder: Ayah
    # NOTE (09/12/2023): Ayah adjusted the format so it follows the coding standard

    args.update({
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows": grabeventdet[
            (grabeventdet['sampletype'] == 'infauna') & 
            (grabeventdet['matrix'] == 'sediment') & 
            (grabeventdet['sieve_or_depth'].isna())
        ].tmp_row.tolist(),
        "badcolumn": "sieve_or_depth",
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "Sieve_or_Depth is a required field since sampletype is Benthic infauna and matrix is sediment. Please enter the sieve size if the sample was sieved in the field. If the sample was not sieved in the field, enter -88."
    })
    errs = [*errs, checkData(**args)]
    
    print("# END OF CHECK - 5")

    print("# CHECK - 6")
    # Description: sieve_or_depth is required when matrix is water
    # Created Coder: Ayah 
    # Created Date: NA
    # Last Edited Date: 2/8/2024
    # Last Edited Coder: Robert B
    # NOTE (09/12/2023): Ayah adjusted the format so it follows the coding standard
    # NOTE (09/14/2023): Ayah made the lu list for all water matrix for the check
    # NOTE (09/25/2023): Aria updated code to catch error(sieve_or_depth is numeric so it has to be -88 not "Not recorded") and updated error_message
    # NOTE (09/28/2023): Aria changed the logic of this check (changed from Not recorded to -88 since coresizediameter is a numeric column) and changed the logic from != to == -88.
    # NOTE (2/8/2024): Not having the check enforced for blankwater

    lu_matrix_filtered = pd.read_sql("SELECT matrix FROM lu_matrix where matrix like '%%water' AND matrix != 'blankwater' ;",g.eng)
    lu_matrix_filtered = lu_matrix_filtered['matrix'].tolist()

    args.update({
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows":grabeventdet[
            (grabeventdet['matrix'].isin(lu_matrix_filtered)) & 
            ((grabeventdet['sieve_or_depth'] == -88) | (grabeventdet['sieve_or_depth'].isna()) )
            ].tmp_row.tolist(),
        "badcolumn": "sieve_or_depth",
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "Sieve_or_Depth is a required field since matrix is water (except blankwater). Please enter the depth at which the sample was collected."
    })
    
    # Feb 13, 2024
    # Per Jan this check has been made a warning for the sake of loading historical data - temporarily
    warnings = [*warnings, checkData(**args)]
    print("# END OF CHECK - 6")

    print("# CHECK - 7")
    # Description: color should be 'Not recorded' in when matrix is water
    # Created Coder: Ayah 
    # Created Date: NA
    # Last Edited Date: 09/25/2023
    # Last Edited Coder: Aria Askaryar
    # NOTE (09/12/2023): Ayah adjusted the format so it follows the coding standard
    # NOTE (09/14/2023): Ayah made the lu list for all water matrix for the check
    # NOTE (09/25/2023): Aria updated code changed "Not Recored" to "Not recorded" and updated error_message

    args.update({
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows":grabeventdet[(grabeventdet['matrix'].isin(lu_matrix_filtered)) & 
            (grabeventdet['color'] != 'Not recorded')
            ].tmp_row.tolist(),
        "badcolumn": "color",
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "Color should be 'Not recorded' since matrix is water"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 7")


    print("# CHECK - 8")
    # Description: odor should be 'Not recorded' when matrix is water
    # Created Coder: Ayah 
    # Created Date: NA
    # Last Edited Date: 09/26/2023
    # Last Edited Coder: Duy
    # NOTE (09/12/2023): Ayah adjusted the format so it follows the coding standard
    # NOTE (09/14/2023): Ayah made the lu list for all water matrix for the check
    # NOTE (09/26/2023): This would give an error .isin([lu_matrix_filtered]) since lu_matrix_filtered is already a list. so Duy removed it.
    args.update({
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows":grabeventdet[
            (grabeventdet['matrix'].isin(lu_matrix_filtered)) & 
            (grabeventdet['odor'] != 'Not recorded')
        ].tmp_row.tolist(),
        "badcolumn": "odor",
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "Odor should be 'Not recorded' since matrix is water"
    }) 
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 8")

    print("# CHECK - 9")
    # Description: composition should not get filled in when matrix is water
    # Created Coder: Ayah 
    # Created Date: NA
    # Last Edited Date: 09/14/2023
    # Last Edited Coder: Ayah
    # NOTE (09/12/2023): Ayah adjusted the format so it follows the coding standard
    # NOTE (09/14/2023): Ayah made the lu list for all water matrix for the check
    args.update({
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows":grabeventdet[
            (grabeventdet['matrix'].isin(lu_matrix_filtered)) &
            (grabeventdet['composition'] != 'Not recorded' ) 
            ].tmp_row.tolist(),
        "badcolumn": "composition",
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "Composition should be 'Not recorded' since matrix is water"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 9")

    print("# CHECK - 10a")
    # Description: If sampletype is "infauna" then a value for sieve_or_depth field must be pulled from lu_benthicsievesize. 
    # Created Coder: Aria Askaryar
    # Created Date: 09/02/2023
    # Last Edited Date: 09/28/2023
    # Last Edited Coder: Aria Askaryar 
    # NOTE (09/12/2023): Ayah Adjusted format so it follows the coding standard
    # NOTE (09/22/2023): Adjusted the logic for obtaining the badrows - wrapped last two conditions in parentheses
    # NOTE (09/22/2023): Adjusted lookup list link - lu_list_script_root is not necessarily needed so long as there is no slash in front of scraper
    # NOTE (09/28/2023): Aria and Duy -Made severe changes to the check broke the check into two checks 10a and 10b. Fixed the logic, added tmp_grabeventdet and tmp_grabeventdet, and fixed the error when empty value cant be int

    lu_benthicsievesize = pd.read_sql("SELECT * FROM lu_benthicsievesize", g.eng).sievesize.tolist()
    lu_benthicsievesizeunits = pd.read_sql("SELECT * FROM lu_benthicsievesizeunits", g.eng).sievesizeunits.tolist()
    tmp_grabeventdet = grabeventdet[grabeventdet['sampletype'] == 'infauna']
    tmp_grabeventdet['sieve_or_depth'] = [int(x) if not pd.isna(x) else x for x in tmp_grabeventdet['sieve_or_depth']] 
    args.update({
        "dataframe": grabeventdet,
        "tablename": "tbl_grabevent_details",
        "badrows":grabeventdet[
            (grabeventdet['sampletype'] == 'infauna') &
            (
                (~grabeventdet['sieve_or_depth'].isin(lu_benthicsievesize)) | 
                (~grabeventdet['sieve_or_depthunits'].isin(lu_benthicsievesizeunits))
            )
        ].tmp_row.tolist(),
        "badcolumn": "sieve_or_depthunits,sieve_or_depth",
        "error_type": "mismatched value",
        "is_core_error": False,
        "error_message": "If sampletype is 'infauna' then a value for sieve_or_depth field must come from <a href='/checker/scraper?action=help&layer=lu_benthicsievesize' target='_blank'>lu_benthicsievesize</a>"  
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 10a")

    
    print("# CHECK - 10b")
    # Description: If sampletype is "infauna" then a value needs to be set for the sieve_or_depthunits field (lu_benthicsievesizeunits).
    # Created Coder: Aria Askaryar
    # Created Date: 09/28/2023
    # Last Edited Date: 09/28/2023
    # Last Edited Coder: Aria Askaryar 
    # NOTE (09/28/2023): Aria wrote check 

    args.update({
        "dataframe": grabeventdet,
        "tablename": "tbl_grabevent_details",
        "badrows": grabeventdet[(grabeventdet['sampletype'] == 'infauna') & (~grabeventdet['sieve_or_depthunits'].isin(lu_benthicsievesizeunits))].tmp_row.tolist(),
        "badcolumn": "sieve_or_depthunits",
        "error_type": "mismatched value",
        "is_core_error": False,
        "error_message": "If sampletype is 'infauna' then a sieve_or_depthunits must come from <a href='/checker/scraper?action=help&layer=lu_benthicsievesizeunits' target='_blank'>lu_benthicsievesizeunits.</a>"  
    })
    errs = [*errs, checkData(**args)]
    print("# END OF CHECK - 10b")

    print("# CHECK - 11")
    # Description: samplereplicate must be consecutive within primary keys
    # Created Coder: Duy Nguyen
    # Created Date: 09/26/2023
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (09/26/2023): Duy created the check
    groupby_cols = [x for x in grabevent_pkey if x not in ['latitude','longitude','samplereplicate']]
    args.update({
        "dataframe": grabeventdet,
        "tablename": "tbl_grabevent_details",
        "badrows" : check_consecutiveness(grabeventdet, groupby_cols, 'samplereplicate'),
        "badcolumn": "samplereplicate",
        "error_type": "Replicate Error",
        "error_message": f"samplereplicate values must be consecutive. Records are grouped by {','.join(groupby_cols)}"
    })
    errs = [*errs, checkData(**args)]

    print("# END OF CHECK - 11")



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------ END OF GrabEventDetail Checks ----------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################









    return {'errors': errs, 'warnings': warnings}
