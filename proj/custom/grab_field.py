# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData
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

    print("Begin Grab Field Custom Checks..")

   
#aria started here
    #check specific to tbl_grabevent_detail: If sampletype is "infauna" then a value for sieve_or_depth field must be pulled from lu_benthicsievesize. Also a value needs to be set for the sieve_or_depthunits field (lu_benthicsievesizeunits).
    print("begin check grabfield: ")
    lookup_sql = f"SELECT * FROM lu_benthicsievesize;"
    lu_benthicsievesize = pd.read_sql(lookup_sql, g.eng)
    sievesize_list = [int(x) for x in lu_benthicsievesize['sievesize']]
    badrows = grabeventdet[(grabeventdet['sampletype'] == 'infauna') & (grabeventdet['sieve_or_depth'].isin(sievesize_list))]
    print("before")
    print(f"sievesize_list: {sievesize_list}")
    print(f"badrows: {badrows}")

    args.update({
        "dataframe": grabeventdet,
        "tablename": "tbl_grabevent_details",
        "badrows":grabeventdet[(grabeventdet['sampletype'] == 'infauna') & (~grabeventdet['sieve_or_depth'].isin(sievesize_list))].tmp_row.tolist(),
        "badcolumn": "sieve_or_depth",
        "error_type": "mismatched value",
        "is_core_error": False,
        # "error_message": f" If sampletype is 'infauna' then a value for sieve_or_depth field must be from lu_benthicsievesize table."
        "error_message": f' If sampletype is "infauna" then a value for sieve_or_depth field must be from lu_benthicsievesize table.'
            '<a '
            f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_benthicsievesize" '
            'target="_blank">lu_benthicsievesize</a>'        
    })
    errs = [*errs, checkData(**args)]
    print("check ran- If sampletype is 'infauna' then a value for sieve_or_depth field must be pulled from lu_benthicsievesize.")

    ##check: If sampletype is "infauna" then a value for sieve_or_depthunits field must come from (lu_benthicsievesizeunits).
    print('begin check  grabfield')
    lookup_sql2 = f"SELECT * FROM lu_benthicsievesizeunits;"
    lu_benthicsievesizeunits = pd.read_sql(lookup_sql2, g.eng)
    sievesizeunits_list = lu_benthicsievesizeunits['sievesizeunits'].tolist()
    # badrows = grabeventdet[(grabeventdet['sampletype'] == 'infauna') & (grabeventdet['sieve_or_depthunits'].isin(sievesizeunits_list))]

    args.update({
        "dataframe": grabeventdet,
        "tablename": "tbl_grabevent_details",
        "badrows":grabeventdet[(grabeventdet['sampletype'] == 'infauna') & (~grabeventdet['sieve_or_depthunits'].isin(sievesizeunits_list))].tmp_row.tolist(),
        "badcolumn": "sieve_or_depthunits",
        "error_type": "mismatched value",
        "is_core_error": False,
        "error_message": f' If sampletype is "infauna" then a value for sieve_or_depthunits field must come from (lu_benthicsievesizeunits)'
            '<a '
            f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_benthicsievesizeunits" '
            'target="_blank">lu_benthicsievesizeunits</a>'        
    })
    errs = [*errs, checkData(**args)]
    print('check ran - If sampletype is "infauna" then a value for sieve_or_depthunits field must come from (lu_benthicsievesizeunits).')
#aria ended here


#Check: Coresizediameter should be filled when matrix is sediment
    print("begin check grabfield ")
    args.update({
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows":grabeventdet[ 
            (grabeventdet['matrix'].isin(['sediment','eDNA benthic core sediment','eDNA sediment'])) & 
            ( (grabeventdet['coresizediameter'] == -88) | (grabeventdet['coresizediameter'].isna()) )
        ].tmp_row.tolist(),
        "badcolumn": "coresizediameter",
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "Since Matrix is sediment related, CoreSizeDiameter cannot be null value or -88"
    })
    errs = [*errs, checkData(**args)]
    print('check ran - error check for matrix and coresizediameter')

#Check: Coresizedepth should be filled when matrix is sediment
    print('begin check grabfield')
    args = {
            "dataframe":grabeventdet,
            "tablename":'tbl_grabevent_details',
            "badrows":grabeventdet[(grabeventdet['matrix'].isin(['sediment','eDNA benthic core sediment','eDNA sediment'])) &
                ((grabeventdet['coresizedepth']== -88)| (grabeventdet['coresizedepth'].isna())) 
                ].tmp_row.tolist(),
            "badcolumn": "coresizedepth",
            "error_type": "empty value",
            "is_core_error": False,
            "error_message": "Since Matrix is sediment related, CoreSizeDepth cannot be null value or -88"
    }
    errs = [*errs, checkData(**args)]
    print('check ran - error check for matrix  and coresizedepth')


#Check: Sieve_or_depth is required when matrix is water
    print('begin check  grabfield')
    args = {
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows":grabeventdet[(grabeventdet['matrix'].isin(['blankwater','labwater','saltwater','freshwater'])) & (
            (grabeventdet['sieve_or_depth']== -88) | (grabeventdet['sieve_or_depth'].isna())) 
            ].tmp_row.tolist(),
        "badcolumn": "sieve_or_depth",
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "Since Matrix is water related, Sieve_or_Depth cannot be null value or -88"
    }
    errs = [*errs, checkData(**args)]
    print('check ran - error check for matrix  and sieve_or_depth')

#Check: Composition should not get filled in when matrix is water
    print('begin check grab field')
    args = {
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows":grabeventdet[(grabeventdet['matrix'].isin(['blankwater','labwater','saltwater','freshwater'])) &
            ((grabeventdet['composition'] == -88) | (grabeventdet['composition'].isna())) 
            ].tmp_row.tolist(),
        "badcolumn": "composition",
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "Since Matrix is water related, composition cannot be null value"
    }
    errs = [*errs, checkData(**args)]
    print('check ran error check for matrix  and grab')

#Check: Color should not get filled in when matrix is water
    print("begin check grabfied:")
    args = {
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows":grabeventdet[(grabeventdet['matrix'].isin(['blankwater','labwater','saltwater','freshwater'])) & 
            ((grabeventdet['color']== -88) | (grabeventdet['color'].isna()))
            ].tmp_row.tolist(),
        "badcolumn": "color",
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "Since Matrix is water related, color cannot be a null value or -88"
    }
    errs = [*errs, checkData(**args)]
    print('check ran - error check for matrix  and grab')
 
#Check: Odor should not get filled in when matrix is water
    print("begin check grabfied:")
    args = {
        "dataframe":grabeventdet,
        "tablename":'tbl_grabevent_details',
        "badrows":grabeventdet[(grabeventdet['matrix'].isin(['blankwater','labwater','saltwater','freshwater']))
        & ((grabeventdet['odor'] == -88) | (grabeventdet['odor'].isna()))].tmp_row.tolist(),
        "badcolumn": "odor",
        "error_type": "empty value",
        "is_core_error": False,
        "error_message": "Since Matrix is water related, odor cannot be a null value or -88"
    }
    errs = [*errs, checkData(**args)]
    print('check ran - error check for matrix  and grab')

# #testing time check
#     args = {
#             "dataframe":grabeventdet,
#             "tablename":'tbl_grabevent_details',
#             "badrows":time_format_check(grabeventdet),
#             "badcolumn": "samplecollectiontime",
#             "error_type": "empty value",
#             "is_core_error": False,
#             "error_message": "Samplecollectiontime is in the wrong format (HH:MM:SS). Make sure the number format of that column on the Excel sheet is set to text"
#             }
#     errs = [*errs, checkData(**args)]
#     print('error check for matrix and coresizediameter')

# #Ayah Finished


    return {'errors': errs, 'warnings': warnings}