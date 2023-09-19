from flask import current_app, g
import pandas as pd
import re
import time
import numpy as np

def strip_whitespace(all_dfs: dict):
    for table_name in all_dfs.keys():
        #First get all the foreign keys columns
        meta = pd.read_sql(
            f"""
            SELECT 
                table_name, 
                column_name, 
                is_nullable, 
                data_type,
                udt_name, 
                character_maximum_length, 
                numeric_precision, 
                numeric_scale 
            FROM 
                information_schema.columns 
            WHERE 
                table_name = '{table_name}'
                AND column_name NOT LIKE 'login_%%'
                AND column_name NOT IN ('{"','".join(current_app.system_fields)}');
            """, 
             g.eng
        )
        
        meta[meta['udt_name'] == 'varchar']
        table_df = all_dfs[f'{table_name}'] 
        # Get all varchar cols from table in all_dfs
        all_varchar_cols = meta[meta['udt_name'] == 'varchar'].column_name.values
        
        # Strip whitespace left side and right side
        table_df[all_varchar_cols] = table_df[all_varchar_cols].apply(
            lambda col: col.apply(lambda x: str(x).strip() if not pd.isnull(x) else x)
        )
        all_dfs[f"{table_name}"] = table_df
    return all_dfs

def fix_case(all_dfs: dict):
    for table_name in all_dfs.keys():
        table_df = all_dfs[f'{table_name}'] 
    #Among all the varchar cols, only get the ones tied to the lookup list -- modified to only find lu_lists that are not of numeric types
        lookup_sql = f"""
            SELECT
                kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                isc.data_type AS column_data_type 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
                JOIN information_schema.columns as isc
                ON isc.column_name = ccu.column_name
                AND isc.table_name = ccu.table_name
            WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND tc.table_name='{table_name}'
            AND ccu.table_name LIKE 'lu_%%'
            AND isc.data_type NOT IN ('integer', 'smallint', 'numeric');
        """
        lu_info = pd.read_sql(lookup_sql, g.eng)
           
        # The keys of this dictionary are the column's names in the dataframe, values are their lookup values
        foreignkeys_luvalues = dict(
            zip(
                lu_info.column_name,
                [
                    pd.read_sql(f"SELECT {lu_col} FROM {lu_table}",  g.eng)[f'{lu_col}'].to_list() 
                    for lu_col,lu_table 
                    in zip (lu_info.foreign_column_name, lu_info.foreign_table_name) 
                ]
            ) 
        )
        # Get their actual values in the dataframe
        foreignkeys_rawvalues = {
            x : [
                item 
                for item in set(table_df[x])
                if str(item).lower() in list(map(str.lower,foreignkeys_luvalues[x])) # bug: 'lower' for 'str' objects doesn't apply to 'int' object
            ]  
            for x in lu_info.column_name
        }
        # Remove the empty lists in the dictionary's values. 
        # Empty lists indicate there are values that are not in the lu list for a certain column
        # This will be taken care of by core check.
        foreignkeys_rawvalues = {x:y for x,y in foreignkeys_rawvalues.items() if len(y)> 0}
        
        # Now we need to make a dictionary to fix the case, something like {col: {wrongvalue:correctvalue} }
        # this is good indentation, looks good and easy to read - Robert
        fix_case  = {
            col : {
                  item : new_item 
                  for item in foreignkeys_rawvalues[col] 
                  for new_item in foreignkeys_luvalues[col] 
                  if str(item).lower() == new_item.lower()
            } 
            for col in foreignkeys_rawvalues.keys()
        }
        table_df = table_df.replace(fix_case)                
        all_dfs[f'{table_name}'] = table_df
    return all_dfs

def fill_daubenmiremidpoint(all_dfs):
    if 'tbl_vegetativecover_data' in all_dfs.keys():
        df = all_dfs['tbl_vegetativecover_data']
        lu_estimatedcover = pd.read_sql('SELECT * from lu_estimatedcover', g.eng)

        lu_dict = {
            (a,b): (c,d) 
            for a,b,c,d in zip(
                lu_estimatedcover['estimatedcover_min'],
                lu_estimatedcover['estimatedcover_max'],
                lu_estimatedcover['percentcovercode'],
                lu_estimatedcover['daubenmiremidpoint']
            )
        }

        def find_key_by_value(dictionary, value):
            if value == 0:
                return (0,0)
            for key in dictionary.keys():
                if key[0] < value <= key[1]:
                    return key
            return None
        
        df['percentcovercode'] = df.apply(
            lambda row: lu_dict.get(
                find_key_by_value(lu_dict, row['estimatedcover']), 
                (row['percentcovercode'], None)
            )[0],
            axis=1    
        )
        df['daubenmiremidpoint'] = df.apply(
            lambda row: lu_dict.get(
                find_key_by_value(lu_dict, row['estimatedcover']), 
                (None, row['daubenmiremidpoint'])
            )[1],
            axis=1    
        )
        all_dfs['tbl_vegetativecover_data'] = df

    return all_dfs

fishmacro_tbls = [
    'tbl_bruv_data',
    'tbl_crabbiomass_length',
    'tbl_crabfishinvert_abundance',
    'tbl_epifauna_data',
    'tbl_fish_abundance_data',
    'tbl_fish_length_data'
]

plant_tbls = [
    'tbl_algaecover_data',
    'tbl_floating_data',
    'tbl_savpercentcover_data',
    'tbl_vegetativecover_data'
]
# benthic_tbls = ['tbl_benthicinfauna_abundance',
#                 'tbl_benthicinfauna_biomass',
#                 'tbl_benthiclarge_abundance']

all_tbls = {
    'fishmacro_tbls': {
        'tbls': fishmacro_tbls,
        'lu_list': 'lu_fishmacrospecies'
        },
    'plant_tbls': {
        'tbls': plant_tbls,
        'lu_list': 'lu_plantspecies'
        },
}

def fill_commonname(all_dfs):
    for _, tbl_arr in all_tbls.items():
        lu_list = tbl_arr['lu_list']
        for tbl in tbl_arr['tbls']:
            if tbl in all_dfs.keys():
                df = all_dfs[tbl]
                for label, row in df.iterrows():
                    sci_name = row['scientificname']
                    com_name = row['commonname']
                    if pd.isna(com_name) and pd.notna(sci_name):
                        common_name_df = pd.read_sql(f"SELECT commonname FROM {lu_list} WHERE scientificname = '{sci_name}'", g.eng)
                        new_common_name = str(common_name_df.iat[0,0])
                        df.loc[label, 'commonname'] = new_common_name
                        all_dfs[tbl] = df
                        print(f'filled common name for {sci_name} with {new_common_name}')
    return all_dfs

def fill_status(all_dfs):
    for _, tbl_arr in all_tbls.items():
        lu_list = tbl_arr['lu_list']
        for tbl in tbl_arr['tbls']:
            if tbl in all_dfs.keys():
                df = all_dfs[tbl]
                for label, row in df.iterrows():
                    sci_name = row['scientificname']
                    status = row['status']
                    if pd.isna(status) and pd.notna(sci_name):
                        status_df = pd.read_sql(f"SELECT status FROM {lu_list} WHERE scientificname = '{sci_name}'", g.eng)
                        new_status = str(status_df.iat[0,0])
                        df.loc[label, 'status'] = new_status
                        all_dfs[tbl] = df
                        print(f'filled status for {sci_name} with {new_status}')
    return all_dfs

def fill_area(all_dfs):
    if 'tbl_fish_sample_metadata' in all_dfs.keys():
        df = all_dfs['tbl_fish_sample_metadata']
        for label, row in df.iterrows():
            area = row['area_m2']
            length = row['seinelength_m']
            distance = row['seinedistance_m']
            if pd.isna(area):
                if pd.notna(length) and pd.notna(distance):
                    new_area = length * distance
                    df.loc[label, 'area_m2'] = new_area
                    all_dfs['tbl_fish_sample_metadata'] = df
    return all_dfs
    

def fix_projectid(all_dfs):
    '''
    The function "fix_projectid" takes a dictionary of dataframes, modifies the 'projectid' column in each dataframe 
    by replacing its value with 'Baja-rails' if the corresponding value in the 'siteid' column is 'Baja-SQ' or 'Baja-PB'. 
    The final dictionary of modified dataframes is returned.
    '''
    for table_name in all_dfs.keys():
        print(table_name)
        table_df = all_dfs[table_name] 
        if ('siteid' not in table_df.columns) | ('projectid' not in table_df.columns):
            continue
        table_df = table_df.assign(
            projectid = table_df.apply(
                lambda row: 'Baja-rails' if row['siteid'] in ['Baja-SQ','Baja-PB']
                else row['projectid'],
                axis=1
            )
        )
        all_dfs[table_name] = table_df
    return all_dfs



def clean_data(all_dfs):
    print("BEGIN preprocessing")

    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------Clean Data ------------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################
    print("# begin data cleaning - 1")
    # Description: Strip whitespaces for all values
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/18/23
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard.
    all_dfs = strip_whitespace(all_dfs)
    print("# end data cleaning - 1")
    

    print("# begin data cleaning - 2")
    # Description: Match the value to lookup list value if case insensitivity is the only issue
    # Created Coder: NA
    # Created Date: NA
    # Last Edited Date: 08/28/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard.
    # NOTE (08/28/23): This doesnt work for lu lists that are related indirectly to the table-- see QA screenshots
    all_dfs = fix_case(all_dfs)
    print("# end data cleaning - 2")


    print("# begin data cleaning - 3")
    # Description: Modifies the 'projectid' column in each dataframe by replacing its value with 'Baja-rails' if the corresponding value in the 'siteid' column is 'Baja-SQ' or 'Baja-PB'
    # Created Coder: Nick L
    # Created Date: Nick L
    # Last Edited Date: 08/18/23
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard.
    all_dfs = fix_projectid(all_dfs)
    print("# end data cleaning - 3")


    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------END Clean Data ----------------------------------------------------#
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################



    #--------------------------------------------------------------------------------------------------------------------#



    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------Fill  Data ------------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################

    print("# begin data filling - 1")
    # Description: fill in daubenmiremidpoint values if estimatedcover and percent cover code match in the table and lookup list
    # Created Coder: Ayah 
    # Created Date: Ayah 
    # Last Edited Date: 08/18/23
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard.
    all_dfs = fill_daubenmiremidpoint(all_dfs)
    print("# end data filling - 1")
    
    
    
    
    print("# begin data filling - 2")
    # Description: fill commonname based on scientificname from appropriate lookup list
    # Created Coder: Caspian Thackeray
    # Created Date:  08/28/23
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/28/23): Begin writing this check
    # NOTE (08/29/23): Finished writing this check
    all_dfs = fill_commonname(all_dfs)
    print("# end data filling - 2")
    
    
    
    
    print("# begin data filling - 3")
    # Description: fill status based on scienticficname,commonname from appropriate lookup lists
    # Created Coder: Caspian Thackeray
    # Created Date:  08/29/23
    # Last Edited Date: 08/29/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/29/23): Wrote this check
    all_dfs = fill_status(all_dfs)
    print("# end data filling - 3")
    
    
    
    
    print("# begin data filling - 4")
    # Description: fill the area_m2 column if it is empty. Formula: area_m2 = seinelength_m x seinedistance_m
    # Created Coder: Caspian Thackeray
    # Created Date:  08/30/23
    # Last Edited Date: 08/30/23
    # Last Edited Coder: Caspian Thackeray
    # NOTE (08/17/23): Wrote this check
    all_dfs = fill_area(all_dfs)
    print("# end data filling - 4")


    
  
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------End Fill  Data --------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################


    #--------------------------------------------------------------------------------------------------------------------#

    print("END preprocessing")
    return all_dfs
