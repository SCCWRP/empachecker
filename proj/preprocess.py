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
    print("BEGIN fix_case function")
    for table_name in all_dfs.keys():
        print(table_name)
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
        print(f"foreignkeys_rawvalues = {foreignkeys_rawvalues}")
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
    print("END fix_case function")
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
    # Last Edited Date: 08/18/23
    # Last Edited Coder: Duy Nguyen
    # NOTE (08/17/23): Duy adjusts the format so it follows the coding standard.
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


    
  
    
    ######################################################################################################################
    # ------------------------------------------------------------------------------------------------------------------ #
    # ------------------------------------------------End Fill  Data --------------------------------------------------- #
    # ------------------------------------------------------------------------------------------------------------------ #
    ######################################################################################################################


    #--------------------------------------------------------------------------------------------------------------------#

    print("END preprocessing")
    return all_dfs





'''
def fill_speciesnames(all_dfs):

    # I don't know why the code below is commented out - DUY 09-28-22
    # lu_fishspecies = pd.read_sql('SELECT scientificname, commonname FROM lu_fishspecies', g.eng)
    # names = {
    #     c: s  for s, c in list(zip(lu_fishspecies.scientificname, lu_fishspecies.commonname))
    # }

    # # I dont really know how to explain the code with comments to be honest but hopefully it makes sense
    # all_dfs['tbl_fish_abundance_data']['scientificname'] = all_dfs['tbl_fish_abundance_data'] \
    #     .apply(
    #         lambda x:
    #         names[x['scientificname']] if (pd.isnull(x['commonname']) or x['commonname'] == '') else x['commonname']
    #         ,
    #         axis = 1
    #     )
    
    # # here we need to get the key of the dictionary based on the value
    # all_dfs['tbl_fish_abundance_data']['commonname'] = all_dfs['tbl_fish_abundance_data'] \
    #     .apply(
    #         lambda x:
    #         list(names.keys())[list(names.values()).index(x['scientificname'])] if (pd.isnull(x['scientificname']) or x['scientificname'] == '') else x['scientificname']
    #         ,
    #         axis = 1
    #     )
    print("begin fill species name")
    print(all_dfs.keys())
    lu_list_to_fill = {
        'tbl_bruv_data': 'lu_fishmacrospecies',
        'tbl_fish_length_data': 'lu_fishmacrospecies',
        'tbl_fish_abundance_data': 'lu_fishmacrospecies',
        'tbl_vegetativecover_data': 'lu_plantspecies',
        'tbl_crabbiomass_length': 'lu_fishmacrospecies',
        'tbl_crabfishinvert_abundance': 'lu_fishmacrospecies'
    }
    for tab in all_dfs.keys():
        if tab in lu_list_to_fill.keys():
            print(tab)
            lu_list = pd.read_sql(f'SELECT scientificname, commonname, status FROM {lu_list_to_fill.get(tab)}', g.eng)
            print(lu_list)
            names = {
                (x,y): z  for x,y,z in list(
                    zip(
                        lu_list.scientificname, 
                        lu_list.commonname,
                        lu_list.status
                    )
                )
            }
            print(names)
            if not all_dfs[tab].empty:
                all_dfs[tab]['status'] = all_dfs[tab].apply(
                    lambda row: names[(row['scientificname'], row['commonname'])]
                    if ((row['scientificname'], row['commonname']) in names.keys()) and (pd.isnull(row['status']) or row['status'] == '') 
                    else row['status'] ,
                    axis=1
                )
    print("end fill species name")
    return all_dfs
'''