import re

########## Ayah's elevation check function:
def check_elevation_fields(all_dfs):
    output = {
        'core_errors': [],
        'core_warnings': [],
    }

    sql = f"""
        select table_name from information_schema.columns
        where column_name in ('elevation_ellipsoid','elevation_orthometric')
        and table_name like 'tbl_%%' 
        group by table_name 
    """
    tbls_with_elevation = pd.read_sql(sql, g.eng)

    #A list of all the columns that need to be checked if empty or not
    columns_to_check = ['elevation_time','elevation_units','elevation_corr','elevation_datum']
    #Checking if the table names in all_dfs match the table in database that have the elevation columns 
    for item in all_dfs.keys():
        if item in list(tbls_with_elevation['table_name']):
    #Looping through every column to see if it is empty after filtring out the tables with the elevation columns 
            for cols in columns_to_check:
                badrows = all_dfs[item][(all_dfs[item]['elevation_ellipsoid'].notna() | all_dfs[item]['elevation_orthometric'].notna()) & ((all_dfs[item][cols] == -88) | (all_dfs[item][cols] == 'Not Recorded'))].index.tolist()
    #Adding them to output dictionary        
                if len(badrows) > 0:
                    tmp = {
                        'table': item,
                        'rows': badrows,
                        'columns': cols,
                        'error_type': 'Elevation Error',
                        'is_core_error': False,
                        'error_message': f'Since elevation_ellipsoid and/or elevation_orthometric are reported, then {cols} are required and cannot be -88 or "Not Recorded" '
                    }
                    output['core_errors'].append(tmp)
                        
    return output