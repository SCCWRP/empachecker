import pandas as pd
import multiprocessing as mp
import re, time
from math import log10
from functools import lru_cache
import datetime
from flask import g 

def checkData(dataframe, tablename, badrows, badcolumn, error_type, is_core_error = True, error_message = "Error", errors_list = [], q = None):
    if len(badrows) > 0:
        if q is not None:
            # This is the case where we run with multiprocessing
            # q would be a mutliprocessing.Queue() 
            q.put({
                "table": tablename,
                "rows":badrows,
                "columns":badcolumn,
                "error_type":error_type,
                "is_core_error" : is_core_error,
                "error_message":error_message
            })

        return {
            "table": tablename,
            "rows":badrows,
            "columns":badcolumn,
            "error_type":error_type,
            "is_core_error" : is_core_error,
            "error_message":error_message
        }
    return {}
        
      

# For the sake of checking the data in multiple ways at the same time
def multitask(functions: list, *args):
    '''funcs is a list of functions that will be turned into processes'''
    output = mp.Queue()
    processes = [
        mp.Process(target = function, args = (*args,), kwargs = {'output': output}) 
        for function in functions
    ]

    starttime = time.time()
    for p in processes:
        print("starting a process")
        p.start()
        
    for p in processes:
        print("joining processes")
        p.join()

    finaloutput = []
    while output.qsize() > 0:
        finaloutput.append(output.get())
    print("output from the multitask/mutliprocessing function")

    # printing final output caused an error because of ascii encoding
    # We must be careful to not leave stuff like this uncommented, but rather only print during testing and debugging
    # then remove after
    
    # print("did this print the finaloutput")
    # print(finaloutput)
    return finaloutput



@lru_cache(maxsize=128, typed=True)
def convert_dtype(t, x):
    try:
        if ((pd.isnull(x)) and (t == int)):
            return True
        t(x)
        return True
    except Exception as e:
        if t == pd.Timestamp:
            # checking for a valid postgres timestamp literal
            # Postgres technically also accepts the format like "January 8 00:00:00 1999" but we won't be checking for that unless it becomes a problem
            datepat = re.compile("\d{4}-\d{1,2}-\d{1,2}\s*(\d{1,2}:\d{1,2}:\d{2}(\.\d+){0,1}){0,1}$")
            return bool(re.match(datepat, str(x)))
        return False

@lru_cache(maxsize=128, typed=True)
def check_precision(x, precision):
    try:
        int(x)
    except Exception as e:
        # if you cant call int on it, its not numeric
        # Meaning it is not valid to check precision
        # thus we return true.
        # if its the wrong datatype it should get picked up by that check
        return True

    if pd.isnull(precision):
        return True

    x = abs(x)
    if 0 < x < 1:
        # if x is a fraction, it doesnt matter. it should be able to go into a numeric field regardless
        return True
    left = int(log10(x)) + 1 if x > 0 else 1
    if 'e-' in str(x):
        # The idea is if the number comes in in scientific notation
        # it will look like 7e11 or something like that
        # We dont care if it is to a positive power of 10 since that doesnt affect the digits to the right
        # we care if it's a negative power, which looks like 7.23e-5 (.0000723)
        powerof10 = int(str(x).split('e-')[-1])
        
        # search for the digits to the right of the decimal place
        rightdigits = re.search("\.(\d+)",str(x).split('e-')[0])
        
        if rightdigits: # if its not a NoneType, it found a match
            rightdigits = rightdigits.groups()[0]
        
        right = powerof10 + len(rightdigits)
    else:
        # frac part is zero if there is no decimal place, or if it came in with scientific notation
        # because this else block represents the case where the power was positive
        
        frac_part = abs(int(re.sub("\d*\.","",str(x)))) if ( '.' in str(x) ) and ('e' not in str(x)) else 0
        
        # remove trailing zeros (or zeroes?)
        if frac_part > 0:
            while (frac_part % 10 == 0):
                frac_part = int(frac_part / 10)

        right = len(str(frac_part)) if frac_part > 0 else 0
    return True if left + right <= precision else False

@lru_cache(maxsize=128, typed=True)
def check_scale(x, scale):
    if pd.isnull(scale):
        return True
    if pd.isnull(x):
        return True
    try:
        int(x)
    except Exception as e:
        # if you cant call int on it, its not numeric
        # Meaning it is not valid to check precision
        # thus we return true.
        # if its the wrong datatype it should get picked up by that check
        return True
    x = abs(x)

    if 'e-' in str(x):
        # The idea is if the number comes in in scientific notation
        # it will look like 7e11 or something like that
        # We dont care if it is to a positive power of 10 since that doesnt affect the digits to the right
        # we care if it's a negative power, which looks like 7.23e-5 (.0000723)
        powerof10 = int(str(x).split('e-')[-1])
        
        # search for the digits to the right of the decimal place
        rightdigits = re.search("\.(\d+)",str(x).split('e-')[0])
        
        if rightdigits: # if its not a NoneType, it found a match
            rightdigits = rightdigits.groups()[0]
            right = powerof10 + len(rightdigits)
        else:
            right = 0
    else:
        # frac part is zero if there is no decimal place, or if it came in with scientific notation
        # because this else block represents the case where the power was positive
        #print('HERE')
        #print(x)
        #print(str(x))
        frac_part = abs(int(re.sub("\d*\.","",str(x)))) if ( '.' in str(x) ) and ('e' not in str(x)) else 0
        #print('NO')
        
        # remove trailing zeros (or zeroes?)
        if frac_part > 0:
            while (frac_part % 10 == 0):
                frac_part = int(frac_part / 10)

        right = len(str(frac_part)) if frac_part > 0 else 0
    return True if right <= scale else False

@lru_cache(maxsize=128, typed=True)
def check_length(x, maxlength):
    if pd.isnull(maxlength):
        return True
    return True if len(str(x)) <= int(maxlength) else False



def fetch_meta(tablename, eng):

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
                table_name = '{tablename}';
            """, 
            eng
        )

    meta['dtype'] = meta \
        .udt_name \
        .apply(
            # This pretty much only works if the columns were defined through Arc
            lambda x: 
            int if 'int' in x 
            else str if x == 'varchar' 
            else pd.Timestamp if x == 'timestamp' 
            else float if x == 'numeric' 
            else None
        )  

    return meta



# This function allows you to put in a table name and get back the primary key fields of the table
def get_primary_key(tablename, eng):
    # eng is a sqlalchemy database connection

    # This query gets us the primary keys of a table. Not in a python friendly format
    # Copy paste to Navicat, pgadmin, or do a pd.read_sql to see what it gives
    pkey_query = f"""
        SELECT 
            c.column_name, 
            c.data_type
        FROM information_schema.table_constraints tc 
        JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
        JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
            AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
        WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{tablename}';
    """
    pkey_df = pd.read_sql(pkey_query, eng)
    
    pkey = pkey_df.column_name.tolist() if not pkey_df.empty else []
    
    return pkey

def check_time_format(all_dfs):
    """
        This function checks if the time columns inside the data frame is in the correct format (HH:MM:SS or HH:MM).
        The way we use to determine if the column is time-column or not is we look if the name ends with time.
    """
    output = {'core_errors': []}
    for key in all_dfs.keys():
        df = all_dfs[key]
        df['tmp_row'] = df.index
        for col in df.columns:
            if col.endswith('time'):
                badrows = df[df[col].apply(
                    lambda val: check_bad_time_format(val)
                )].tmp_row.tolist()
                if len(badrows) > 0:
                    tmp = {
                        'table': key,
                        'rows': badrows,
                        'columns': col,
                        'error_type': 'Time Format Error',
                        'is_core_error': True,
                        'error_message': 'Time should be entered as 24-hour format HH:MM or HH:MM:SS'
                    }
                    output['core_errors'].append(tmp)
    return output


def check_bad_time_format(value):
    """
        Check if a value is in the format HH:MM or HH:MM:SS 24-hour
    """
    correct_time_format =  r'^(0?[0-9]|1\d|2[0-3]):([0-5]\d)(:([0-5]\d))?$'
    return not bool(re.match(correct_time_format, str(value)))

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
                badrows = all_dfs[item][(all_dfs[item]['elevation_ellipsoid'].notna() | all_dfs[item]['elevation_orthometric'].notna()) & ( all_dfs[item][cols].isna() | (all_dfs[item][cols] == -88) | (all_dfs[item][cols].str.lower() == 'not recorded') )].index.tolist()
    #Adding them to output dictionary        
                if len(badrows) > 0:
                    tmp = {
                        'table': item,
                        'rows': badrows,
                        'columns': cols,
                        'error_type': 'Elevation Error',
                        'is_core_error': False,
                        'error_message': f'{cols} cannot be null or -88 since Elevation_ellipsoid and/or Elevation_orthometric are nonempty '
                    }
                    output['core_errors'].append(tmp)
                        
    return output


                






