import re
import pandas as pd
from pandas import isnull, read_sql, concat
from .functions import checkData, get_primary_key
from flask import current_app, session

# All the functions for the Core Checks should have the dataframe and the datatype as the two main arguments
# This is to allow the multiprocessing to work, so it can pass in the same args to all the functions
# Of course it would be possible to do it otherwise, but the mutlitask function we wrote in utils assumes 
# the case that all of the functions have the same arguments
def checkDuplicatesInSession(dataframe, tablename, eng, *args, output = None, **kwargs):
    """
    check for duplicates in session only
    """
    print("BEGIN function - checkDuplicatesInSession")
    
    pkey = get_primary_key(tablename, eng)

    # For duplicates within session, dataprovider is not necessary to check
    # Since it is assumed that all records within the submission are from the same dataprovider
    pkey = [col for col in pkey if col not in current_app.system_fields]

    # initialize return value
    ret = []

    if len(pkey) == 0:
        print("No Primary Key")
        return ret

    if any(dataframe.duplicated(pkey)):

        badrows = dataframe[dataframe.duplicated(pkey, keep = False)].index.tolist()
        ret = [
            checkData(
                dataframe = dataframe,
                tablename = tablename,
                badrows = badrows,
                badcolumn = ','.join(pkey),
                error_type = "Duplicated Rows",
                is_core_error = True,
                error_message = "You have duplicated rows{}".format( 
                    f" based on the primary key fields {', '.join(pkey)}"
                )
            )
        ]

        if output:
            output.put(ret)

        
    print("END function - checkDuplicatesInSession")
    return ret


def checkDuplicatesInProduction(dataframe, tablename, eng, *args, output = None, **kwargs):
    print("BEGIN function - checkDuplicatesInProduction")
    
    if session.get("final_submit_requested") == False:
        return []

    pkey = get_primary_key(tablename, eng)
    print(pkey)
    

    # initialize return values
    ret = []

    if len(pkey) == 0:
        print("No Primary Key")
        return ret
    
    dtype = session.get('datatype')
    submissionid = session.get('submissionid')
    
    if not dtype or not submissionid:
        raise ValueError("Session does not contain 'dtype' or 'submissionid'")
    
    # Set the table name
    tmp_table_name = f'tmp_{dtype}_{submissionid}'
    
    # Load the dataframe to the database table
    dataframe.to_sql(tmp_table_name, con=eng, if_exists='replace', index=False)

    # SQL query to get common rows based on primary key
    join_condition = " AND ".join([f"tmp.{col}::VARCHAR = tbl.{col}::VARCHAR" for col in pkey])

    # SQL query to get common rows based on the primary keys
    query = f"""
        SELECT tmp.* 
        FROM {tmp_table_name} tmp
        INNER JOIN {tablename} tbl
        ON {join_condition};
    """
    
    print("Executing duplicate check query...")
    
    # Execute the query and return the result as a DataFrame
    duplicates_df = pd.read_sql_query(query, eng)
    badrows = duplicates_df.index.tolist()


    
    ret = [
        checkData(
            dataframe = dataframe,
            tablename = tablename,
            badrows = badrows,
            badcolumn = ','.join([col for col in pkey if col not in current_app.system_fields]),
            error_type = "Duplicate",
            is_core_error = True,
            error_message = "This is a record which already exists in the database"
        )
    ]

    if output:
        output.put(ret)

    eng.execute(f"DROP TABLE IF EXISTS {tmp_table_name};")

    print("END function - checkDuplicatesInProduction")
    return ret
