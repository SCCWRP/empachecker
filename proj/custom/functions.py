import json, os
from pandas import isnull, DataFrame, read_sql, merge

def checkData(tablename, badrows, badcolumn, error_type, is_core_error = False, error_message = "Error", errors_list = [], q = None, **kwargs):
    
    # See comments on the get_badrows function
    # doesnt have to be used but it makes it more convenient to plug in a check
    # that function can be used to get the badrows argument that would be used in this function
    
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
        



# checkLogic() returns indices of rows with logic errors
def checkLogic(df1, df2, cols: list, error_type = "Logic Error", df1_name = "", df2_name = ""):
    ''' each record in df1 must have a corresponding record in df2'''
    print(f"cols: {cols}")
    print(f"df1 cols: {df1.columns.tolist()}")
    print(set([x.lower() for x in cols]).issubset(set(df1.columns)))

    print(f"df2 cols: {df2.columns.tolist()}")
    assert \
    set([x.lower() for x in cols]).issubset(set(df1.columns)), \
    "({}) not in columns of {} ({})" \
    .format(
        ','.join([x.lower() for x in cols]), df1_name, ','.join(df1.columns)
    )
    print("passed 1st assertion")
    assert \
    set([x.lower() for x in cols]).issubset(set(df2.columns)), \
    "({}) not in columns of {} ({})" \
    .format(
        ','.join([x.lower() for x in cols]), df2_name, ','.join(df2.columns)
    )
    print("passed 2nd assertion")
    # 'Kristin wrote this code in ancient times.'
    # 'I still don't fully understand what it does.'
    # all() returns whether all elements are true
    print("before badrows")
    badrows = df1[~df1[[x.lower() for x in cols]].isin(df2[[x.lower() for x in cols]].to_dict(orient='list')).all(axis=1)].index.tolist()
    print(f"badrows: {badrows}")
    print("after badrows")
    #consider raising error if cols list is not str (see mp) --- ask robert though bc maybe nah

    return(badrows)

def mismatch(df1, df2, mergecols = None, left_mergecols = None, right_mergecols = None, row_identifier = 'tmp_row'):
    
    # gets rows in df1 that are not in df2
    # row identifier column is tmp_row by default

    # If the first dataframe is empty, then there can be no badrows
    if df1.empty:
        return []

    # if second dataframe is empty, all rows in df1 are mismatched
    if df2.empty:
        return df1[row_identifier].tolist() if row_identifier != 'index' else df1.index.tolist()

    # Hey, you never know...
    assert not '_present_' in df1.columns, 'For some reason, the reserved column name _present_ is in columns of df1'
    assert not '_present_' in df2.columns, 'For some reason, the reserved column name _present_ is in columns of df2'

    if mergecols is not None:
        assert set(mergecols).issubset(set(df1.columns)), f"""In mismatch function - {','.join(mergecols)} is not a subset of the columns of the dataframe """
        assert set(mergecols).issubset(set(df2.columns)), f"""In mismatch function - {','.join(mergecols)} is not a subset of the columns of the dataframe """
        tmp = df1.astype(str) \
            .merge(
                df2.astype(str).assign(_present_='yes'),
                on = mergecols, 
                how = 'left',
                suffixes = ('','_df2')
            )
    
    elif (right_mergecols is not None) and (left_mergecols is not None):
        assert set(left_mergecols).issubset(set(df1.columns)), f"""In mismatch function - {','.join(left_mergecols)} is not a subset of the columns of the dataframe of the first argument"""
        assert set(right_mergecols).issubset(set(df2.columns)), f"""In mismatch function - {','.join(right_mergecols)} is not a subset of the columns of the dataframe of the second argument"""
        
        tmp = df1.astype(str) \
            .merge(
                df2.astype(str).assign(_present_='yes'),
                left_on = left_mergecols, 
                right_on = right_mergecols, 
                how = 'left',
                suffixes = ('','_df2')
            )

    else:
        raise Exception("In mismatch function - improper use of function - No merging columns are defined")

    if not tmp.empty:
        badrows = tmp[isnull(tmp._present_)][row_identifier].tolist() \
            if row_identifier not in (None, 'index') \
            else tmp[isnull(tmp._present_)].index.tolist()
    else:
        badrows = []

    assert \
        all(isinstance(item, int) or (isinstance(item, str) and item.isdigit()) for item in badrows), \
        "In mismatch function - Not all items in 'badrows' are integers or strings representing integers"
    
    badrows = [int(x) for x in badrows]
    return badrows

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
    pkey_df = read_sql(pkey_query, eng)
    
    pkey = pkey_df.column_name.tolist() if not pkey_df.empty else []
    
    return pkey

def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
    assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
    assert isinstance(lookup_cols, list), "lookup columns is not a list"

    lookup_df = lookup_df.assign(match="yes")
    
    for c in check_cols:
        df_to_check[c] = df_to_check[c].apply(lambda x: str(x).lower().strip())
    for c in lookup_cols:
        lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())

    merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
    badrows = merged[pd.isnull(merged.match)].tmp_row.tolist()
    return(badrows)