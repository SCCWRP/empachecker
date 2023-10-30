import json, os, re
import pandas as pd
import geopandas as gpd



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
        badrows = tmp[pd.isnull(tmp._present_)][row_identifier].tolist() \
            if row_identifier not in (None, 'index') \
            else tmp[pd.isnull(tmp._present_)].index.tolist()
    else:
        badrows = []

    assert \
        all(isinstance(item, int) or (isinstance(item, str) and item.isdigit()) for item in badrows), \
        "In mismatch function - Not all items in 'badrows' are integers or strings representing integers"

    badrows = [int(x) for x in badrows]
    return badrows

def match(df1, df2, mergecols = None, left_mergecols = None, right_mergecols = None, row_identifier = 'tmp_row'):
    # gets rows in df1 that are in df2
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
        badrows = tmp[pd.notnull(tmp._present_)][row_identifier].tolist() \
            if row_identifier not in (None, 'index') \
            else tmp[pd.notnull(tmp._present_)].index.tolist()
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
        WITH tmp AS (
            SELECT
                C.COLUMN_NAME,
                C.data_type
            FROM
                information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage AS ccu USING (CONSTRAINT_SCHEMA, CONSTRAINT_NAME)
                JOIN information_schema.COLUMNS AS C ON C.table_schema = tc.CONSTRAINT_SCHEMA
                AND tc.TABLE_NAME = C.TABLE_NAME
                AND ccu.COLUMN_NAME = C.COLUMN_NAME
            WHERE
                constraint_type = 'PRIMARY KEY'
                AND tc.TABLE_NAME = '{tablename}'
        )
        SELECT
            tmp.COLUMN_NAME,
            tmp.data_type,
                column_order.custom_column_position
        FROM
            tmp
            LEFT JOIN (
                SELECT
                    COLUMN_NAME,
                    custom_column_position
                FROM
                    column_order
                WHERE
                    TABLE_NAME = '{tablename}'
            ) column_order ON column_order."column_name" = tmp.COLUMN_NAME
                ORDER BY
                custom_column_position
    """
    pkey_df = pd.read_sql(pkey_query, eng)
    
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

def check_bad_time_format(value):
    """
        Check if a value is in the format HH:MM or HH:MM:SS 24-hour
    """
    if value == "Not recorded":
        return False
    
    correct_time_format =  r'^(0?[0-9]|1\d|2[0-3]):([0-5]\d)(:([0-5]\d))?$'
    return not bool(re.match(correct_time_format, str(value)))

def check_bad_start_end_time(start_time, end_time):
    # note that this function returns True if the row of data is bad
    # if the format is bad, don't consider them in this check, i.e. return False
    if start_time == "Not recorded" or end_time == "Not recorded":
        return False
    if check_bad_time_format(start_time) or check_bad_time_format(end_time):
        return False
    # Timestamp uses the current date as the date if only a time is provided
    # so, use UTC to avoid daylight savings issues
    return pd.Timestamp(start_time, tz = "UTC") > pd.Timestamp(end_time, tz = "UTC")

def check_elevation_columns(df, column):
    return (
        (
            (~pd.isnull(df['elevation_ellipsoid']) & (df['elevation_ellipsoid'] != -88)) |
            (~pd.isnull(df['elevation_orthometric']) & (df['elevation_orthometric'] != -88)) 
        ) & 
        (
            pd.isnull(df[column]) | (df[column] == 'Not recorded') | (df[column] == '')
        )
    )

def check_multiple_dates_within_site(submission):
    print("enter check_multiple_dates_within_site")
    assert 'siteid' in submission.columns, "'siteid' is not a column in submission dataframe"
    assert 'samplecollectiondate' in submission.columns, "'samplecollectiondate' is not a column in submission dataframe"
    assert 'tmp_row' in submission.columns, "'tmp_row' is not a column in submission dataframe"
    assert not submission.empty, "submission dataframe is empty"

    # group by station code and samplecollectiondate, grab the first index of each unique date, reset to dataframe
    submission_groupby = submission.groupby(['siteid','samplecollectiondate'])['tmp_row'].first().reset_index()
    print(submission_groupby.groupby('siteid'))
    # filter on grouped stations that have more than one unique sample date, output sorted list of indices 
    badrows = sorted(list(set(submission_groupby.groupby('siteid').filter(lambda x: x['samplecollectiondate'].count() > 1)['tmp_row'])))

    # count number of unique dates within a siteid
    num_unique_sample_dates = len(badrows)
    print("done check_multiple_dates_within_site")
    return (badrows, num_unique_sample_dates)


def check_consecutiveness(df, groupcols, col_to_check):
    '''
        This function checks for consecutive values in a field within a group, and return indices if the values are not consecutive
    '''
    assert 'tmp_row' in df.columns, 'tmp_row not found in dataframe'
    assert df[col_to_check].apply(lambda val: isinstance(val, int)).all(), f'all values in {col_to_check} needed to be integers'

    def is_consecutive(df):
        df = df[df[col_to_check] != -88].sort_values(by=col_to_check)
        consecutive = (df[col_to_check].diff().dropna() == 1).all()
        if not consecutive:
            return df.tmp_row.tolist()
        else:
            return []    

    badrows = [idx for indexes in df.groupby(groupcols).apply(is_consecutive) for idx in indexes]
    return badrows

def check_date_order(df, before_date, after_date):
    # This function checks that dates are in the correct order

    df[before_date] = pd.to_datetime(df[before_date], format='%d/%m/%Y').dt.date
    df[after_date] = pd.to_datetime(df[after_date], format='%d/%m/%Y').dt.date
    
    return df[(df[before_date] > df[after_date])].tmp_row.tolist()



def check_coordinates_in_shapefile(df, shapefile_path, lat_col, long_col):
    assert 'tmp_row' in df.columns, 'tmp_row not found in dataframe'
    shapefile_path = os.path.join(os.getcwd(), 'shapes', 'california_combined.shp')
    cali_shapefile = gpd.read_file(shapefile_path)
    if lat_col in df.columns and long_col in df.columns:
            gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[longcol], df[latcol]))
            badrows = gdf[gdf.disjoint(cali_shapefile.unary_union)]
            return badrows
    
