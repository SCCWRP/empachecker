from pandas import isnull

def checkData(dataframe, tablename, badrows, badcolumn, error_type, is_core_error, error_message, errors_list = [], q = None):
    if len(badrows) > 0:
        if q is not None:
            # This is the case where we run with multiprocessing
            # q would be a mutliprocessing.Queue() 
            q.put({
                "table": tablename,
                "rows":badrows,
                "columns":badcolumn,
                "error_type":error_type,
                "core_error" : is_core_error,
                "error_message":error_message
            })

        return {
            "table": tablename,
            "rows":badrows,
            "columns":badcolumn,
            "error_type":error_type,
            "core_error" : is_core_error,
            "error_message":error_message
        }
    return {}
        

def get_badrows(df, mask, errmsg):
    """
    df is a dataframe and mask is a series of booleans used to filter that dataframe. errmsg is self explanatory
    The way the 'mask' should be written should be such that it is True if it is a bad row, otherwise, False
    """

    assert len(df) == len(mask), "function - get_badrows - dataframe and mask have different number of rows"
    
    if not any(mask):
        return []

    badrows = [
        {
            'row_number': int(rownum),
            'value': val if not isnull(val) else '',
            'message': msg
        } 
        for rownum, val, msg in
        df[mask] \
        .apply(
            lambda row:
            (
                row.name + 1,

                # We wont be including the specific cell value in the error message for custom checks, 
                # it would be too complicated to implement in the cookie cutter type fashion that we are looking for, 
                # since the cookie cutter model that we have with the other checker proved effective for faster onboarding of new people to writing their own checks. 
                # Plus in my opinion, the inclusion of the specific value is really mostly helpful for the lookup list error. 
                # The only reason why the dictionary still includes this item is for the sake of consistency - 
                # (all the other "badrows" dictionaries are formatted in this way, since there are a few error types in core checks where the specific cell value was included.) 
                # This is ok since Core checks is 99.9% not going to change or have any additional features added, 
                # thus we dont need to make it super convenient for others to add checks

                # Note that for this "get_badrows" function, it works essentially the same way as the previous checker, 
                # where the user basically provides a line of code to subset the dataframe, along with an accompanying error message
                None, 
                errmsg
            ),
            axis = 1
        ) \
        .values
    ]

    return badrows
    