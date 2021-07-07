import multiprocessing as mp
import pandas as pd
import time
from itertools import chain
from .dupes import checkDuplicatesInSession, checkDuplicatesInProduction
from .lookups import checkLookUpLists
from .metadata import checkNotNull, checkPrecision, checkScale, checkLength, checkDataTypes
from .functions import fetch_meta, multitask


# goal here is to take in all_dfs as an argument and assemble the CoreChecker processes
def core(all_dfs, eng, all_meta):

    debug = True
    # to run with no multiprocessing
    errs = []
    for tbl, df in all_dfs.items():
        print(tbl)
        errs.extend(
            [
                checkDuplicatesInSession(df, tbl, eng, all_meta[tbl]),
                checkDuplicatesInProduction(df, tbl, eng, all_meta[tbl]),
                checkLookUpLists(df, tbl, eng, all_meta[tbl]),
                checkNotNull(df, tbl, eng, all_meta[tbl]),
                checkPrecision(df, tbl, eng, all_meta[tbl]),
                checkScale(df, tbl, eng, all_meta[tbl]),
                checkLength(df, tbl, eng, all_meta[tbl]),
                checkDataTypes(df, tbl, eng, all_meta[tbl])
            ]
            
            if debug 
            else
            
            multitask(
                [
                    checkDuplicatesInSession,
                    checkDuplicatesInProduction,
                    checkLookUpLists,
                    checkNotNull,
                    checkPrecision,
                    checkScale,
                    checkLength,
                    checkDataTypes
                ],
                df,
                tbl,
                eng,
                all_meta[tbl]
            )
        )
        print("done")
    print("core checks done")
    return errs
    
