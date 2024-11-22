# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, check_consecutiveness,get_primary_key,mismatch
import re
import pandas as pd
import datetime as dt
import time

def trash(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # define errors and warnings list
    errs = []
    warnings = []

    # Safely handle missing tables
    trashsamplearea = all_dfs.get('tbl_trashsamplearea', pd.DataFrame())
    trashquadrattally = all_dfs.get('tbl_trashquadrattally', pd.DataFrame())
    trashtimesearchtally = all_dfs.get('tbl_trashtimesearchtally', pd.DataFrame())

    if not trashsamplearea.empty:
        trashsamplearea['tmp_row'] = trashsamplearea.index
    if not trashquadrattally.empty:
        trashquadrattally['tmp_row'] = trashquadrattally.index
    if not trashtimesearchtally.empty:
        trashtimesearchtally['tmp_row'] = trashtimesearchtally.index

    # Primary keys
    trashsamplearea_pkey = get_primary_key('tbl_trashsamplearea', g.eng) if not trashsamplearea.empty else []
    trashquadrattally_pkey = get_primary_key('tbl_trashquadrattally', g.eng) if not trashquadrattally.empty else []
    trashquadrattally_trashsamplearea_shared_pkey = (
        [x for x in trashquadrattally_pkey if x in trashsamplearea_pkey]
        if not trashsamplearea.empty and not trashquadrattally.empty
        else []
    )
    ######################################################################################################################
    #--------------------------------------------------------------------------------------------------------------------#
    #--------------------------------------------- Logic Checks ---------------------------------------------------------#
    #--------------------------------------------------------------------------------------------------------------------#
    ###################################################################################################################### 

    if not trashsamplearea.empty and not trashquadrattally.empty:
        print("# LOGIC CHECK - 1")
        # Description: Records in samplearea need to be in quadrat
        # Created Coder: Duy Nguyen
        # Created Date: 11/22/2024
        # Last Edited Date: 

        errs.append(
            checkData(
                tablename='tbl_trashsamplearea',
                badrows=mismatch(trashsamplearea, trashquadrattally, trashquadrattally_trashsamplearea_shared_pkey),
                badcolumn=','.join(trashquadrattally_trashsamplearea_shared_pkey),
                error_type='Undefined Error',
                error_message=f"Records in the trashsamplearea should have the corresponding records in the trashquadrattally based on these columns {','.join(trashquadrattally_trashsamplearea_shared_pkey)}"
            )
        )
        print("# END LOGIC CHECK - 1")

        print("# LOGIC CHECK - 2")
        # Description: Records in quadrat need to be in samplearea
        # Created Coder: Duy Nguyen
        # Created Date: 11/22/2024
        # Last Edited Date: 

        errs.append(
            checkData(
                tablename='tbl_trashquadrattally',
                badrows=mismatch(trashquadrattally, trashsamplearea, trashquadrattally_trashsamplearea_shared_pkey),
                badcolumn=','.join(trashquadrattally_trashsamplearea_shared_pkey),
                error_type='Undefined Error',
                error_message=f"Records in the trashquadrattally should have the corresponding records in the trashsamplearea based on these columns {','.join(trashquadrattally_trashsamplearea_shared_pkey)}"
            )
        )
        print("# END LOGIC CHECK - 2")

        print("# LOGIC CHECK - 3")
        # Description: If trash is 'No' in trashsamplearea, then the corresponding record should have 'None' in trashdebriscategory 
        # and 'No Trash Present' in debrisitem in quadrat
        # Created Coder: Duy Nguyen
        # Created Date: 11/22/2024
        # Last Edited Date: 

        errs.append(
            checkData(
                tablename='tbl_trashsamplearea',
                badrows=trashsamplearea[
                    (trashsamplearea['trash'] == 'No') &
                    (
                        (trashquadrattally['trashdebriscategory'] != 'None') |
                        (trashquadrattally['debrisitem'] != 'No Trash Present')
                    )
                ],
                badcolumn='trash, trashdebriscategory, debrisitem',
                error_type='Logic Error',
                error_message="If trash is 'No' in trashsamplearea, the corresponding record in quadrat should have 'None' in trashdebriscategory and 'No Trash Present' in debrisitem."
            )
        )
        print("# END LOGIC CHECK - 3")

    if not trashsamplearea.empty:
        print("# LOGIC CHECK - 4")
        # Description: Quadrat must be consecutive within primary keys ('projectid', 'siteid', 'sampledate', 'quadrat', 'stationno', 'estuaryname', 'transect')
        # Created Coder: Ayah Halabi  
        # Created Date: 11/16/2023
        # Last Edited Date: 

        groupby_cols = ['projectid', 'estuaryname', 'sampledate', 'siteid', 'stationno', 'transect']
        errs.append(
            checkData(
                tablename='tbl_trashsamplearea',
                badrows=check_consecutiveness(trashsamplearea, groupby_cols, 'quadrat'),
                badcolumn='quadrat',
                error_type='Undefined Error',
                error_message=f"quadrat values must be consecutive for each transect. Records are grouped by {', '.join(groupby_cols)}"
            )
        )
        print("# END LOGIC CHECK - 4")




    ######################################################################################################################
    #--------------------------------------------------------------------------------------------------------------------#
    #--------------------------------------------- END OF Logic Checks --------------------------------------------------#
    #--------------------------------------------------------------------------------------------------------------------#
    ###################################################################################################################### 

 
    lu_trashplastic= pd.read_sql("SELECT item FROM lu_trashplastic",g.eng).item.tolist()
    lu_trashnonplastic = pd.read_sql("SELECT coniitemtemstruction FROM lu_trashnonplastic",g.eng).item.tolist()


    ######################################################################################################################
    #--------------------------------------------------------------------------------------------------------------------#
    #--------------------------------------------- Trash Quadrat Tally Checks ------------------------------------------#
    #--------------------------------------------------------------------------------------------------------------------#
    ######################################################################################################################

    if not trashquadrattally.empty:
        print("# LOGIC CHECK - 5")
        # Description: If resulttotal is empty, then debrisitem must be 'No Trash Present'. 
        # If debrisitem is not 'No Trash Present', then resulttotaltext is required, and its value must be 'M' or 'H'.
        # Created Coder: Duy Nguyen
        # Created Date: 11/22/2024
        # Last Edited Date: 

        errs.append(
            checkData(
                tablename='tbl_trashquadrattally',
                badrows=trashquadrattally[
                    ((trashquadrattally['resulttotal'].isnull()) & (trashquadrattally['debrisitem'] != 'No Trash Present')) |
                    ((trashquadrattally['debrisitem'] != 'No Trash Present') &
                    ((trashquadrattally['resulttotaltext'].isnull()) | ~trashquadrattally['resulttotaltext'].isin(['M', 'H'])))
                ],
                badcolumn='resulttotal, debrisitem, resulttotaltext',
                error_type='Logic Error',
                error_message="If resulttotal is empty, debrisitem must be 'No Trash Present'. If debrisitem is not 'No Trash Present', resulttotaltext must be 'M' or 'H'."
            )
        )
        print("# END LOGIC CHECK - 5")

        print("# LOGIC CHECK - 6")
        # Description: If debriscategory is 'Plastic', then item must be in lu_trashplastic.
        # Created Coder: Duy Nguyen
        # Created Date: 11/22/2024
        # Last Edited Date: 

        errs.append(
            checkData(
                tablename='tbl_trashquadrattally',
                badrows=trashquadrattally[
                    (trashquadrattally['debriscategory'] == 'Plastic') &
                    ~trashquadrattally['item'].isin(lu_trashplastic)
                ],
                badcolumn='debriscategory, item',
                error_type='Logic Error',
                error_message="If debriscategory is 'Plastic', item must be in lu_trashplastic."
            )
        )
        print("# END LOGIC CHECK - 6")

        print("# LOGIC CHECK - 7")
        # Description: If debriscategory is 'Non-Plastic', then item must be in lu_trashnonplastic.
        # Created Coder: Duy Nguyen
        # Created Date: 11/22/2024
        # Last Edited Date: 

        errs.append(
            checkData(
                tablename='tbl_trashquadrattally',
                badrows=trashquadrattally[
                    (trashquadrattally['debriscategory'] == 'Non-Plastic') &
                    ~trashquadrattally['item'].isin(lu_trashnonplastic)
                ],
                badcolumn='debriscategory, item',
                error_type='Logic Error',
                error_message="If debriscategory is 'Non-Plastic', item must be in lu_trashnonplastic."
            )
        )
        print("# END LOGIC CHECK - 7")


    ######################################################################################################################
    #--------------------------------------------------------------------------------------------------------------------#
    #----------------------------------------------- Trash Time Search Tally --------------------------------------------#
    #--------------------------------------------------------------------------------------------------------------------#
    ######################################################################################################################

    if not trashtimesearchtally.empty:
        print("# LOGIC CHECK - 8")
        # Description: If resulttotal is empty, then debrisitem must be 'No Trash Present'. 
        # If debrisitem is not 'No Trash Present', then resulttotaltext is required, and its value must be 'M' or 'H'.
        # Created Coder: Duy Nguyen
        # Created Date: 11/22/2024
        # Last Edited Date: 

        errs.append(
            checkData(
                tablename='tbl_trashtimesearchtally',
                badrows=trashtimesearchtally[
                    ((trashtimesearchtally['resulttotal'].isnull()) & 
                    (trashtimesearchtally['debrisitem'] != 'No Trash Present')) |
                    ((trashtimesearchtally['debrisitem'] != 'No Trash Present') &
                    ((trashtimesearchtally['resulttotaltext'].isnull()) | 
                    ~trashtimesearchtally['resulttotaltext'].isin(['M', 'H'])))
                ],
                badcolumn='resulttotal, debrisitem, resulttotaltext',
                error_type='Logic Error',
                error_message="If resulttotal is empty, debrisitem must be 'No Trash Present'. If debrisitem is not 'No Trash Present', resulttotaltext must be 'M' or 'H'."
            )
        )
        print("# END LOGIC CHECK - 8")

        print("# LOGIC CHECK - 9")
        # Description: If debriscategory is 'Plastic', then item must be in lu_trashplastic.
        # Created Coder: Duy Nguyen
        # Created Date: 11/22/2024
        # Last Edited Date: 

        errs.append(
            checkData(
                tablename='tbl_trashtimesearchtally',
                badrows=trashtimesearchtally[
                    (trashtimesearchtally['debriscategory'] == 'Plastic') &
                    ~trashtimesearchtally['item'].isin(lu_trashplastic)
                ],
                badcolumn='debriscategory, item',
                error_type='Logic Error',
                error_message="If debriscategory is 'Plastic', item must be in lu_trashplastic."
            )
        )
        print("# END LOGIC CHECK - 9")

        print("# LOGIC CHECK - 10")
        # Description: If debriscategory is 'Non-Plastic', then item must be in lu_trashnonplastic.
        # Created Coder: Duy Nguyen
        # Created Date: 11/22/2024
        # Last Edited Date: 

        errs.append(
            checkData(
                tablename='tbl_trashtimesearchtally',
                badrows=trashtimesearchtally[
                    (trashtimesearchtally['debriscategory'] == 'Non-Plastic') &
                    ~trashtimesearchtally['item'].isin(lu_trashnonplastic)
                ],
                badcolumn='debriscategory, item',
                error_type='Logic Error',
                error_message="If debriscategory is 'Non-Plastic', item must be in lu_trashnonplastic."
            )
        )
        print("# END LOGIC CHECK - 10")


    return {'errors': errs, 'warnings': warnings}
    
