# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, checkLogic, mismatch, get_primary_key
import re
import time


def global_custom(all_dfs):
    print("begin global custom checks")
    errs = []
    warnings = []


    #args = {
    #     "dataframe": pd.DataFrame({}),
    #     "tablename": '',
    #     "badrows": [],
    #     "badcolumn": "",
    #     "error_type": "",
    #     "is_core_error": False,
    #     "error_message": ""
    #}
    #errs = [*errs, checkData(**args)]

    print("# GLOBAL CUSTOM CHECK - 1")
    # Description: Siteid/estuaryname pair must match lookup list (multicolumn check)
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (): 
    # NOTE (): 
    print("# END GLOBAL CUSTOM CHECK - 1")




    
    print("# GLOBAL CUSTOM CHECK - 2")
    # Description: Scientificname/commoname pair for species must match lookup
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (): 
    # NOTE (): 
    print("# END GLOBAL CUSTOM CHECK - 2")




    print("# GLOBAL CUSTOM CHECK - 3")
    # Description: Scientificname/commoname/status for species must match lookup
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (): 
    # NOTE (): 
    print("# END GLOBAL CUSTOM CHECK - 3")




    print("# GLOBAL CUSTOM CHECK - 4")
    # Description: starttime should be entered in HH:MM format on 24-hour-clock
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (): 
    # NOTE (): 
    print("# END GLOBAL CUSTOM CHECK - 4")




    print("# GLOBAL CUSTOM CHECK - 5")
    # Description: endtime should be entered in HH:MM format on 24-hour-clock
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (): 
    # NOTE (): 
    print("# END GLOBAL CUSTOM CHECK - 5")




    print("# GLOBAL CUSTOM CHECK - 6")
    # Description: starttime must be before endtime
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (): 
    # NOTE (): 
    print("# END GLOBAL CUSTOM CHECK - 6")




    print("# GLOBAL CUSTOM CHECK - 7")
    # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Time_Ele is required
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (): 
    # NOTE (): 
    print("# END GLOBAL CUSTOM CHECK - 7")




    print("# GLOBAL CUSTOM CHECK - 8")
    # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_units is required
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (): 
    # NOTE (): 
    print("# END GLOBAL CUSTOM CHECK - 8")




    print("# GLOBAL CUSTOM CHECK - 9")
    # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Corr is required
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (): 
    # NOTE (): 
    print("# END GLOBAL CUSTOM CHECK - 9")




    print("# GLOBAL CUSTOM CHECK - 10")
    # Description: If Elevation_Ellipsoid or Elevation_Orthometric is reported, then Elevation_Datum is required
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (): 
    # NOTE (): 
    print("# END GLOBAL CUSTOM CHECK - 10")




    print("# GLOBAL CUSTOM CHECK - 11")
    # Description: 
    # Created Coder: 
    # Created Date: 
    # Last Edited Date: 
    # Last Edited Coder: 
    # NOTE (): 
    # NOTE (): 
    print("# END GLOBAL CUSTOM CHECK - 11")












    print("end global custom checks")
    return {'errors': errs, 'warnings': warnings}