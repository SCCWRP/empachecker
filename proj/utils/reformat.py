import os
import csv
import pandas as pd
import re
from .loggervars import TIMEZONE_MAP, TEMPLATE_COLUMNS, SUPPORTED_SENSORTYPES
from flask import session, g

def read_tidbit(tidbit_path):
    # this csv has a different encoding, which is weird
    # just read first line to get serial number
    with open(tidbit_path, encoding='utf-8-sig') as tidbit_file:
        # split on ':' so easy to access serial number
        # this file uses quotes to prevent splitting, so we ignore quotes while reading, and strip them out later
        tidbit_reader = csv.reader(tidbit_file, delimiter = ':', quoting = csv.QUOTE_NONE) 
        tidbit_serial_num = tidbit_reader.__next__()[1].strip(' "')

    # use only 2nd and 3rd column since those are datetimes and temperature readings
    tidbit_data = pd.read_csv(tidbit_path, encoding='utf-8-sig', header = 1, usecols = [1, 2], parse_dates = [0], infer_datetime_format = True)
    
    tidbit_data = pd.concat([tidbit_data, pd.DataFrame(columns=TEMPLATE_COLUMNS)])
    
    # this one has extra columns that I don't think we care about, which introduces some NaNs between the regular intervals
    tidbit_data = tidbit_data[tidbit_data.iloc[:, 1].isna() == False]
    tidbit_data['samplecollectiontimestamp'] =  tidbit_data.iloc[:, 0]
    tidbit_data['samplecollectiontimezone'] = TIMEZONE_MAP[tidbit_data.iloc[:, 0].name.split(", ")[1]]

    tidbit_data['raw_h2otemp'] = tidbit_data.iloc[:, 1]
    # search in temperature column for degree symbol, use single character after that
    tidbit_data['raw_h2otemp_unit'] = re.search(r"(?<=\N{DEGREE SIGN}).", tidbit_data.iloc[:, 1].name, flags=re.UNICODE)[0]

    tidbit_data['raw_h2otemp_unit'] = tidbit_data['raw_h2otemp_unit'].str.replace("C","deg C")

    tidbit_data['sensorid'] = tidbit_serial_num

    tidbit_data = tidbit_data[TEMPLATE_COLUMNS]
    return tidbit_data

def read_minidot(minidot_path):
    # read csv header, get serial number/sensor ID from first two lines of header
    with open(minidot_path, encoding='utf-8') as minidot_file:
        minidot_reader = csv.reader(minidot_file, delimiter = ':') # splitting on ':' puts sensor ID in standard position on second line
        minidot_reader.__next__() # read first line, do nothing with it
        minidot_serial_num = minidot_reader.__next__()[1].strip() # read second line, get second item in list, which is sensor ID, strip out any whitespace

    # read in minidot data, starting at row 5
    # tab separated, but can sometimes be more than one tab
    # since regex, this uses python engine to parse through csv, which may be slow
    # tested, 3 times slower than c engine, but files won't be large enough for this to matter, I think
    minidot_data = pd.read_csv(minidot_path, header = 5, sep = '[\t,]+', engine = 'python', encoding='utf-8')
    minidot_data.columns = [column.strip() for column in minidot_data.columns]
    minidot_data = pd.concat([minidot_data, pd.DataFrame(columns=TEMPLATE_COLUMNS)])

    minidot_data['sensorid'] = minidot_serial_num

    template_to_raw_map = {
        'raw_h2otemp': 'Temperature',
        'raw_do': 'Dissolved Oxygen',
        'raw_do_pct': 'Dissolved Oxygen Saturation',
        'raw_qvalue': 'Q'
    }

    raw_columns = list(template_to_raw_map.keys())
    minidot_data[raw_columns] = minidot_data[raw_columns].apply(
        lambda x: minidot_data[template_to_raw_map[x.name]]
    )

    unit_columns = [
        column for column in TEMPLATE_COLUMNS \
        if "unit" in column and column[:-5] in template_to_raw_map
    ]

    minidot_data[unit_columns] = minidot_data[unit_columns].apply(
        # get all characters within parentheses in first row of each column, excluding parentheses
        lambda x: re.search(r"(?<=\().+?(?=\))", minidot_data[x.name[:-5]][0])[0] 
    )

    minidot_data.drop(0, inplace = True) # drop first row, which is just units for each column

    minidot_data['raw_do_unit'] = minidot_data['raw_do_unit'].apply(
        lambda x: 'mg/L' if x == 'mg/l' else x
    )
    minidot_data['samplecollectiontimestamp'] = pd.to_datetime(minidot_data['UTC_Date_&_Time'])
    minidot_data['samplecollectiontimezone'] = 'UTC'
    minidot_data[raw_columns] = minidot_data[raw_columns].astype('float64')

    minidot_data = minidot_data[TEMPLATE_COLUMNS]

    return minidot_data


# this data is already in a friendly format :D
# but no unit information at all, not even timezone D:

def read_ctd(ctd_path):
    ctd_data = pd.read_excel(ctd_path)
    ctd_data = pd.concat([ctd_data, pd.DataFrame(columns=TEMPLATE_COLUMNS)])

    ctd_data['samplecollectiontimestamp'] = pd.to_datetime(ctd_data['TimeStamp'])

    template_to_raw_map = {
        'raw_conductivity': 'Conductivity',
        'raw_pressure': 'Pressure',
        'raw_h2otemp': 'Temperature'
    }
    



    raw_columns = list(template_to_raw_map.keys())
    ctd_data[raw_columns] = ctd_data[raw_columns].apply(
        lambda x: ctd_data[template_to_raw_map[x.name]]
    )

    ctd_data['sensorid'] = ctd_data['SerialNumber']
    ctd_data = ctd_data[TEMPLATE_COLUMNS]

    ctd_data['raw_conductivity_unit'] = session['login_info'].get('login_ctd_conductivityunit')
    ctd_data['raw_pressure_unit'] = session['login_info'].get('login_ctd_pressureunit')
    ctd_data['raw_h2otemp_unit'] = session['login_info'].get('login_ctd_temperatureunit')
    ctd_data['samplecollectiontimezone'] = session['login_info'].get('login_ctd_timezone')
    
    return ctd_data

def read_troll(troll_path):
    print("inside read troll")
    # read csv header, get values for reading in as dataframe later
    with open(troll_path,  encoding='utf-8') as troll_file:
        troll_reader = csv.reader(troll_file)
        print("troll reader")
        for i, row in enumerate(troll_reader):
            if 'Serial Number' in row:
                serial_index = row.index('Serial Number')
                troll_serial_num = row[serial_index + 1] # text search for serial number
            elif len(row) > 0 and 'Time Zone:' in row[0]: # 'Time Zone:' indicates data is about to start
                timezone = row[0].split(": ")[1]
                header_index = i + 4  # data begins 4 rows below 'Time Zone:' line, adjust for 0 indexing
                break
    troll_data = pd.read_csv(troll_path, header = header_index, usecols = [0,2,3,4], skip_blank_lines=False)
    # if file has double empty line at end of file, pandas reads one as a row, so drop it
    if pd.isnull(troll_data.iloc[-1]).all():
        troll_data.drop(troll_data.tail(1).index, inplace=True)

    troll_data = pd.concat([troll_data, pd.DataFrame(columns=TEMPLATE_COLUMNS)])

    troll_data['sensorid'] = troll_serial_num
    troll_data['samplecollectiontimestamp'] = pd.to_datetime(troll_data['Date and Time'])
    troll_data['samplecollectiontimezone'] = TIMEZONE_MAP[timezone] \
        if timezone in TIMEZONE_MAP else "UNKNOWN"    

    template_to_raw_map = {
        'raw_depth': 'Depth',
        'raw_pressure': 'Pressure',
        'raw_h2otemp': 'Temperature'
    }

    raw_columns = list(template_to_raw_map.keys())
    troll_data[raw_columns] = troll_data[raw_columns].apply(
        lambda x: troll_data[troll_data.columns[troll_data.columns.str.startswith(template_to_raw_map[x.name])][0]]
    )

    unit_columns = [
        column for column in TEMPLATE_COLUMNS \
        if "unit" in column and column[:-5] in template_to_raw_map
    ]
    
    troll_data[unit_columns] = troll_data[raw_columns].apply(
        lambda x: re.search(
            r"(?<=\().+?(?=\))", # get all characters within parentheses, excluding parentheses
            troll_data.columns[troll_data.columns.str.startswith(template_to_raw_map[x.name])][0]
        )[0]
    )
    troll_data['raw_h2otemp_unit'] = troll_data['raw_h2otemp_unit'].apply(
        lambda x: f'deg {x}' if x in ['C', 'F'] else x
    )
    troll_data['raw_pressure_unit'] = troll_data['raw_pressure_unit'].apply(
        lambda x: x.lower() if x == 'mBar' else x
    )
    troll_data['raw_depth_unit'] = troll_data['raw_depth_unit'].apply(
        lambda x: x.lower()
    )

    troll_data = troll_data[TEMPLATE_COLUMNS]

    return troll_data

def read_hydrolab(hydrolab_path):
    print("in read_hydrolab")
    header_index = 0

    with open(hydrolab_path, encoding='utf-16') as hydrolab_file:
        hydrolab_reader = csv.reader(hydrolab_file, delimiter="\t") # tab delimited
        for i, row in enumerate(hydrolab_reader):
            print()
            if "Time Zone (UTC)" in row:
                hours_mins = row[1]
            elif "Date & Time" in row:
                header_index = i
                break
    # weird csv encoding
    hydrolab_data = pd.read_csv(hydrolab_path, header = header_index, sep='\t', encoding='utf-16')
    print(hydrolab_data)

    hydrolab_data = pd.concat([hydrolab_data, pd.DataFrame(columns = TEMPLATE_COLUMNS)])

    print("check column in dataframe")
    print('Sonde HL7 Serial Number' in hydrolab_data.columns.tolist())
    print(hydrolab_data.columns)

    hydrolab_data['sensorid'] = hydrolab_data['Sonde HL7 Serial Number'].str.split(":", expand=True)[1]
    hydrolab_data['samplecollectiontimestamp'] = pd.to_datetime(hydrolab_data['Date & Time'])
    
    hydrolab_data['samplecollectiontimezone'] = TIMEZONE_MAP[hours_mins] \
        if hours_mins in TIMEZONE_MAP else "UNKNOWN"

    template_to_raw_map = {
        'raw_conductivity': 'Conductivity µS/cm (Conductivity)' \
            if 'Conductivity µS/cm (Conductivity)' in hydrolab_data.columns \
            else 'Specific Conductivity mS/cm (Conductivity)',
        'raw_turbidity': 'Turbidity NTU (Turbidity/Brush)' \
            if 'Turbidity NTU (Turbidity/Brush)' in hydrolab_data.columns \
            else 'Turbidity mV (Turbidity/Brush)',
        'raw_h2otemp': u'Temperature \N{DEGREE SIGN}C (Temperature)', # unicode characters?? 
        'raw_ph': 'pH units (pH)',
        'raw_do': 'DO mg/L (Hach LDO)',
        'raw_do_pct': 'DO %SAT (Hach LDO)',
        'raw_salinity': 'Salinity psu (Conductivity)',
        'raw_chlorophyll': 'Chlorophyll a µg/L (Chlorophyll a)'
    }

    raw_columns = list(template_to_raw_map.keys())
    hydrolab_data[raw_columns] = hydrolab_data[raw_columns].transform(
        lambda x: hydrolab_data[template_to_raw_map[x.name]]
    )

    unit_columns = [
        column for column in TEMPLATE_COLUMNS \
        if "unit" in column and column[:-5] in template_to_raw_map
    ]

    hydrolab_data[unit_columns] = hydrolab_data[unit_columns].apply(
        # want to transform the name of all the units columns
        # -5 index becauseof definition of template/raw mapping above
        lambda x: template_to_raw_map[x.name[:-5]]\
            # units begin right before first open parenthesis in column name
            # then the last word in the column name before the parenthesis
            # is the unit word
            .rsplit(" (")[0].rsplit(" ")[-1]\
            # replace uncommon characters with common ones
            .replace("µ", "u").replace(u"\N{DEGREE SIGN}", "deg ")
    )

    hydrolab_data = hydrolab_data[TEMPLATE_COLUMNS]

    return hydrolab_data


# One function to rule them all
def parse_raw_logger_data(loggertype: str, filepath: str):
    
    # These so far are the supported sensor raw file types
    # Since i'm going to be using these values in eval, i put in the function definition so that nothing outside the function can cause unwanted code to be executed
    # if we add another supported type, we can add here
    SUPPORTED_SENSORTYPES = ['tidbit','troll','ctd','minidot','hydrolab']
    lowercase_loggertype = str(loggertype).lower()
    lowercase_loggertype = "hydrolab" if lowercase_loggertype == "hl series sensors" else lowercase_loggertype

    assert \
        lowercase_loggertype in SUPPORTED_SENSORTYPES, \
        f"""Logger Type {lowercase_loggertype} is not (yet) supported. Supported types are: {', '.join(SUPPORTED_SENSORTYPES)}"""
    
    dat = eval(f"read_{lowercase_loggertype}")(filepath)

    # add robot columns
    robocols = [
        'raw_ph_qcflag_robot',
        'raw_pressure_qcflag_robot',
        'raw_chlorophyll_qcflag_robot',
        'raw_conductivity_qcflag_robot',
        'raw_orp_qcflag_robot',
        'raw_h2otemp_qcflag_robot',
        'raw_turbidity_qcflag_robot',
        'raw_depth_qcflag_robot',
        'raw_do_qcflag_robot',
        'raw_qvalue_qcflag_robot',
        'raw_do_pct_qcflag_robot',
        'raw_salinity_qcflag_robot',
        'qaqc_comment'
    ]
    for col in robocols:
        dat[col] = None
    
    # Order columns in a user friendly manner
    orderedcols = pd.read_sql("SELECT column_name FROM column_order WHERE table_name = 'tbl_wq_logger_raw' ORDER BY custom_column_position", g.eng).column_name.tolist()
    orderedcols = [c for c in orderedcols if c in dat.columns]

    return dat[orderedcols]


