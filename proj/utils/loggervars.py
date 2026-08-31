import pandas as pd
from sqlalchemy.exc import ProgrammingError

# This should probably come from the database somehow, but for now, its ok lets just leave it as it is
TEMPLATE_COLUMNS = [
    'projectid',
    'siteid',
    'estuaryname',
    'stationno',
    'sensortype',
    'sensorid',
    'samplecollectiontimestamp',
    'samplecollectiontimezone',
    'sensorlocation',
    'raw_depth',
    'raw_depth_unit',
    'raw_depth_qcflag_human',
    'raw_pressure',
    'raw_pressure_unit',
    'raw_pressure_qcflag_human',
    'raw_h2otemp',
    'raw_h2otemp_unit',
    'raw_h2otemp_qcflag_human',
    'raw_ph',
    'raw_ph_qcflag_human',
    'raw_conductivity',
    'raw_conductivity_unit',
    'raw_conductivity_qcflag_human',
    'raw_turbidity',
    'raw_turbidity_unit',
    'raw_turbidity_qcflag_human',
    'raw_do',
    'raw_do_unit',
    'raw_do_qcflag_human',
    'raw_do_pct',
    'raw_do_pct_qcflag_human',
    'raw_salinity',
    'raw_salinity_unit',
    'raw_salinity_qcflag_human',
    'raw_chlorophyll',
    'raw_chlorophyll_unit',
    'raw_chlorophyll_qcflag_human',
    'raw_orp',
    'raw_orp_unit',
    'raw_orp_qcflag_human',
    'raw_qvalue',
    'raw_qvalue_qcflag_human',
    'organization',
    'wqnotes',
    'raw_atmospheric_pressure_qcflag_human', 
    'raw_atmospheric_pressure', 
    'raw_atmospheric_pressure_unit', 
    'raw_atmospheric_pressure_qcflag_robot'
]


# There are many different ways the timezones come in for the raw logger files, this is a key value pairing to help us put the correct timezone
TIMEZONE_MAP = {
    "0:00": "UTC",
    "0": "UTC",
    "-7:00": "PDT",
    "-7": "PDT",
    "-8:00": "PST",
    "-8": "PST",
    "Coordinated Universal Time": "UTC",
    "Pacific Daylight Time": "PDT",
    "Pacific Standard Time": "PST",
    "GMT-07:00": "PDT",
    "GMT-08:00": "PST",
    "GMT-7:00": "PDT",
    "GMT-8:00": "PST",
    "GMT-00:00": "UTC",
    "GMT": "UTC"
}

SUPPORTED_SENSORTYPES = [
    'troll','tidbit','minidot','ctd', 'hydrolab'
]

# Parameters shown as buttons/plots on the Logger Data Visual tab (must match LOGGER_DATA_VISUAL_PARAMS in globals.js)
LOGGER_PLOT_PARAMS = ['do', 'do_pct', 'h2otemp', 'turbidity', 'salinity', 'pressure', 'chlorophyll']


def logger_plot_columns(columns):
    """Columns (that actually exist in `columns`) needed to render/edit the Logger Data Visual tab."""
    cols = ['samplecollectiontimestamp', 'samplecollectiontimezone']
    for param in LOGGER_PLOT_PARAMS:
        for suffix in ('', '_unit', '_qcflag_robot', '_qcflag_human'):
            col = f'raw_{param}{suffix}'
            if col in columns:
                cols.append(col)
    return cols


def get_logger_param_ranges(df, eng):
    """
    Valid min/max range (from the lu_{param}_unit lookup tables - the same ones the automated
    QA/QC flagging in logger_custom.py uses to compute the -4/-5 qcflags) for each plotted parameter,
    so the Logger Data Visual chart can draw a reference range for whichever unit the data is actually in.
    """
    ranges = {}
    for param in LOGGER_PLOT_PARAMS:
        col = f'raw_{param}'
        unit_col = f'{col}_unit'
        if col not in df.columns or unit_col not in df.columns:
            continue

        units_present = [u for u in df[unit_col].dropna().unique().tolist() if u != '']
        if not units_present:
            continue

        lu_table = 'lu_temperature_unit' if param == 'h2otemp' else f'lu_{param}_unit'
        try:
            lu_list = pd.read_sql(f"SELECT unit, min, max FROM {lu_table}", eng)
        except ProgrammingError:
            continue

        # most submissions use a single consistent unit for a given parameter - take the first one with a match
        for unit in units_present:
            match = lu_list[lu_list['unit'] == unit]
            if match.empty:
                continue
            row = match.iloc[0]
            if pd.isnull(row['min']) and pd.isnull(row['max']):
                continue
            ranges[param] = {
                'unit': str(unit),
                'min': None if pd.isnull(row['min']) else float(row['min']),
                'max': None if pd.isnull(row['max']) else float(row['max'])
            }
            break

    return ranges
