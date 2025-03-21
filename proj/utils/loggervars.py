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


