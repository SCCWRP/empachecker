"""Unit review: find logger sensor series whose values look like a different unit than recorded.

A series is one submission, site, station, sensor type, sensor id, parameter and recorded unit.
For each parameter + unit the usual value is the median of all series means with that parameter
and unit. A series is flagged when its mean is FACTOR times or more above or below the usual value.
Reviewers then record the correct unit for each flagged series. Raw data is never edited.
"""
import math
import statistics

FACTOR = 10

# parameter -> raw unit column, or the fixed unit text when the raw table has no unit column
UNIT_COLUMNS = {"h2otemp": "raw_h2otemp_unit", "conductivity": "raw_conductivity_unit",
                "salinity": "raw_salinity_unit", "do": "raw_do_unit", "do_pct": None,
                "depth": "raw_depth_unit", "pressure": "raw_pressure_unit",
                "atmospheric_pressure": "raw_atmospheric_pressure_unit", "qvalue": None, "ph": None,
                "turbidity": "raw_turbidity_unit", "chlorophyll": "raw_chlorophyll_unit", "orp": "raw_orp_unit"}
FIXED_UNITS = {"do_pct": "%", "qvalue": "unitless", "ph": "pH units"}
NOT_RECORDED = "Not recorded"

# parameter -> checker lookup table that lists its allowed units (column "unit")
LOOKUPS = {"h2otemp": "lu_temperature_unit", "conductivity": "lu_conductivity_unit",
           "salinity": "lu_salinity_unit", "do": "lu_do_unit", "depth": "lu_depth_unit",
           "pressure": "lu_pressure_unit", "atmospheric_pressure": "lu_pressure_unit",
           "turbidity": "lu_turbidity_unit", "chlorophyll": "lu_chlorophyll_unit", "orp": "lu_orp_unit"}


def series_means_sql(raw_table):
    """One row per series: identity, n, mean. -88, NULL and NaN readings are left out."""
    parts = []
    for p, col in UNIT_COLUMNS.items():
        unit = f"{col}::text" if col else f"'{FIXED_UNITS[p]}'"
        parts.append(f"('{p}', r.raw_{p}::float8, coalesce({unit}, '{NOT_RECORDED}'))")
    return f"""
        SELECT r.submissionid, r.siteid, r.stationno, r.sensortype, r.sensorid, v.parameter, v.unit,
               count(*) AS n, avg(v.val) AS mean
          FROM {raw_table} r
         CROSS JOIN LATERAL (VALUES {", ".join(parts)}) v(parameter, val, unit)
         WHERE v.val IS NOT NULL AND v.val <> -88 AND v.val <> 'NaN'::float8
           AND r.submissionid IS NOT NULL
         GROUP BY 1, 2, 3, 4, 5, 6, 7"""


def series_key(r):
    return "|".join(str("" if r.get(k) is None else r[k]) for k in
                    ("submissionid", "siteid", "stationno", "sensortype", "sensorid", "parameter", "unit"))


def times_off(mean, usual):
    """How many times the mean is away from the usual value, either direction. None when not comparable."""
    if mean is None or usual is None or not math.isfinite(mean) or not math.isfinite(usual) or usual == 0:
        return None
    ratio = mean / usual
    return math.inf if ratio <= 0 else max(ratio, 1 / ratio)


def flag_series(rows, factor=FACTOR):
    """rows: series_means_sql rows as dicts. Returns the flagged rows with key, usual_mean, ratio, times_off."""
    groups = {}
    for r in rows:
        if r.get("mean") is not None and math.isfinite(float(r["mean"])):
            groups.setdefault((r["parameter"], r["unit"]), []).append(float(r["mean"]))
    usual = {k: statistics.median(v) for k, v in groups.items()}
    out = []
    for r in rows:
        if r.get("mean") is None:
            continue
        u = usual.get((r["parameter"], r["unit"]))
        off = times_off(float(r["mean"]), u)
        if off is not None and off >= factor:
            out.append({**r, "key": series_key(r), "mean": float(r["mean"]), "usual_mean": u,
                        "ratio": float(r["mean"]) / u, "times_off": off})
    return out
