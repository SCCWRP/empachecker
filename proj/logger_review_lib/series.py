"""Turn raw logger rows into the per-submission payload the review page uses.

Ported from build_submission_review.py (database-admin, logger-threshold-calibration), adapted to pandas 1.5.
The series logic is the same, split so that the payload builder and the removal logic
share one set of QC runs.
"""
import copy
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from . import qc_harness as qh

LIB_DIR = Path(__file__).resolve().parent
THRESHOLDS_FILE = LIB_DIR / "thresholds_main.json"
PARAMS = ["h2otemp", "conductivity", "salinity", "do", "do_pct", "depth", "pressure",
          "atmospheric_pressure", "qvalue", "ph", "turbidity", "chlorophyll", "orp"]
FIXED_UNITS = {"do_pct": "%", "qvalue": "unitless", "ph": "pH units"}
LABELS = {"h2otemp": "Temperature", "conductivity": "Conductivity", "salinity": "Salinity",
          "do": "Dissolved oxygen", "do_pct": "Dissolved oxygen saturation", "depth": "Depth",
          "pressure": "Pressure", "atmospheric_pressure": "Atmospheric pressure", "qvalue": "Q-value",
          "ph": "pH", "turbidity": "Turbidity", "chlorophyll": "Chlorophyll", "orp": "ORP"}
OFFSET_HOURS = {"UTC": 0, "PDT": 7, "PST": 8}
MISSING_VALUE = -88


class DataError(ValueError):
    """The raw rows cannot be turned into series (for example an unknown timezone)."""


def load_defaults():
    return qh.load_config(str(THRESHOLDS_FILE))["parameters"]


def json_value(v):
    """Same conversion as the prototype: datetimes to ISO strings, numbers to float or int."""
    if v is None:
        return None
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.isoformat()
    if isinstance(v, (str, int, bool)):
        return v
    v = float(v)
    return v if math.isfinite(v) else None


def normalize_rows(rows):
    """Rows as plain JSON-friendly dicts. The originals are left untouched."""
    return [{k: json_value(v) for k, v in r.items()} for r in rows]


def is_missing(v):
    return v is None or v == MISSING_VALUE


def raw_factor(p, unit, canonical):
    norm = lambda u: str(u).lower().replace(" ", "").replace("°", "deg").replace("μ", "u").replace("µ", "u")
    u = norm(unit)
    if u == norm(canonical):
        return 1.0
    if p == "h2otemp" and u in {"c", "degc", "celsius", "degreescelsius"}:
        return 1.0
    if p == "depth":
        return {"m": 1.0, "meters": 1.0, "cm": 0.01}.get(u)
    if p in {"pressure", "atmospheric_pressure"}:
        return {"cmh2o": 1.0, "kpa": 10.1972, "mbar": 1.01972, "dbar": 101.972, "psi": 70.307}.get(u)
    if p == "conductivity":
        return {"ms/cm": 1.0, "us/cm": 0.001}.get(u)
    if p == "salinity" and u in {"ppt", "psu"}:
        return 1.0
    if p == "qvalue" and u in {"unitless", "-", "none"}:
        return 1.0
    return None


def thresholds_for_raw(p, unit, cfg):
    """Default thresholds for a parameter converted into the raw unit, and whether the unit is known."""
    b = copy.deepcopy(cfg.get(p, {}))
    factor = raw_factor(p, unit, b.get("unit", ""))
    tests = {t: copy.deepcopy(b.get(t) or {}) for t in qh.TESTS}
    if factor is None:
        return {t: {} for t in qh.TESTS}, False
    for t, keys in {"range": ("fail_min", "suspect_min", "suspect_max", "fail_max"),
                    "spike": ("suspect", "fail"), "roc": ("min_step",), "flatline": ("tol",)}.items():
        for k in keys:
            if tests[t].get(k) is not None:
                tests[t][k] /= factor
    return tests, True


@dataclass
class Series:
    key: str            # identity|parameter|unit, the id the page uses
    identity: tuple
    parameter: str
    unit: str
    rows: list          # normalized rows, sorted by time then objectid
    utc: pd.Series
    x: np.ndarray       # NaN for missing
    interval: float
    nsteps: int
    mult: float
    defaults: dict
    known_units: bool


def _station_text(v):
    try:
        return f"{v:g}"
    except (TypeError, ValueError):
        return str(v)


def sensor_groups(rows):
    groups = {}
    for r in rows:
        identity = (r["siteid"], r["stationno"], r["sensortype"], r["sensorid"])
        groups.setdefault(identity, []).append(r)
    for g in groups.values():
        g.sort(key=lambda r: (r["samplecollectiontimestamp"], r["objectid"]))
    return groups


def build_series(rows, cfg):
    """Yield a Series for every sensor, parameter and unit that has at least one real value.

    rows must already be normalized (see normalize_rows)."""
    groups = sensor_groups(rows)
    out = []
    for identity, group in groups.items():
        for p in PARAMS:
            vals = [r.get("raw_" + p) for r in group]
            if not any(not is_missing(v) for v in vals):
                continue
            unit_of = lambda r: r.get("raw_" + p + "_unit") or FIXED_UNITS.get(p, "Not recorded")
            for unit in sorted({unit_of(r) for r in group}):
                selected = [r for r in group if unit_of(r) == unit]
                local = pd.to_datetime([r["samplecollectiontimestamp"] for r in selected])
                offset = [OFFSET_HOURS.get(r["samplecollectiontimezone"]) for r in selected]
                if any(o is None for o in offset):
                    raise DataError("Unknown samplecollectiontimezone")
                utc = pd.Series(pd.to_datetime(local + pd.to_timedelta(offset, unit="h"), utc=True))
                x = np.array([np.nan if is_missing(r.get("raw_" + p)) else r["raw_" + p] for r in selected], dtype=float)
                if not np.isfinite(x).any():
                    continue
                interval = qh.series_interval(pd.DataFrame({"timestamp_utc": utc}), qh.Options())
                th, supported = thresholds_for_raw(p, unit, cfg)
                info = qh.run_tests(utc, x, th, qh.Options(roc_include_current=False), interval)["_info"]
                key = "|".join(map(str, (*identity, p, unit)))
                out.append(Series(key, identity, p, unit, selected, utc, x, float(interval),
                                  info["nsteps"], info["multiplier"], th, supported))
    return out


def python_thresholds(th):
    """Make a threshold block behave in Python the way the page's JavaScript treats it.

    The page needs both suspect_n and fail_n for flatline, Python only checks suspect_n."""
    th = copy.deepcopy(th or {})
    fl = th.get("flatline") or {}
    if fl.get("suspect_n") is None or fl.get("fail_n") is None:
        th["flatline"] = {}
    return th


def run_flags(series, th):
    """Flags for one series with the given thresholds: dict test -> int list, plus 'any'."""
    flags = qh.run_tests(series.utc, series.x, python_thresholds(th), qh.Options(roc_include_current=False), series.interval)
    result = {t: flags[t] for t in qh.TESTS}
    result["any"] = qh.worst_flag(flags)
    return result


def series_payload(s):
    # whole seconds since the first reading, computed so it works on pandas 1.5 and newer
    whole = s.utc.dt.floor("s")
    sec = (whole - whole.iloc[0]).dt.total_seconds().astype("int64")
    return {"parameter": s.parameter, "label": LABELS[s.parameter], "unit": s.unit,
            "sensor": f"{s.identity[0]}, station {_station_text(s.identity[1])}, {s.identity[2]} {s.identity[3]}",
            "t": list(map(int, sec)),
            "timestamps": [r["samplecollectiontimestamp"] for r in s.rows],
            "zones": [r["samplecollectiontimezone"] for r in s.rows],
            "objectids": [r["objectid"] for r in s.rows],
            "x": [None if not np.isfinite(v) else float(v) for v in s.x],
            "interval": s.interval, "nsteps": s.nsteps, "mult": s.mult,
            "defaults": s.defaults, "known_units": s.known_units}


def build_submission(sid, rows, sites, cfg=None, with_flags=False):
    """The page payload for one submission, as in build_submission_review.py.

    rows: raw rows (any value types). sites: {siteid: {estuary_name, region, estuary_type, estuary_class}}.
    With with_flags also returns the default-threshold flags per 'sid|series key', the
    structure stored in submission_review_python_flags.json."""
    cfg = cfg if cfg is not None else load_defaults()
    rows = normalize_rows(rows)
    if not rows:
        raise DataError("The submission has no rows")
    rows.sort(key=lambda r: (r["samplecollectiontimestamp"], r["objectid"]))
    sid = str(sid)
    series, expected = {}, {}
    all_series = build_series(rows, cfg)
    for s in all_series:
        series[s.key] = series_payload(s)
        if with_flags:
            f = qh.run_tests(s.utc, s.x, s.defaults, qh.Options(roc_include_current=False), s.interval)
            expected[sid + "|" + s.key] = {t: list(map(int, f[t])) for t in qh.TESTS}
    idents = list(sensor_groups(rows))
    first = idents[0]
    site = (sites or {}).get(first[0]) or {}
    meta = {"siteid": first[0], "station": first[1], "sensor_type": first[2], "sensor_id": first[3],
            "estuary_name": site.get("estuary_name") or rows[0].get("estuaryname"),
            "region": site.get("region"), "estuary_type": site.get("estuary_type"),
            "estuary_class": site.get("estuary_class"), "more": len(idents) - 1}
    stamps = [r["samplecollectiontimestamp"] for r in rows]
    data = {"id": sid, "meta": meta, "row_count": len(rows), "start": min(stamps), "end": max(stamps), "series": series}
    return (data, expected) if with_flags else data
