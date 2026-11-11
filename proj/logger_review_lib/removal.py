"""Which readings are removed, and the rows written to the final table.

Rules (same as the review page):
  A reading is removed when its timestamp lies inside a removal period (inclusive at both
  ends, compared as recorded wall-clock text), or when remove-fails is on and its overall
  automatic flag is Fail, unless it lies inside a good period. A removal period beats a
  good period. Periods apply to every parameter and sensor of the submission.
Only real readings are removed. A missing reading (null or -88) has no value to remove and is
copied as is.
"""
from . import series as sr

FAIL = 4
REMOVED_FLAG = -3


def stamp_text(v):
    """Timestamp as the page compares it: YYYY-MM-DDTHH:MM:SS."""
    s = v.isoformat() if hasattr(v, "isoformat") else str(v)
    return s.replace(" ", "T")


def inside(periods, t):
    return any(p["start"] <= t <= p["end"] for p in periods)


def removal_flags(timestamps, any_flags, remove_fails, removed_periods, kept_periods):
    """List of bool per reading, the page rule: removed[i] in submission_review_template.html."""
    out = []
    for t, f in zip(timestamps, any_flags):
        kept = inside(kept_periods, t)
        out.append(inside(removed_periods, t) or (bool(remove_fails) and int(f) == FAIL and not kept))
    return out


def removed_readings(raw_rows, thresholds, remove_fails, removed_periods, kept_periods, cfg=None):
    """Set of (objectid, parameter) for every removed reading.

    thresholds: saved {series key: threshold block} or None. A series with no saved block uses
    its defaults."""
    cfg = cfg if cfg is not None else sr.load_defaults()
    thresholds = thresholds or {}
    rows = sr.normalize_rows(raw_rows)
    removed = set()
    for s in sr.build_series(rows, cfg):
        th = thresholds.get(s.key) or s.defaults
        flags = sr.run_flags(s, th)
        stamps = [r["samplecollectiontimestamp"] for r in s.rows]
        gone = removal_flags(stamps, flags["any"], remove_fails, removed_periods, kept_periods)
        for r, g, v in zip(s.rows, gone, s.x):
            if g and v == v:  # v is NaN for a missing reading
                removed.add((r["objectid"], s.parameter))
    return removed


def resolved_thresholds(raw_rows, saved, cfg=None):
    """Full threshold set used: the saved block per series, defaults where nothing was saved."""
    cfg = cfg if cfg is not None else sr.load_defaults()
    saved = saved or {}
    return {s.key: saved.get(s.key) or s.defaults for s in sr.build_series(sr.normalize_rows(raw_rows), cfg)}


def build_final_rows(raw_rows, removed):
    """Final rows from raw rows. Pure function.

    For every removed (objectid, parameter) the value is NULL and raw_<p>_qcflag_human is -3.
    Everything else is copied as is. Row count equals the raw row count."""
    by_object = {}
    for oid, p in removed:
        by_object.setdefault(oid, set()).add(p)
    final = []
    for r in raw_rows:
        row = dict(r)
        for p in by_object.get(r["objectid"], ()):
            row["raw_" + p] = None
            row["raw_" + p + "_qcflag_human"] = REMOVED_FLAG
        final.append(row)
    return final
