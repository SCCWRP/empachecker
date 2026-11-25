"""Default thresholds from sde.lu_wq_logger_qc_threshold (Store.fetch_thresholds)."""
from proj.logger_review_lib import series as sr
from test_api import SID, make, store, raw  # noqa: F401  (fixtures)

TESTS = ("range", "spike", "roc", "flatline")
COLS = {"range": {"fail_min": "range_fail_min", "suspect_min": "range_suspect_min",
                  "suspect_max": "range_suspect_max", "fail_max": "range_fail_max"},
        "spike": {"suspect": "spike_suspect", "fail": "spike_fail"},
        "roc": {"suspect_sd": "roc_suspect_sd", "fail_sd": "roc_fail_sd", "min_step": "roc_min_step"},
        "flatline": {"suspect_n": "flatline_suspect_n", "fail_n": "flatline_fail_n", "tol": "flatline_tol"}}


def json_as_rows():
    """thresholds_main.json as lookup rows, the same shape as create_threshold_lookup.sql seeds."""
    rows = []
    for name, p in sr.load_defaults().items():
        r = {"parameter": name, "region": "All", "parameter_name": p.get("sheet_name"), "unit": p.get("unit"),
             "notes": p.get("notes")}
        for t, cols in COLS.items():
            for k, c in cols.items():
                r[c] = (p.get(t) or {}).get(k)
        rows.append(r)
        for region, rng in (p.get("range_by_region") or {}).items():
            rr = {"parameter": name, "region": region, **{c: None for cols in COLS.values() for c in cols.values()}}
            rr.update({c: rng.get(k) for k, c in COLS["range"].items()})
            rows.append(rr)
    return rows


def _tests(p):
    return {t: {k: v for k, v in (p.get(t) or {}).items()} or None for t in TESTS}


def test_rows_reproduce_the_json_defaults():
    from_json = sr.load_defaults()
    from_rows = sr.load_defaults(json_as_rows())
    assert set(from_rows) == set(from_json)
    for name, p in from_json.items():
        q = from_rows[name]
        assert q["unit"] == p.get("unit"), name
        assert _tests(q) == _tests(p), name
        assert q.get("range_by_region") == p.get("range_by_region"), name
        if q["flatline"]:
            assert isinstance(q["flatline"]["suspect_n"], int)


def test_null_limits_stay_unset():
    rows = json_as_rows()
    do = next(r for r in rows if r["parameter"] == "do" and r["region"] == "All")
    do["range_fail_max"] = None
    cfg = sr.load_defaults(rows)
    assert cfg["do"]["range"]["fail_max"] is None and cfg["do"]["range"]["suspect_max"] == 25
    assert cfg["qvalue"]["spike"] is None and cfg["ph"]["range"] is None


def _temp_defaults(client):
    series = client.get(f"/logger-review/api/submissions/{SID}").get_json()["submission"]["series"]
    temps = [s for k, s in series.items() if k.split("|")[-2] == "h2otemp"]
    assert temps
    return temps[0]["defaults"]


def test_api_uses_lookup_rows(store):  # noqa: F811
    assert _temp_defaults(make(store))["spike"] == {"suspect": 0.25, "fail": 0.5}   # JSON fallback
    rows = json_as_rows()
    t = next(r for r in rows if r["parameter"] == "h2otemp" and r["region"] == "All")
    t["spike_suspect"], t["spike_fail"] = 0.3, 0.6
    store.thresholds = rows
    assert _temp_defaults(make(store))["spike"] == {"suspect": 0.3, "fail": 0.6}
