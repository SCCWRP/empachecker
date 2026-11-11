import json
import shutil
import subprocess

import pytest

from proj.logger_review_lib import series as sr
from conftest import ROOT

NODE = shutil.which("node")


def test_payload_flags_equal_prototype_flags(payloads, python_flags):
    _, flags = payloads
    assert len(flags) == len(python_flags)
    assert flags == python_flags
    assert sum(len(v) for series in python_flags.values() for v in series.values()) == 10896


def test_payload_shape(payloads, rows_by_sid):
    data, _ = payloads
    assert {k: v["row_count"] for k, v in data.items()} == {1729879198: 232, 1770672364: 316, 1770678690: 360}
    d = data[1729879198]
    assert d["id"] == "1729879198" and d["start"] == "2024-04-01T00:00:00" and d["end"] == "2024-04-03T09:45:00"
    assert set(d) == {"id", "meta", "row_count", "start", "end", "series"}
    for key, s in d["series"].items():
        assert set(s) == {"parameter", "label", "unit", "sensor", "t", "timestamps", "zones", "objectids", "x", "interval",
                          "nsteps", "mult", "defaults", "known_units"}
        assert len(s["t"]) == len(s["x"]) == len(s["timestamps"]) == len(s["objectids"])
        assert key.count("|") == 5
    json.dumps(d, allow_nan=False)


def test_missing_value_is_null(payloads, rows_by_sid):
    data, _ = payloads
    for sid, d in data.items():
        by_id = {r["objectid"]: r for r in rows_by_sid[sid]}
        for s in d["series"].values():
            for oid, x in zip(s["objectids"], s["x"]):
                raw = by_id[oid]["raw_" + s["parameter"]]
                assert x == (None if raw in (None, -88) else raw)


def test_unknown_timezone_is_a_data_error(rows_by_sid):
    rows = [dict(r) for r in rows_by_sid[1729879198]]
    rows[0]["samplecollectiontimezone"] = "XYZ"
    with pytest.raises(sr.DataError):
        sr.build_submission(1729879198, rows, {})


@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_vendored_js_matches_python_flags(payloads, python_flags):
    data, _ = payloads
    script = r"""
const QC = require(process.argv[1]);
const input = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const out = {};
for (const [sid, sub] of Object.entries(input)) for (const [key, s] of Object.entries(sub.series)) {
  const series = {...s, x: Float64Array.from(s.x, v => v === null ? NaN : v)};
  const f = QC.run(series, s.defaults);
  out[sid + '|' + key] = {range: Array.from(f.range), spike: Array.from(f.spike), roc: Array.from(f.roc), flatline: Array.from(f.flatline)};
}
console.log(JSON.stringify(out));
"""
    res = subprocess.run([NODE, "-e", script, str(ROOT / "proj/static/logger_review/qc_tests.js")], input=json.dumps({str(k): v for k, v in data.items()}),
                         capture_output=True, text=True, check=True)
    assert json.loads(res.stdout) == python_flags


def test_js_matches_python_with_edited_thresholds(payloads):
    """Edited thresholds, including a half-empty flatline block, give the same flags in Python and JavaScript."""
    if NODE is None:
        pytest.skip("node not installed")
    data, _ = payloads
    sub = data[1770678690]
    cases = []
    for key, s in sub["series"].items():
        th = json.loads(json.dumps(s["defaults"]))
        th["spike"] = {"suspect": 0.01, "fail": 0.05}
        th["roc"] = {"suspect_sd": 1, "fail_sd": 1.5, "min_step": 0}
        th["flatline"] = {"suspect_n": 1, "fail_n": None}
        th["range"] = {"fail_min": None, "suspect_min": None, "suspect_max": 20, "fail_max": None}
        cases.append((key, s, th))
    script = r"""
const QC = require(process.argv[1]);
const input = JSON.parse(require('fs').readFileSync(0, 'utf8'));
console.log(JSON.stringify(input.map(([key, s, th]) => {
  const series = {...s, x: Float64Array.from(s.x, v => v === null ? NaN : v)};
  const f = QC.run(series, th); const any = QC.worst(f);
  return {range: Array.from(f.range), spike: Array.from(f.spike), roc: Array.from(f.roc), flatline: Array.from(f.flatline), any: Array.from(any)};
})));
"""
    res = subprocess.run([NODE, "-e", script, str(ROOT / "proj/static/logger_review/qc_tests.js")], input=json.dumps(cases),
                         capture_output=True, text=True, check=True)
    js = json.loads(res.stdout)
    series = {s.key: s for s in sr.build_series(sr.normalize_rows(_rows(sub)), sr.load_defaults())}
    for (key, s, th), got in zip(cases, js):
        f = sr.run_flags(series[key], th)
        for test in ("range", "spike", "roc", "flatline", "any"):
            assert list(map(int, f[test])) == got[test], (key, test)


def _rows(sub):
    import json as j
    fx = j.loads((ROOT / "tests/logger_review/fixtures/submission_review_fixture.json").read_text())
    return [r for r in fx["rows"] if r["submissionid"] == int(sub["id"])]
