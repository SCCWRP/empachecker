import json
import shutil
import subprocess

import pytest

from proj.logger_review_lib import removal, series as sr
from conftest import ROOT

NODE = shutil.which("node")
SID = 1770678690


def test_period_inclusive_both_ends():
    stamps = ["2024-04-01T00:00:00", "2024-04-01T00:15:00", "2024-04-01T00:30:00", "2024-04-01T00:45:00"]
    period = [{"start": "2024-04-01T00:15:00", "end": "2024-04-01T00:30:00"}]
    got = removal.removal_flags(stamps, [1, 1, 1, 1], False, period, [])
    assert got == [False, True, True, False]


def test_remove_fails_good_period_and_precedence():
    stamps = ["2024-04-01T00:00:00", "2024-04-01T00:15:00", "2024-04-01T00:30:00", "2024-04-01T00:45:00"]
    flags = [4, 4, 3, 4]
    good = [{"start": "2024-04-01T00:15:00", "end": "2024-04-01T00:30:00"}]
    # fails removed, except inside the good period. Suspect is never removed by the switch.
    assert removal.removal_flags(stamps, flags, True, [], good) == [True, False, False, True]
    # switch off: only periods count
    assert removal.removal_flags(stamps, flags, False, [], good) == [False] * 4
    # a removal period beats a good period
    both = removal.removal_flags(stamps, flags, False, [{"start": "2024-04-01T00:15:00", "end": "2024-04-01T00:15:00"}], good)
    assert both == [False, True, False, False]


def _cases(rows):
    stamps = sorted({r["samplecollectiontimestamp"] for r in rows})
    a, b, c, d = stamps[3], stamps[40], stamps[41], stamps[90]
    return {
        "remove_fails_only": dict(removeFails=True, removedPeriods=[], keptPeriods=[], thresholds=None),
        "removal_period": dict(removeFails=False, removedPeriods=[{"start": a, "end": b}], keptPeriods=[], thresholds=None),
        "good_beats_fails": dict(removeFails=True, removedPeriods=[], keptPeriods=[{"start": a, "end": d}], thresholds=None),
        "removal_beats_good": dict(removeFails=True, removedPeriods=[{"start": b, "end": c}], keptPeriods=[{"start": a, "end": d}], thresholds=None),
    }


def _tight(series_list):
    """Thresholds that make plenty of fails: tiny range and spike limits."""
    out = {}
    for s in series_list:
        th = json.loads(json.dumps(s.defaults))
        th["range"] = {"fail_min": None, "suspect_min": None, "suspect_max": None, "fail_max": float(s.x[~(s.x != s.x)].mean())}
        th["spike"] = {"suspect": 1e-6, "fail": 2e-6}
        out[s.key] = th
    return out


def python_removed_by_series(rows, case, cfg):
    removed = removal.removed_readings(rows, case["thresholds"], case["removeFails"], case["removedPeriods"], case["keptPeriods"], cfg)
    out = {}
    for s in sr.build_series(sr.normalize_rows(rows), cfg):
        out[s.key] = [(r["objectid"], s.parameter) in removed for r in s.rows]
    return out


@pytest.mark.skipif(NODE is None, reason="node not installed")
@pytest.mark.parametrize("name", ["remove_fails_only", "removal_period", "good_beats_fails", "removal_beats_good"])
@pytest.mark.parametrize("tight", [False, True])
def test_python_removal_equals_page_javascript(name, tight, rows_by_sid, payload_dir, tmp_path):
    rows = rows_by_sid[SID]
    cfg = sr.load_defaults()
    case = _cases(rows)[name]
    if tight:
        case["thresholds"] = _tight(sr.build_series(sr.normalize_rows(rows), cfg))
    path = tmp_path / "case.json"
    path.write_text(json.dumps(case))
    res = subprocess.run([NODE, str(ROOT / "tests/logger_review/removal_dump.js"), str(payload_dir), str(SID), str(path)],
                         capture_output=True, text=True, check=True)
    js = json.loads(res.stdout)
    py = python_removed_by_series(rows, case, cfg)
    assert js == py
    assert any(any(v) for v in py.values()), "the case should remove something"


def test_missing_readings_are_not_removed(rows_by_sid):
    rows = [dict(r) for r in rows_by_sid[SID]]
    p = "raw_h2otemp"
    rows[5][p] = -88
    stamp = rows[5]["samplecollectiontimestamp"]
    gone = removal.removed_readings(rows, None, False, [{"start": stamp, "end": stamp}], [])
    assert (rows[5]["objectid"], "h2otemp") not in gone


def test_final_rows(rows_by_sid):
    rows = rows_by_sid[SID]
    removed = {(rows[0]["objectid"], "h2otemp"), (rows[0]["objectid"], "salinity"), (rows[7]["objectid"], "do")}
    final = removal.build_final_rows(rows, removed)
    assert len(final) == len(rows)
    assert final is not rows and rows[0]["raw_h2otemp"] is not None  # input untouched
    for r, f in zip(rows, final):
        for key in r:
            if key in {"raw_" + p for _, p in removed} | {"raw_" + p + "_qcflag_human" for _, p in removed} and (r["objectid"], key.replace("raw_", "").replace("_qcflag_human", "")) in removed:
                continue
            assert f[key] == r[key], key
    assert final[0]["raw_h2otemp"] is None and final[0]["raw_h2otemp_qcflag_human"] == -3
    assert final[0]["raw_salinity"] is None and final[0]["raw_salinity_qcflag_human"] == -3
    assert final[7]["raw_do"] is None and final[7]["raw_do_qcflag_human"] == -3
    assert final[0]["raw_do"] == rows[0]["raw_do"]


def test_final_rows_with_nothing_removed_equal_raw(rows_by_sid):
    rows = rows_by_sid[SID]
    assert removal.build_final_rows(rows, set()) == rows
