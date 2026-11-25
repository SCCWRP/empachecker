"""Unit Review tab: flag logic, API and SQL."""
import re

import pytest

from fakes import FakeUnitStore
from proj import logger_review as lr
from proj.logger_review_lib import store as st
from proj.logger_review_lib import unit_review as ur
from test_api import A, B, make_app
import time


def series(sid, parameter, unit, mean, n=100, sensor="X1"):
    return {"submissionid": sid, "siteid": "CC-PAJ", "stationno": 1, "sensortype": "troll", "sensorid": sensor,
            "parameter": parameter, "unit": unit, "n": n, "mean": mean}


# depth in m: usual about 1, one series at 10.7 (feet labelled m), one at 0.05 (20x lower)
MEANS = [series(100 + i, "depth", "m", m) for i, m in enumerate([0.8, 0.9, 1.0, 1.1, 1.2])] + [
    series(200, "depth", "m", 10.7), series(201, "depth", "m", 0.05),
    series(200, "pressure", "mbar", 1040.0), series(300, "pressure", "mbar", 1050.0), series(301, "pressure", "mbar", 1060.0)]


def client(means=MEANS, who=A):
    store = FakeUnitStore([], {}, means=means)
    app = make_app(store, LOGGER_REVIEW_SYNC_REFRESH=True)
    c = app.test_client()
    if who:
        with c.session_transaction() as s:
            s["logger_review_email"] = who
            s["logger_review_signed_at"] = time.time()
    return c, store


@pytest.fixture(autouse=True)
def reset_refresh_state():
    lr._unit_refresh.update(running=False, startedAt=None, finishedAt=None, error=None, flagged=None)
    yield


# ---- flag logic

def test_flags_ten_times_above_or_below_the_usual_mean():
    flagged = {(f["submissionid"], f["parameter"]): f for f in ur.flag_series(MEANS)}
    assert set(flagged) == {(200, "depth"), (201, "depth")}
    assert flagged[(200, "depth")]["usual_mean"] == pytest.approx(1.0)
    assert flagged[(200, "depth")]["times_off"] == pytest.approx(10.7)
    assert flagged[(201, "depth")]["times_off"] == pytest.approx(20.0)


def test_usual_mean_is_per_parameter_and_unit():
    rows = [series(1, "depth", "m", 1.0), series(2, "depth", "m", 1.0), series(3, "depth", "cm", 100.0), series(4, "depth", "cm", 100.0)]
    assert ur.flag_series(rows) == []


def test_not_comparable_values_are_never_flagged():
    assert ur.times_off(5.0, 0.0) is None and ur.times_off(None, 1.0) is None and ur.times_off(float("nan"), 1.0) is None
    assert ur.times_off(-1.0, 1.0) == float("inf")


def test_series_key_is_stable():
    assert ur.series_key(series(5, "do", "mg/L", 1.0)) == "5|CC-PAJ|1|troll|X1|do|mg/L"


# ---- API

def test_refresh_then_list_then_save():
    c, store = client()
    r = c.post("/logger-review/api/unit-review/refresh")
    assert r.status_code == 202
    data = c.get("/logger-review/api/unit-review").get_json()
    assert data["refresh"]["running"] is False and data["refresh"]["flagged"] == 2
    assert [(i["submissionId"], i["parameter"], i["recordedUnit"]) for i in data["items"]] == [("201", "depth", "m"), ("200", "depth", "m")]
    assert data["options"]["depth"] == ["m", "ft", "cm"] and data["factor"] == 10
    item = next(i for i in data["items"] if i["submissionId"] == "200")
    saved = c.put(f"/logger-review/api/unit-review/{item['id']}", json={"correctUnit": "ft"}).get_json()["item"]
    assert saved["correctUnit"] == "ft" and saved["reviewerEmail"] == A and saved["reviewedDate"]
    cleared = c.put(f"/logger-review/api/unit-review/{item['id']}", json={"correctUnit": None}).get_json()["item"]
    assert cleared["correctUnit"] is None and cleared["reviewerEmail"] is None


def test_second_refresh_keeps_recorded_units_and_unflags_fixed_series():
    c, store = client()
    c.post("/logger-review/api/unit-review/refresh")
    items = c.get("/logger-review/api/unit-review").get_json()["items"]
    reviewed = next(i for i in items if i["submissionId"] == "200")
    c.put(f"/logger-review/api/unit-review/{reviewed['id']}", json={"correctUnit": "ft"})
    # submission 201 resubmitted with sensible values, 200 still off
    store.means = [m if m["submissionid"] != 201 else {**m, "mean": 1.0} for m in MEANS]
    time.sleep(0.01)
    c.post("/logger-review/api/unit-review/refresh")
    items = c.get("/logger-review/api/unit-review").get_json()["items"]
    assert [(i["submissionId"], i["correctUnit"]) for i in items] == [("200", "ft")]
    # an unflagged series that already has a unit stays listed
    store.means = [m if (m["submissionid"], m["parameter"]) != (200, "depth") else {**m, "mean": 1.0} for m in store.means]
    time.sleep(0.01)
    c.post("/logger-review/api/unit-review/refresh")
    items = c.get("/logger-review/api/unit-review").get_json()["items"]
    assert [(i["submissionId"], i["correctUnit"], i["flagged"]) for i in items] == [("200", "ft", False)]


@pytest.mark.parametrize("body", [{"correctUnit": "furlongs"}, {"correctUnit": 5}, {"correctUnit": "mbar"}])
def test_save_rejects_units_not_allowed_for_the_parameter(body):
    c, store = client()
    c.post("/logger-review/api/unit-review/refresh")
    item = c.get("/logger-review/api/unit-review").get_json()["items"][0]
    r = c.put(f"/logger-review/api/unit-review/{item['id']}", json=body)
    assert r.status_code == 400
    assert not [w for w in store.writes if w[0] == "unit"]


def test_save_unknown_series_is_404():
    c, _ = client()
    assert c.put("/logger-review/api/unit-review/999", json={"correctUnit": "m"}).status_code == 404


def test_unit_endpoints_need_a_sign_in():
    c, store = client(who=None)
    assert c.get("/logger-review/api/unit-review").status_code == 401
    assert c.post("/logger-review/api/unit-review/refresh").status_code == 401
    assert c.put("/logger-review/api/unit-review/1", json={"correctUnit": "m"}).status_code == 401
    assert store.writes == []


def test_missing_table_is_reported():
    c, store = client()
    store.missing_tables = True
    assert c.get("/logger-review/api/unit-review").status_code == 503
    c.post("/logger-review/api/unit-review/refresh")
    assert "does not exist" in lr._unit_refresh["error"]


def test_refresh_does_not_start_twice():
    c, _ = client()
    lr._unit_refresh["running"] = True
    assert c.post("/logger-review/api/unit-review/refresh").get_json()["started"] is False


# ---- submission view and confirm

def unit_client(raw, sites, sid, who=A):
    """A store holding the fixture rows of sid, with every plotted series of the first sensor flagged."""
    from proj.logger_review_lib import series as sr
    rows = raw[sid]
    payload = sr.build_submission(sid, rows, sites)
    store = FakeUnitStore([], raw, sites)
    first = rows[0]
    for n, (key, s) in enumerate(list(payload["series"].items())[:2], start=1):
        store.units[n] = {"objectid": n, "series_key": key, "submissionid": sid, "siteid": first["siteid"],
                          "stationno": first["stationno"], "sensortype": first["sensortype"], "sensorid": first["sensorid"],
                          "parameter": s["parameter"], "recorded_unit": s["unit"], "n_values": 5, "series_mean": 10.0,
                          "usual_mean": 1.0, "ratio": 10.0, "flagged": True, "last_checked_date": "2026-11-17T00:00:00+00:00",
                          "correct_unit": None, "reviewer_email": None, "reviewed_date": None}
    store.OPTIONS = {p: [u, "other"] for p, u in {n["parameter"]: n["recorded_unit"] for n in store.units.values()}.items()}
    app = make_app(store)
    c = app.test_client()
    if who:
        with c.session_transaction() as s:
            s["logger_review_email"] = who
            s["logger_review_signed_at"] = time.time()
    return c, store, payload


SID = 1770678690


def test_unit_submission_returns_payload_items_and_series_keys(rows_by_sid, fixture):
    c, store, payload = unit_client(rows_by_sid, fixture["sites"], SID)
    data = c.get(f"/logger-review/api/unit-review/submissions/{SID}").get_json()
    assert data["submission"]["id"] == str(SID) and set(data["submission"]["series"]) == set(payload["series"])
    assert len(data["items"]) == 2
    for item in data["items"]:
        assert item["seriesKey"] in data["submission"]["series"]
        s = data["submission"]["series"][item["seriesKey"]]
        assert (s["parameter"], s["unit"]) == (item["parameter"], item["recordedUnit"])


def test_unit_submission_key_matches_when_station_types_differ(rows_by_sid, fixture):
    c, store, _ = unit_client(rows_by_sid, fixture["sites"], SID)
    for u in store.units.values():
        u["stationno"] = float(u["stationno"]) if str(u["stationno"]).replace(".", "").isdigit() else u["stationno"]
    items = c.get(f"/logger-review/api/unit-review/submissions/{SID}").get_json()["items"]
    assert all(i["seriesKey"] for i in items)


def test_unit_submission_unknown_is_404_and_signed_out_is_401(rows_by_sid, fixture):
    c, store, _ = unit_client(rows_by_sid, fixture["sites"], SID)
    assert c.get("/logger-review/api/unit-review/submissions/12345").status_code == 404
    assert c.get("/logger-review/api/unit-review/submissions/abc").status_code == 400
    out, _, _ = unit_client(rows_by_sid, fixture["sites"], SID, who=None)
    assert out.get(f"/logger-review/api/unit-review/submissions/{SID}").status_code == 401
    assert out.put(f"/logger-review/api/unit-review/submissions/{SID}/confirm", json={"units": {}}).status_code == 401


def test_unit_submission_works_before_2023(rows_by_sid, fixture):
    old = {SID: [{**r, "samplecollectiontimestamp": "2019" + str(r["samplecollectiontimestamp"])[4:]} for r in rows_by_sid[SID]]}
    c, store, _ = unit_client(old, fixture["sites"], SID)
    assert c.get(f"/logger-review/api/submissions/{SID}").status_code == 404      # Expert Review rule
    r = c.get(f"/logger-review/api/unit-review/submissions/{SID}")
    assert r.status_code == 200 and len(r.get_json()["items"]) == 2


def test_confirm_saves_every_series_in_one_write(rows_by_sid, fixture):
    c, store, _ = unit_client(rows_by_sid, fixture["sites"], SID)
    units = {str(o): r["recorded_unit"] for o, r in store.units.items()}
    units["1"] = "other"
    r = c.put(f"/logger-review/api/unit-review/submissions/{SID}/confirm", json={"units": units})
    assert r.status_code == 200
    items = r.get_json()["items"]
    assert [i["correctUnit"] for i in items] == [units[str(i["id"])] for i in items]
    assert all(i["reviewerEmail"] == A and i["reviewedDate"] for i in items)
    assert [w[0] for w in store.writes] == ["units"] and len(store.writes[0][1]) == 2
    again = c.put(f"/logger-review/api/unit-review/submissions/{SID}/confirm", json={"units": {**units, "1": store.units[1]["recorded_unit"]}})
    assert again.status_code == 200 and store.units[1]["correct_unit"] == store.units[1]["recorded_unit"]


@pytest.mark.parametrize("mutate", [
    lambda u: u.pop("1"), lambda u: u.update({"1": ""}), lambda u: u.update({"1": "furlongs"}),
    lambda u: u.update({"1": 5}), lambda u: u.update({"999": "m"}), lambda u: u.clear()])
def test_confirm_rejects_missing_or_invalid_units_without_writing(rows_by_sid, fixture, mutate):
    c, store, _ = unit_client(rows_by_sid, fixture["sites"], SID)
    units = {str(o): r["recorded_unit"] for o, r in store.units.items()}
    mutate(units)
    r = c.put(f"/logger-review/api/unit-review/submissions/{SID}/confirm", json={"units": units})
    assert r.status_code == 400
    assert store.writes == [] and all(u["correct_unit"] is None for u in store.units.values())


def test_confirm_bad_body_and_unknown_submission(rows_by_sid, fixture):
    c, store, _ = unit_client(rows_by_sid, fixture["sites"], SID)
    assert c.put(f"/logger-review/api/unit-review/submissions/{SID}/confirm", json={"units": []}).status_code == 400
    assert c.put("/logger-review/api/unit-review/submissions/12345/confirm", json={"units": {}}).status_code == 404
    assert store.writes == []


# ---- SQL

UNIT_TABLE = "sde.tbl_wq_logger_unit_review"


@pytest.mark.parametrize("sql", st.UNIT_WRITE_SQL)
def test_unit_write_sql_touches_only_the_unit_table(sql):
    assert set(re.findall(r"\bsde\.[a-z_]+", sql)) == {UNIT_TABLE}
    assert not re.search(r"\b(DROP|ALTER|CREATE|TRUNCATE|GRANT|DELETE)\b", sql)
    assert re.search(r":\w+", sql)


def test_refresh_never_overwrites_recorded_units():
    upsert = st.SQL_UPSERT_UNIT_FLAG.split("DO UPDATE SET", 1)[1]
    for col in ("correct_unit", "reviewer_email", "reviewed_date"):
        assert col not in upsert
    assert "correct_unit" not in st.SQL_UNFLAG_UNIT


def test_unit_save_is_scoped_to_one_row():
    assert st.SQL_SAVE_UNIT.strip().endswith("WHERE objectid = :objectid")


def test_confirm_uses_the_scoped_save_statement():
    import inspect
    src = inspect.getsource(st.Store.save_units)
    assert "SQL_SAVE_UNIT" in src and "SELECT" not in src and "DELETE" not in src
    assert "WHERE objectid = :objectid" in st.SQL_SAVE_UNIT and st.SQL_SAVE_UNIT in st.UNIT_WRITE_SQL


def test_series_means_sql_only_reads_raw():
    sql = ur.series_means_sql(st.RAW)
    assert re.match(r"\s*SELECT", sql) and not re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER)\b", sql)
    assert "<> -88" in sql and "'NaN'::float8" in sql
