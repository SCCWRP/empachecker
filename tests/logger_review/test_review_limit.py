"""The tool opens only REVIEW_SUBMISSION_IDS (Jan and Nicholas's 5 test submissions)."""
from fakes import FakeUnitStore
from proj import logger_review as lr
from test_api import SID, make, make_app, store, raw  # noqa: F401  (fixtures)
from test_unit_review import MEANS
import time


def limited(store, ids):  # noqa: F811
    store.thresholds = None
    app = make_app(store, LOGGER_REVIEW_SUBMISSION_IDS=ids)
    c = app.test_client()
    with c.session_transaction() as s:
        s["logger_review_email"] = "janw@sccwrp.org"
        s["logger_review_signed_at"] = time.time()
    return c


def test_default_is_the_five_test_submissions():
    assert len(lr.REVIEW_SUBMISSION_IDS) == 5 and len(set(lr.REVIEW_SUBMISSION_IDS)) == 5


def test_list_shows_only_open_submissions(store):  # noqa: F811
    c = limited(store, [SID, 1770672364])
    j = c.get("/logger-review/api/submissions").get_json()
    assert [i["submissionId"] for i in j["items"]] == [str(SID), "1770672364"] and j["total"] == 2


def test_other_submissions_are_not_found(store):  # noqa: F811
    c = limited(store, [1770672364])
    assert c.get(f"/logger-review/api/submissions/{SID}").status_code == 404
    assert c.put(f"/logger-review/api/submissions/{SID}/review", json={
        "notes": "", "removeFails": False, "removedPeriods": [], "keptPeriods": [], "thresholds": None}).status_code == 404
    assert c.post(f"/logger-review/api/submissions/{SID}/decision", json={"decision": "finalized"}).status_code == 404
    c = limited(store, [SID])
    assert c.get(f"/logger-review/api/submissions/{SID}").status_code == 200


def test_none_opens_everything(store):  # noqa: F811
    j = make(store).get("/logger-review/api/submissions").get_json()
    assert j["total"] > 2


def test_unit_review_lists_only_open_submissions():
    st = FakeUnitStore([], {}, means=MEANS)
    app = make_app(st, LOGGER_REVIEW_SYNC_REFRESH=True)
    c = app.test_client()
    with c.session_transaction() as s:
        s["logger_review_email"] = "duyn@sccwrp.org"
        s["logger_review_signed_at"] = time.time()
    c.post("/logger-review/api/unit-review/refresh")
    app.config["LOGGER_REVIEW_SUBMISSION_IDS"] = None
    assert {i["submissionId"] for i in c.get("/logger-review/api/unit-review").get_json()["items"]} == {"200", "201"}
    app.config["LOGGER_REVIEW_SUBMISSION_IDS"] = [200]
    assert {i["submissionId"] for i in c.get("/logger-review/api/unit-review").get_json()["items"]} == {"200"}
    other = next(r for r in st.units.values() if r["submissionid"] == 201)
    assert c.put(f"/logger-review/api/unit-review/{other['objectid']}", json={"correctUnit": None}).status_code == 404
    assert c.get("/logger-review/api/unit-review/submissions/201").status_code == 404
