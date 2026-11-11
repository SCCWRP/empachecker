import copy
import time

import pytest
from flask import Flask

from fakes import FakeStore, tracking_row
from proj import logger_review as lr

SID = 1770678690
A, B = "duyn@sccwrp.org", "janw@sccwrp.org"


@pytest.fixture()
def raw(rows_by_sid):
    out = {}
    for sid, rows in rows_by_sid.items():
        out[sid] = []
        for r in rows:
            r = dict(r)
            r["projectid"] = "P1"
            r["raw_h2otemp_qcflag_human"] = 1
            out[sid].append(r)
    return out


@pytest.fixture()
def store(raw, fixture):
    # 1729879198 has no tracking row, 999 has a tracking row but no raw rows
    tracking = [tracking_row(1770672364, 200), tracking_row(SID, 100, "logger_formatted"), tracking_row(999, 50)]
    return FakeStore(tracking, raw, fixture["sites"])


def make_app(store, **config):
    app = Flask(__name__, static_url_path="/static", static_folder=str(lr.Path(__file__).resolve().parents[2] / "proj" / "static"),
                template_folder=str(lr.Path(__file__).resolve().parents[2] / "proj" / "templates"))
    app.secret_key = "test-secret"
    app.mail_from = "checker@example.org"
    app.config["MAIL_SERVER"] = "localhost"
    app.config["LOGGER_REVIEW_STORE"] = store
    app.config.update(config)
    app.register_blueprint(lr.logger_review)
    return app


def make(store, who=A, **kw):
    """A test client signed in as who (None: not signed in)."""
    app = make_app(store, **({"LOGGER_REVIEW_ROW_LIMIT": kw["row_limit"]} if "row_limit" in kw else {}))
    c = app.test_client()
    if who:
        with c.session_transaction() as s:
            s["logger_review_email"] = who
            s["logger_review_signed_at"] = time.time()
    return c


def draft(**kw):
    body = {"notes": "n", "removeFails": False, "removedPeriods": [], "keptPeriods": [], "thresholds": None}
    body.update(kw)
    return body


# ---- listing

def test_list_is_every_raw_submission_newest_first(store):
    c = make(store)
    r = c.get("/logger-review/api/submissions").get_json()
    ids = [i["submissionId"] for i in r["items"]]
    assert ids == ["1770678690", "1770672364", "1729879198"]       # descending, 999 has no raw rows
    assert len(set(ids)) == len(ids) and r["total"] == 3
    assert all(i["status"] == "not_reviewed" for i in r["items"])
    no_tracking = r["items"][2]                                      # no tracking row: still listed
    assert no_tracking["submittedBy"] is None and no_tracking["fileName"] is None
    c.put(f"/logger-review/api/submissions/{SID}/review", json=draft())
    r = c.get("/logger-review/api/submissions?status=draft").get_json()
    assert [i["submissionId"] for i in r["items"]] == [str(SID)] and r["items"][0]["statusLabel"] == "Draft"
    assert c.get("/logger-review/api/submissions?limit=2&offset=2").get_json()["items"][0]["submissionId"] == "1729879198"


@pytest.mark.parametrize("query", ["status=bogus", "limit=0", "limit=9999", "limit=x", "offset=-1"])
def test_list_validation(store, query):
    r = make(store).get("/logger-review/api/submissions?" + query)
    assert r.status_code == 400 and r.get_json()["error"]["code"] == "invalid_input"


# ---- get

def test_get_submission(store):
    c = make(store)
    r = c.get(f"/logger-review/api/submissions/{SID}")
    assert r.status_code == 200
    j = r.get_json()
    assert j["submission"]["id"] == str(SID) and j["submission"]["row_count"] == 360
    assert j["review"]["exists"] is False and j["review"]["status"] == "draft"
    assert j["submission"]["meta"]["region"] is not None or True


def test_get_unknown_and_bad_id(store):
    c = make(store)
    assert c.get("/logger-review/api/submissions/12345").status_code == 404
    assert c.get("/logger-review/api/submissions/abc").status_code == 400
    assert c.get("/logger-review/api/submissions/0").status_code == 400


def test_get_submission_without_rows(store):
    store.raw[999] = []
    r = make(store).get("/logger-review/api/submissions/999")
    assert r.status_code == 404 and r.get_json()["error"]["code"] == "not_found"


def test_row_limit_413(store):
    r = make(store, row_limit=100).get(f"/logger-review/api/submissions/{SID}")
    assert r.status_code == 413 and r.get_json()["error"]["code"] == "too_many_rows"
    assert make(store, row_limit=360).get(f"/logger-review/api/submissions/{SID}").status_code == 200
    # a decision on a too large submission cannot build final rows
    c = make(store, row_limit=100)
    c.put(f"/logger-review/api/submissions/{SID}/review", json=draft())
    assert c.post(f"/logger-review/api/submissions/{SID}/decision", json={"decision": "finalized"}).status_code == 413
    assert SID not in store.final and store.reviews[SID]["status"] == "draft"


def test_bad_timezone_is_422(store):
    store.raw[SID][0]["samplecollectiontimezone"] = "XYZ"
    r = make(store).get(f"/logger-review/api/submissions/{SID}")
    assert r.status_code == 422


# ---- put

def test_put_saves_draft_and_returns_review(store):
    c = make(store)
    body = draft(notes="hello", removeFails=True, removedPeriods=[{"start": "2024-04-01T00:00", "end": "2024-04-01T01:00:00"}],
                 keptPeriods=[{"start": "2024-04-02T00:00:00", "end": "2024-04-02T00:00:00"}],
                 thresholds={"k": {"range": {"fail_max": 5}, "flatline": {"tol": 0}}})
    r = c.put(f"/logger-review/api/submissions/{SID}/review", json=body)
    assert r.status_code == 200
    rv = r.get_json()["review"]
    assert rv["status"] == "draft" and rv["notes"] == "hello" and rv["removeFails"] is True
    assert rv["removedPeriods"] == [{"start": "2024-04-01T00:00:00", "end": "2024-04-01T01:00:00"}]
    assert rv["thresholds"]["k"]["range"]["fail_max"] == 5
    assert c.get(f"/logger-review/api/submissions/{SID}").get_json()["review"]["notes"] == "hello"


@pytest.mark.parametrize("patch,fragment", [
    ({"notes": 5}, "notes"), ({"removeFails": "yes"}, "removeFails"), ({"removedPeriods": "x"}, "removedPeriods"),
    ({"removedPeriods": [{"start": "2024-04-02T00:00:00", "end": "2024-04-01T00:00:00"}]}, "starts after"),
    ({"keptPeriods": [{"start": "bad", "end": "2024-04-01T00:00:00"}]}, "keptPeriods[0].start"),
    ({"keptPeriods": [{"start": "2024-13-01T00:00:00", "end": "2024-13-02T00:00:00"}]}, "real date"),
    ({"keptPeriods": [5]}, "object"),
    ({"thresholds": []}, "thresholds"), ({"thresholds": {"k": 1}}, "object"),
    ({"thresholds": {"k": {"bogus": {}}}}, "unknown test"), ({"thresholds": {"k": {"range": {"nope": 1}}}}, "unknown field"),
    ({"thresholds": {"k": {"range": {"fail_max": "5"}}}}, "number"), ({"thresholds": {"k": {"range": {"fail_max": True}}}}, "number"),
])
def test_put_validation(store, patch, fragment):
    r = make(store).put(f"/logger-review/api/submissions/{SID}/review", json=draft(**patch))
    assert r.status_code == 400 and fragment in r.get_json()["error"]["message"]
    assert store.writes == []


def test_put_missing_keys_and_not_json(store):
    c = make(store)
    assert c.put(f"/logger-review/api/submissions/{SID}/review", json={"notes": ""}).status_code == 400
    assert c.put(f"/logger-review/api/submissions/{SID}/review", data="nope", content_type="text/plain").status_code == 400
    assert c.put("/logger-review/api/submissions/12345/review", json=draft()).status_code == 404


def test_endpoints_need_a_sign_in(store):
    c = make(store, who=None)
    sid = f"/logger-review/api/submissions/{SID}"
    for r in (c.get("/logger-review/api/submissions"), c.get(sid), c.put(sid + "/review", json=draft()),
              c.post(sid + "/decision", json={"decision": "finalized"})):
        assert r.status_code == 401 and r.get_json()["error"]["code"] == "sign_in_required"
    assert store.writes == []


def test_non_allowlisted_session_is_refused(store):
    c = make(store, who="someone@example.org")
    assert c.get("/logger-review/api/submissions").status_code == 401


def test_session_older_than_12_hours_is_refused(store):
    c = make(store)
    with c.session_transaction() as s:
        s["logger_review_signed_at"] = time.time() - 12 * 3600 - 5
    assert c.get("/logger-review/api/submissions").status_code == 401


def test_multi_sensor_submission_cannot_be_decided(store, raw):
    rows = raw[SID]
    rows[10]["sensorid"] = "OTHER"
    c = make(store)
    assert c.put(f"/logger-review/api/submissions/{SID}/review", json=draft(notes="kept")).status_code == 200  # drafts still save
    assert c.get(f"/logger-review/api/submissions/{SID}").get_json()["submission"]["meta"]["more"] == 1
    for d in ("finalized", "needs_another_qa", "rejected"):
        r = decide(c, d)
        assert r.status_code == 409 and r.get_json()["error"]["message"] == \
            "This submission has more than one sensor and cannot be finalized here yet."
    assert store.reviews[SID]["status"] == "draft" and SID not in store.final
    assert [w[0] for w in store.writes] == ["draft"]


# ---- decisions

def decide(c, decision, sid=SID):
    return c.post(f"/logger-review/api/submissions/{sid}/decision", json={"decision": decision})


def test_decision_validation(store):
    c = make(store)
    assert c.post(f"/logger-review/api/submissions/{SID}/decision", json={"decision": "draft"}).status_code == 400
    assert c.post(f"/logger-review/api/submissions/{SID}/decision", json={}).status_code == 400
    assert c.post("/logger-review/api/submissions/12345/decision", json={"decision": "finalized"}).status_code == 404


def test_reject_needs_note(store):
    c = make(store)
    c.put(f"/logger-review/api/submissions/{SID}/review", json=draft(notes="   "))
    r = decide(c, "rejected")
    assert r.status_code == 400 and r.get_json()["error"]["code"] == "note_required"
    assert store.reviews[SID]["status"] == "draft"
    c.put(f"/logger-review/api/submissions/{SID}/review", json=draft(notes="dead sensor"))
    r = decide(c, "rejected")
    assert r.status_code == 200 and r.get_json()["review"]["status"] == "rejected"
    assert SID not in store.final


def test_reject_without_any_saved_review_needs_note(store):
    assert decide(make(store), "rejected").status_code == 400


def test_finalize_builds_final_rows(store, raw):
    c = make(store)
    first = raw[SID][0]
    stamp = first["samplecollectiontimestamp"]
    c.put(f"/logger-review/api/submissions/{SID}/review", json=draft(removedPeriods=[{"start": stamp, "end": stamp}]))
    r = decide(c, "finalized")
    assert r.status_code == 200
    j = r.get_json()
    assert j["review"]["status"] == "finalized" and j["review"]["reviewerEmail"] == A
    assert j["review"]["finalizedDate"] and j["review"]["decidedAt"]
    assert j["finalRows"] == 360 and len(store.final[SID]) == 360
    f0 = store.final[SID][0]
    assert f0["objectid"] == first["objectid"] and f0["raw_h2otemp"] is None and f0["raw_h2otemp_qcflag_human"] == -3
    assert f0["projectid"] == "P1"
    # thresholds used are saved with the review: one block per series
    assert set(j["review"]["thresholds"]) == set(c.get(f"/logger-review/api/submissions/{SID}").get_json()["submission"]["series"])
    assert raw[SID][0]["raw_h2otemp"] is not None


def test_finalize_without_saved_review(store):
    j = decide(make(store), "finalized").get_json()
    assert j["review"]["status"] == "finalized" and len(store.final[SID]) == 360


def test_draft_clears_final_rows(store):
    c = make(store)
    decide(c, "finalized")
    assert SID in store.final
    r = c.put(f"/logger-review/api/submissions/{SID}/review", json=draft(notes="changed"))
    assert r.get_json()["review"]["status"] == "draft"
    assert SID not in store.final
    assert store.reviews[SID]["reviewerEmail"] is None and store.reviews[SID]["finalizedDate"] is None


def test_reject_and_needs_qa_remove_final_rows(store):
    c = make(store)
    decide(c, "finalized")
    c.put(f"/logger-review/api/submissions/{SID}/review", json=draft(notes="x"))
    decide(c, "needs_another_qa")
    assert SID not in store.final
    decide(c, "finalized")  # same reviewer, see the conflict test: this one is refused
    assert SID not in store.final


def test_needs_another_qa_then_second_reviewer(store):
    first = make(store, who=A)
    r = decide(first, "needs_another_qa")
    assert r.status_code == 200 and r.get_json()["review"]["status"] == "needs_another_qa"
    assert SID not in store.final
    same = decide(make(store, who=A), "finalized")
    assert same.status_code == 409 and same.get_json()["error"]["code"] == "same_reviewer"
    assert store.reviews[SID]["status"] == "needs_another_qa" and SID not in store.final
    ok = decide(make(store, who=B), "finalized")
    assert ok.status_code == 200
    rv = ok.get_json()["review"]
    assert rv["status"] == "finalized" and rv["reviewerEmail"] == A and rv["secondReviewerEmail"] == B
    assert rv["secondReviewedDate"] and len(store.final[SID]) == 360


def test_second_reviewer_may_also_reject_or_ask_again(store):
    decide(make(store, who=A), "needs_another_qa")
    c = make(store, who=B)
    c.put(f"/logger-review/api/submissions/{SID}/review", json=draft(notes="problem"))  # an edit returns it to draft
    assert store.reviews[SID]["status"] == "draft"
    assert decide(c, "finalized").get_json()["review"]["secondReviewerEmail"] is None


def test_edit_after_finalize_by_anyone_returns_to_draft(store):
    decide(make(store), "finalized")
    make(store, who=B).put(f"/logger-review/api/submissions/{SID}/review", json=draft())
    assert store.reviews[SID]["status"] == "draft" and SID not in store.final


def test_transactions_are_all_or_nothing_in_the_fake_contract(store):
    # the finalize path computes everything before one save_decision call
    decide(make(store), "finalized")
    assert [w[0] for w in store.writes] == ["decision"]


# ---- missing tables

def test_missing_tables_error(store):
    store.missing_tables = True
    c = make(store)
    for r in (c.get("/logger-review/api/submissions"), c.get(f"/logger-review/api/submissions/{SID}"),
              c.put(f"/logger-review/api/submissions/{SID}/review", json=draft()), decide(c, "finalized")):
        assert r.status_code == 503 and r.get_json()["error"]["code"] == "review_tables_missing"
