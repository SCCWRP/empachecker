"""EMPA logger expert review tool at /logger-review.

A reviewer opens a logger submission, checks the automatic QC flags, edits thresholds, removes or
marks periods, and finalizes, asks for another QA, or rejects. Sign-in is an allowlisted email plus
a shared password whose PBKDF2 hash is LOGGER_REVIEW_PASSWORD_HASH in proj/config/config.json. Review writes touch only sde.tbl_wq_logger_review, sde.tbl_wq_logger_review_period
and sde.tbl_wq_logger_final (see logger_review_lib/store.py). Raw data is never edited.
"""
import hashlib
import hmac
import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, current_app, g, jsonify, redirect, render_template, request, session, url_for

from .logger_review_lib import removal, validation
from .logger_review_lib import series as sr
from .logger_review_lib.store import ReviewTablesMissing, Store
from .logger_review_lib.validation import ValidationError

logger_review = Blueprint("logger_review", __name__)

# Only these reviewers may sign in. Compared lowercased and trimmed.
ALLOWED_EMAILS = ("duyn@sccwrp.org", "janw@sccwrp.org")

MAX_TRIES = 5                 # wrong passwords per email before a lockout
LOCK_SECONDS = 15 * 60        # how long that lockout lasts
SESSION_SECONDS = 12 * 60 * 60
DEFAULT_ROW_LIMIT = 100000
MAX_PAGE = 1000
MULTI_SENSOR_MESSAGE = "This submission has more than one sensor and cannot be finalized here yet."
STATUS_LABEL = {"not_reviewed": "Not reviewed", "draft": "Draft", "needs_another_qa": "Needs another QA",
                "finalized": "Finalized", "rejected": "Rejected"}
EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Wrong-password counts live in this process (the app runs as one uwsgi process), not the cookie.
_lock = threading.Lock()
_failures = {}     # email -> {"count", "until"}
LIST_CACHE_SECONDS = 5 * 60
_ids_lock = threading.Lock()
_ids_cache = {"at": 0.0, "ids": []}   # submission ids in the review list, newest first (per worker process)


class ApiError(Exception):
    def __init__(self, status, code, message):
        self.status, self.code, self.message = status, code, message


@logger_review.errorhandler(ApiError)
def api_error(e):
    return jsonify({"error": {"code": e.code, "message": e.message}}), e.status


@logger_review.errorhandler(ValidationError)
def validation_error(e):
    return jsonify({"error": {"code": "invalid_input", "message": str(e)}}), 400


@logger_review.errorhandler(ReviewTablesMissing)
def tables_missing(_):
    return jsonify({"error": {"code": "review_tables_missing",
                              "message": "The review tables do not exist in the database yet."}}), 503


@logger_review.teardown_request
def release_engine(_exc):
    eng = g.get("eng")
    try:
        if eng is not None:
            eng.dispose()
    except Exception:
        pass


# ---- sign-in

def clean_email(value):
    return value.strip().lower() if isinstance(value, str) else ""


def password_ok(password):
    """Check against "pbkdf2_sha256$<iterations>$<salt hex>$<hash hex>" from config.json."""
    stored = current_app.config.get("LOGGER_REVIEW_PASSWORD_HASH")
    if not isinstance(stored, str) or stored.count("$") != 3:
        raise ApiError(503, "password_not_configured", "Password sign-in is not set up on this server.")
    algo, iterations, salt, digest = stored.split("$")
    if algo != "pbkdf2_sha256":
        raise ApiError(503, "password_not_configured", "Password sign-in is not set up on this server.")
    got = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), int(iterations)).hex()
    return hmac.compare_digest(got, digest)


def json_body():
    body = request.get_json(silent=True)
    return body if isinstance(body, dict) else {}


def current_reviewer():
    """The signed-in reviewer email, or None. Sessions end after 12 hours."""
    email = session.get("logger_review_email")
    signed = session.get("logger_review_signed_at")
    if not email or not isinstance(signed, (int, float)) or time.time() - signed > SESSION_SECONDS \
            or email not in ALLOWED_EMAILS:
        for k in ("logger_review_email", "logger_review_signed_at"):
            session.pop(k, None)
        return None
    return email


@logger_review.post("/logger-review/api/sign-in")
def sign_in():
    body = json_body()
    email = clean_email(body.get("email"))
    password = body.get("password") if isinstance(body.get("password"), str) else ""
    if not EMAIL.match(email):
        raise ApiError(400, "invalid_email", "Enter a valid email address.")
    if email not in ALLOWED_EMAILS:
        raise ApiError(403, "email_not_authorized", "This email is not authorized to use logger review.")
    now = time.time()
    with _lock:
        state = _failures.get(email)
        if state and state["until"] > now:
            raise ApiError(429, "locked", "Too many wrong passwords. Try again in 15 minutes.")
    ok = password_ok(password)
    with _lock:
        if not ok:
            state = _failures.get(email)
            if not state or state["until"]:
                state = _failures[email] = {"count": 0, "until": 0}
            state["count"] += 1
            if state["count"] >= MAX_TRIES:
                state["until"] = now + LOCK_SECONDS
        else:
            _failures.pop(email, None)
    if not ok:
        raise ApiError(401, "invalid_password", "That password is not correct.")
    session["logger_review_email"] = email
    session["logger_review_signed_at"] = time.time()
    warm_list_cache(current_app._get_current_object())
    return jsonify({"email": email})


@logger_review.post("/logger-review/api/logout")
def logout():
    for k in ("logger_review_email", "logger_review_signed_at"):
        session.pop(k, None)
    return jsonify({"ok": True})


# ---- pages

def page_base():
    return request.script_root + "/logger-review"


def js_string(value):
    return json.dumps(value).replace("<", "\\u003c")


@logger_review.get("/logger-review")
def review_page():
    email = current_reviewer()
    if not email:
        return render_template("logger_review_signin.html", base=page_base())
    html = render_template("logger_review.html", static_base=url_for("static", filename="logger_review/")[:-1])
    html = html.replace('var LR_BASE = "";', "var LR_BASE = " + js_string(request.script_root) + ";", 1)
    html = html.replace('var LR_EMAIL = "";', "var LR_EMAIL = " + js_string(email) + ";", 1)
    resp = current_app.response_class(html, mimetype="text/html")
    resp.headers["Cache-Control"] = "no-store"
    return resp


# ---- review API

def store():
    cached = current_app.config.get("LOGGER_REVIEW_STORE")
    return cached if cached is not None else Store(g.eng)


def reviewer_or_401():
    email = current_reviewer()
    if not email:
        raise ApiError(401, "sign_in_required", "Sign in to use the logger review.")
    return email


def row_limit():
    return current_app.config.get("LOGGER_REVIEW_ROW_LIMIT") or int(os.environ.get("MAX_SUBMISSION_ROWS", DEFAULT_ROW_LIMIT))


def empty_review():
    return {"exists": False, "status": "draft", "notes": "", "removeFails": False, "removedPeriods": [],
            "keptPeriods": [], "thresholds": None, "reviewerEmail": None, "reviewedDate": None,
            "secondReviewerEmail": None, "secondReviewedDate": None, "finalizedDate": None, "updatedDate": None,
            "decidedAt": None}


def review_json(saved):
    if saved is None:
        return empty_review()
    r = {**saved, "exists": True}
    r["decidedAt"] = None if saved["status"] == "draft" else (saved["secondReviewedDate"] or saved["reviewedDate"])
    return r


def eligible_ids(st=None, force=False):
    """The review list ids, cached for 5 minutes because finding them in the raw table takes about 20 seconds."""
    with _ids_lock:
        if force or time.time() - _ids_cache["at"] > LIST_CACHE_SECONDS:
            _ids_cache["ids"] = (st or store()).eligible_submission_ids()
            _ids_cache["at"] = time.time()
        return _ids_cache["ids"]


def warm_list_cache(app):
    """Fill the list cache in the background right after sign-in so the page does not wait for it."""
    if app.config.get("LOGGER_REVIEW_STORE") is not None or time.time() - _ids_cache["at"] <= LIST_CACHE_SECONDS:
        return

    def run():
        try:
            from sqlalchemy import create_engine
            eng = create_engine(os.environ["DB_CONNECTION_STRING"], pool_pre_ping=True)
            try:
                eligible_ids(Store(eng))
            finally:
                eng.dispose()
        except Exception as exc:
            app.logger.error("Logger review list warm-up failed: %s", type(exc).__name__)
    threading.Thread(target=run, daemon=True).start()


def require_submission(sid):
    if not store().submission_exists(sid):
        raise ApiError(404, "not_found", f"Submission {sid} was not found.")


def load_rows(sid):
    """All raw rows of a submission, or an ApiError when it is empty or above the row limit."""
    limit = row_limit()
    rows = store().fetch_raw(sid, limit + 1)
    if not rows:
        raise ApiError(404, "no_rows", f"Submission {sid} has no logger rows.")
    if len(rows) > limit:
        raise ApiError(413, "too_many_rows",
                       f"Submission {sid} has more than {limit} rows, which is too large to review in the page yet.")
    return rows


@logger_review.get("/logger-review/api/submissions")
def list_submissions():
    reviewer_or_401()
    status = request.args.get("status") or None
    if status is not None and status not in validation.LIST_STATUSES:
        raise ValidationError("status must be one of " + ", ".join(validation.LIST_STATUSES))
    try:
        limit = int(request.args.get("limit", MAX_PAGE))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        raise ValidationError("limit and offset must be integers") from None
    if not 1 <= limit <= MAX_PAGE or offset < 0:
        raise ValidationError(f"limit must be 1 to {MAX_PAGE} and offset 0 or more")
    rows, total = store().list_submissions(status, limit, offset, eligible_ids())
    items = []
    for r in rows:
        items.append({"submissionId": str(r["submissionid"]), "datatype": r.get("datatype"),
                      "submittedBy": r.get("login_email"), "submittedAt": r.get("time_stamp"),
                      "estuary": r.get("login_estuary"), "siteId": r.get("login_siteid"),
                      "station": r.get("login_stationno"), "sensorType": r.get("login_sensortype"),
                      "sensorId": r.get("login_sensorid"), "fileName": r.get("original_filename"),
                      "status": r["review_status"], "statusLabel": STATUS_LABEL[r["review_status"]],
                      "reviewerEmail": r.get("reviewer_email"), "secondReviewerEmail": r.get("second_reviewer_email")})
    return jsonify({"items": items, "total": total, "limit": limit, "offset": offset})


@logger_review.get("/logger-review/api/submissions/<sid>")
def get_submission(sid):
    reviewer_or_401()
    sid = validation.submission_id(sid)
    require_submission(sid)
    rows = load_rows(sid)
    sites = store().fetch_sites([r.get("siteid") for r in rows])
    try:
        data = sr.build_submission(sid, rows, sites)
    except sr.DataError as e:
        raise ApiError(422, "bad_data", str(e)) from None
    return jsonify({"submission": data, "review": review_json(store().get_review(sid))})


@logger_review.put("/logger-review/api/submissions/<sid>/review")
def put_review(sid):
    user = reviewer_or_401()
    sid = validation.submission_id(sid)
    review = validation.review_body(request.get_json(silent=True))
    require_submission(sid)
    store().save_draft(sid, review, user)
    return jsonify({"review": review_json(store().get_review(sid))})


@logger_review.post("/logger-review/api/submissions/<sid>/decision")
def post_decision(sid):
    user = reviewer_or_401()
    sid = validation.submission_id(sid)
    decision = validation.decision_body(request.get_json(silent=True))
    require_submission(sid)
    if store().sensor_count(sid) > 1:
        raise ApiError(409, "multiple_sensors", MULTI_SENSOR_MESSAGE)
    saved = store().get_review(sid)
    review = {k: saved[k] for k in ("notes", "removeFails", "removedPeriods", "keptPeriods", "thresholds")} if saved else \
        {"notes": "", "removeFails": False, "removedPeriods": [], "keptPeriods": [], "thresholds": None}
    now = datetime.now(timezone.utc)
    final_rows = None
    if decision == "rejected":
        if not review["notes"].strip():
            raise ApiError(400, "note_required", "Add a note before rejecting.")
        fields = {"status": "rejected", "reviewer_email": user, "reviewed_date": now}
    elif decision == "needs_another_qa":
        fields = {"status": "needs_another_qa", "reviewer_email": user, "reviewed_date": now}
    else:
        if saved and saved["status"] == "needs_another_qa":
            first = saved["reviewerEmail"] or ""
            if first.strip().lower() == user.lower():
                raise ApiError(409, "same_reviewer", "A different reviewer must finalize a submission that needs another QA.")
            fields = {"status": "finalized", "reviewer_email": saved["reviewerEmail"], "reviewed_date": saved["reviewedDate"],
                      "second_reviewer_email": user, "second_reviewed_date": now, "finalized_date": now}
        else:
            fields = {"status": "finalized", "reviewer_email": user, "reviewed_date": now, "finalized_date": now}
        raw = load_rows(sid)
        try:
            cfg = sr.load_defaults()
            gone = removal.removed_readings(raw, review["thresholds"], review["removeFails"],
                                            review["removedPeriods"], review["keptPeriods"], cfg)
            review["thresholds"] = removal.resolved_thresholds(raw, review["thresholds"], cfg)
        except sr.DataError as e:
            raise ApiError(422, "bad_data", str(e)) from None
        final_rows = removal.build_final_rows(raw, gone)
    store().save_decision(sid, review, user, fields, final_rows)
    return jsonify({"review": review_json(store().get_review(sid)),
                    "finalRows": len(final_rows) if final_rows is not None else 0})


@logger_review.errorhandler(Exception)
def unexpected(exc):
    from werkzeug.exceptions import HTTPException
    if isinstance(exc, HTTPException):
        return exc
    current_app.logger.error("Logger review error: %s", type(exc).__name__)
    return jsonify({"error": {"code": "server_error", "message": "Something went wrong."}}), 500
