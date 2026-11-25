"""All SQL. Raw and tracking tables are only read. Writes touch only the three review tables.

The write statements are module constants (and final_insert_sql) so tests can check them as text.
Every value is a bound parameter. The only text built dynamically is the final-table column
list, and it is checked against a strict name pattern.
"""
import json
import re
import logging
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import bindparam, text
from sqlalchemy.exc import ProgrammingError

RAW = "sde.tbl_wq_logger_raw"
REVIEW = "sde.tbl_wq_logger_review"
PERIOD = "sde.tbl_wq_logger_review_period"
FINAL = "sde.tbl_wq_logger_final"
THRESHOLD = "sde.lu_wq_logger_qc_threshold"   # default thresholds, read only by the app
CHUNK = 2000
MIN_DATA_DATE = "2023-01-01"   # only submissions with a reading on or after this date are reviewed
COLUMN = re.compile(r"^[a-z_][a-z0-9_]*$")


log = logging.getLogger(__name__)


class ReviewTablesMissing(Exception):
    pass


# The reviewed_parameters column is new. Until it exists in the database the store falls back to the
# statements without it (the *_LEGACY ones). None = not known yet, True = column works, False = missing.
_column = {"present": None}


def reviewed_column_missing():
    """True once an undefined-column error showed that reviewed_parameters does not exist yet."""
    return _column["present"] is False


def reset_column_state():
    _column["present"] = None


SQL_UPSERT_REVIEW = f"""
INSERT INTO {REVIEW} (submissionid, status, notes, remove_fails, thresholds, reviewed_parameters,
                      reviewer_email, reviewed_date, second_reviewer_email, second_reviewed_date,
                      finalized_date, created_user, last_edited_user, last_edited_date)
VALUES (:sid, :status, :notes, :remove_fails, CAST(:thresholds AS jsonb), CAST(:reviewed_parameters AS jsonb),
        :reviewer_email, CAST(:reviewed_date AS timestamptz), :second_reviewer_email,
        CAST(:second_reviewed_date AS timestamptz), CAST(:finalized_date AS timestamptz), :user, :user, now())
ON CONFLICT (submissionid) DO UPDATE SET
    status = EXCLUDED.status, notes = EXCLUDED.notes, remove_fails = EXCLUDED.remove_fails,
    thresholds = EXCLUDED.thresholds, reviewed_parameters = EXCLUDED.reviewed_parameters, reviewer_email = EXCLUDED.reviewer_email,
    reviewed_date = EXCLUDED.reviewed_date, second_reviewer_email = EXCLUDED.second_reviewer_email,
    second_reviewed_date = EXCLUDED.second_reviewed_date, finalized_date = EXCLUDED.finalized_date,
    last_edited_user = EXCLUDED.last_edited_user, last_edited_date = now()
"""
# Same statement without reviewed_parameters, used while the column is missing in the database.
SQL_UPSERT_REVIEW_LEGACY = SQL_UPSERT_REVIEW.replace(" reviewed_parameters,", "", 1) \
    .replace(" CAST(:reviewed_parameters AS jsonb),", "", 1) \
    .replace(" reviewed_parameters = EXCLUDED.reviewed_parameters,", "", 1)
SQL_DELETE_PERIODS = f"DELETE FROM {PERIOD} WHERE submissionid = :sid"
SQL_INSERT_PERIOD = (f"INSERT INTO {PERIOD} (submissionid, period_type, period_start, period_end, created_user) "
                     "VALUES (:sid, :period_type, CAST(:period_start AS timestamp), CAST(:period_end AS timestamp), :user)")
SQL_DELETE_FINAL = f"DELETE FROM {FINAL} WHERE submissionid = :sid"
WRITE_SQL = (SQL_UPSERT_REVIEW, SQL_UPSERT_REVIEW_LEGACY, SQL_DELETE_PERIODS, SQL_INSERT_PERIOD, SQL_DELETE_FINAL)

# ---- unit review (sde.tbl_wq_logger_unit_review, one row per flagged sensor series)
UNIT = "sde.tbl_wq_logger_unit_review"
# A refresh inserts newly flagged series and updates the numbers of known ones. It never touches
# correct_unit, reviewer_email or reviewed_date.
SQL_UPSERT_UNIT_FLAG = f"""
INSERT INTO {UNIT} (series_key, submissionid, siteid, stationno, sensortype, sensorid, parameter, recorded_unit,
                    n_values, series_mean, usual_mean, ratio, flagged, last_checked_date, created_user, last_edited_user)
VALUES (:key, :submissionid, :siteid, :stationno, :sensortype, :sensorid, :parameter, :unit,
        :n, :mean, :usual_mean, :ratio, true, CAST(:checked_at AS timestamptz), :user, :user)
ON CONFLICT (series_key) DO UPDATE SET
    n_values = EXCLUDED.n_values, series_mean = EXCLUDED.series_mean, usual_mean = EXCLUDED.usual_mean,
    ratio = EXCLUDED.ratio, flagged = true, last_checked_date = EXCLUDED.last_checked_date
"""
# Series that were flagged before but not in this refresh. Rows are kept, only the flag is cleared.
SQL_UNFLAG_UNIT = (f"UPDATE {UNIT} SET flagged = false, last_checked_date = CAST(:checked_at AS timestamptz) "
                   "WHERE flagged AND last_checked_date < CAST(:checked_at AS timestamptz)")
SQL_SAVE_UNIT = (f"UPDATE {UNIT} SET correct_unit = :correct_unit, reviewer_email = :reviewer_email, "
                 "reviewed_date = CAST(:reviewed_date AS timestamptz), last_edited_user = :user, last_edited_date = now() "
                 "WHERE objectid = :objectid")
UNIT_WRITE_SQL = (SQL_UPSERT_UNIT_FLAG, SQL_UNFLAG_UNIT, SQL_SAVE_UNIT)


def final_insert_sql(columns):
    for c in columns:
        if not COLUMN.match(c):
            raise ValueError("Unexpected column name")
    names = ", ".join(columns + ["review_finalized_date"])
    values = ", ".join([f":c_{c}" for c in columns] + [":finalized_at"])
    return f"INSERT INTO {FINAL} ({names}) VALUES ({values})"


def _detail(exc):
    orig = getattr(exc, "orig", None)
    # psycopg2 keeps the SQLSTATE in pgcode, not in the message; pg8000 puts it in args.
    return " ".join(map(str, getattr(orig, "args", []))) + " " + str(getattr(orig, "pgcode", "") or "") + " " + str(exc)


def _is_missing_reviewed_column(exc):
    """Undefined column (42703) naming reviewed_parameters."""
    detail = _detail(exc)
    return "42703" in detail and "reviewed_parameters" in detail


def _is_missing_table(exc):
    detail = _detail(exc)
    if "42703" in detail:   # undefined column, not a missing table
        return False
    return "42P01" in detail or "does not exist" in detail


@contextmanager
def _guard():
    try:
        yield
    except ProgrammingError as exc:
        if _is_missing_table(exc):
            raise ReviewTablesMissing() from None
        raise


def _note_column_missing():
    if _column["present"] is not False:
        log.warning("sde.tbl_wq_logger_review.reviewed_parameters does not exist yet; saving reviews without it "
                    "and not enforcing the all-parameters-reviewed rule on the server")
    _column["present"] = False


SQL_GET_REVIEW = f"""SELECT status, notes, remove_fails, thresholds, reviewed_parameters, reviewer_email, reviewed_date,
        second_reviewer_email, second_reviewed_date, finalized_date, last_edited_date
        FROM {REVIEW} WHERE submissionid = :sid"""
SQL_GET_REVIEW_LEGACY = SQL_GET_REVIEW.replace(" reviewed_parameters,", "", 1)


def _dt(v):
    return datetime.fromisoformat(v) if isinstance(v, str) else v


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


class Store:
    def __init__(self, engine):
        self.engine = engine

    # ---- reads

    def eligible_submission_ids(self):
        """Ids of submissions with at least one reading on or after MIN_DATA_DATE, largest (newest) first.

        A loose index scan on idx_tbl_wq_logger_raw_submissionid steps through the distinct ids, and for
        each one an EXISTS probe checks the date. A plain SELECT DISTINCT or GROUP BY scans the whole raw
        table. Even so it takes about 20 seconds, so the caller caches the result."""
        sql = text(f"""
            WITH RECURSIVE ids(submissionid) AS (
              (SELECT submissionid FROM {RAW} WHERE submissionid IS NOT NULL ORDER BY submissionid LIMIT 1)
              UNION ALL
              SELECT (SELECT submissionid FROM {RAW} WHERE submissionid > ids.submissionid ORDER BY submissionid LIMIT 1)
              FROM ids WHERE ids.submissionid IS NOT NULL)
            SELECT ids.submissionid FROM ids
            WHERE ids.submissionid IS NOT NULL
              AND EXISTS (SELECT 1 FROM {RAW} r WHERE r.submissionid = ids.submissionid
                          AND r.samplecollectiontimestamp >= :min_date)
            ORDER BY ids.submissionid DESC""")
        with self.engine.connect() as c:
            return [int(r[0]) for r in c.execute(sql, {"min_date": MIN_DATA_DATE})]

    def list_submissions(self, status, limit, offset, ids):
        """The given submission ids (newest first, already filtered by date) with review status and the
        optional tracking fields (one tracking row per id, none is fine)."""
        where = ""
        params = {"ids": list(ids), "limit": limit, "offset": offset}
        if status:
            where = "WHERE coalesce(r.status, 'not_reviewed') = :status"
            params["status"] = status
        sql = text(f"""
            WITH ids(submissionid) AS (SELECT unnest(CAST(:ids AS bigint[]))),
            t AS (
              SELECT DISTINCT ON (submissionid) submissionid, datatype, login_email, time_stamp,
                     login_estuary, login_siteid, login_stationno, login_sensortype, login_sensorid, original_filename
              FROM sde.submission_tracking_table
              WHERE submissionid IN (SELECT submissionid FROM ids)
              ORDER BY submissionid, time_stamp DESC NULLS LAST)
            SELECT ids.submissionid, t.datatype, t.login_email, t.time_stamp, t.login_estuary, t.login_siteid,
                   t.login_stationno, t.login_sensortype, t.login_sensorid, t.original_filename,
                   coalesce(r.status, 'not_reviewed') AS review_status, r.reviewer_email,
                   r.second_reviewer_email, count(*) OVER () AS total
            FROM ids LEFT JOIN t ON t.submissionid = ids.submissionid
                     LEFT JOIN {REVIEW} r ON r.submissionid = ids.submissionid
            {where}
            ORDER BY ids.submissionid DESC
            LIMIT :limit OFFSET :offset""")
        with _guard(), self.engine.connect() as c:
            rows = [dict(r) for r in c.execute(sql, params).mappings()]
        total = rows[0]["total"] if rows else 0
        for r in rows:
            r.pop("total")
        return rows, total

    def submission_exists(self, sid):
        """True when the submission has a reading on or after MIN_DATA_DATE (the same rule as the list)."""
        sql = text(f"SELECT 1 FROM {RAW} WHERE submissionid = :sid AND samplecollectiontimestamp >= :min_date LIMIT 1")
        with self.engine.connect() as c:
            return c.execute(sql, {"sid": sid, "min_date": MIN_DATA_DATE}).first() is not None

    def sensor_count(self, sid):
        """Number of distinct (siteid, stationno, sensortype, sensorid) in the submission."""
        sql = text(f"SELECT count(*) FROM (SELECT DISTINCT siteid, stationno, sensortype, sensorid FROM {RAW} "
                   "WHERE submissionid = :sid) a")
        with self.engine.connect() as c:
            return int(c.execute(sql, {"sid": sid}).scalar())

    def fetch_raw(self, sid, limit):
        """At most limit rows of the submission, all columns. Ask for row_limit + 1 to detect overflow."""
        sql = text(f"SELECT * FROM {RAW} WHERE submissionid = :sid ORDER BY samplecollectiontimestamp, objectid LIMIT :limit")
        with self.engine.connect() as c:
            return [dict(r) for r in c.execute(sql, {"sid": sid, "limit": limit}).mappings()]

    def fetch_thresholds(self):
        """Rows of the default threshold lookup, or None when the table does not exist yet."""
        sql = text(f"SELECT * FROM {THRESHOLD} ORDER BY parameter, region")
        try:
            with _guard(), self.engine.connect() as c:
                return [dict(r) for r in c.execute(sql).mappings()]
        except ReviewTablesMissing:
            return None

    def fetch_sites(self, siteids):
        siteids = sorted({s for s in siteids if s})
        if not siteids:
            return {}
        sql = text("SELECT DISTINCT siteid, estuaryname, region, estuarytype, estuaryclass FROM sde.search "
                   "WHERE siteid IN :sites").bindparams(bindparam("sites", expanding=True))
        sites = {}
        with self.engine.connect() as c:
            for r in c.execute(sql, {"sites": siteids}).mappings():
                sites.setdefault(r["siteid"], {"estuary_name": r["estuaryname"], "region": r["region"],
                                               "estuary_type": r["estuarytype"], "estuary_class": r["estuaryclass"]})
        return sites

    def get_review(self, sid):
        """The saved review as a dict, or None when none exists. reviewedParameters is None when the
        column does not exist in the database yet."""
        if _column["present"] is not False:
            try:
                return self._get_review(sid, True)
            except ProgrammingError as exc:
                if not _is_missing_reviewed_column(exc):
                    raise
                _note_column_missing()
        return self._get_review(sid, False)

    def _get_review(self, sid, with_column):
        with _guard(), self.engine.connect() as c:
            r = c.execute(text(SQL_GET_REVIEW if with_column else SQL_GET_REVIEW_LEGACY), {"sid": sid}).mappings().first()
            if r is None:
                return None
            ps = c.execute(text(f"SELECT period_type, period_start, period_end FROM {PERIOD} "
                                "WHERE submissionid = :sid ORDER BY period_start, objectid"), {"sid": sid}).mappings().all()
        th = r["thresholds"]
        if isinstance(th, str):
            th = json.loads(th)
        reviewed = None
        if with_column:
            reviewed = r["reviewed_parameters"]
            if isinstance(reviewed, str):
                reviewed = json.loads(reviewed)
            reviewed = [k for k in (reviewed or []) if isinstance(k, str)]
        fmt = lambda v: v.strftime("%Y-%m-%dT%H:%M:%S")
        return {"status": r["status"], "notes": r["notes"] or "", "removeFails": bool(r["remove_fails"]),
                "thresholds": th, "reviewedParameters": reviewed,
                "removedPeriods": [{"start": fmt(p["period_start"]), "end": fmt(p["period_end"])} for p in ps if p["period_type"] == "remove"],
                "keptPeriods": [{"start": fmt(p["period_start"]), "end": fmt(p["period_end"])} for p in ps if p["period_type"] == "good"],
                "reviewerEmail": r["reviewer_email"], "reviewedDate": _iso(r["reviewed_date"]),
                "secondReviewerEmail": r["second_reviewer_email"], "secondReviewedDate": _iso(r["second_reviewed_date"]),
                "finalizedDate": _iso(r["finalized_date"]), "updatedDate": _iso(r["last_edited_date"])}

    # ---- unit review reads

    def series_means(self):
        """Mean of every sensor series in the raw table. Scans the whole raw table (about 3 minutes)."""
        from .unit_review import series_means_sql
        with self.engine.connect() as c:
            return [dict(r) for r in c.execute(text(series_means_sql(RAW))).mappings()]

    def unit_options(self):
        """{parameter: [allowed units]} from the checker's unit lookup tables, without Not Recorded."""
        from .unit_review import FIXED_UNITS, LOOKUPS
        out = {p: [u] for p, u in FIXED_UNITS.items()}
        with self.engine.connect() as c:
            for p, table in LOOKUPS.items():
                units = [r[0] for r in c.execute(text(f"SELECT unit FROM sde.{table} ORDER BY objectid"))]
                out[p] = [u for u in units if u and u.strip().lower() != "not recorded"]
        return out

    def list_unit_reviews(self):
        """Every series that is flagged now or already has a correct unit, by submission (newest first)."""
        sql = text(f"""SELECT objectid, submissionid, siteid, stationno, sensortype, sensorid, parameter, recorded_unit,
                              n_values, series_mean, usual_mean, ratio, flagged, last_checked_date,
                              correct_unit, reviewer_email, reviewed_date
                         FROM {UNIT} WHERE flagged OR correct_unit IS NOT NULL
                        ORDER BY submissionid DESC, siteid, stationno, sensorid, parameter, recorded_unit""")
        with _guard(), self.engine.connect() as c:
            rows = [dict(r) for r in c.execute(sql).mappings()]
        for r in rows:
            for k in ("last_checked_date", "reviewed_date"):
                r[k] = _iso(r[k])
        return rows

    def get_unit_review(self, objectid):
        sql = text(f"SELECT objectid, parameter, submissionid FROM {UNIT} WHERE objectid = :objectid")
        with _guard(), self.engine.connect() as c:
            r = c.execute(sql, {"objectid": objectid}).mappings().first()
        return dict(r) if r else None

    # ---- unit review writes

    def save_unit_flags(self, flagged, checked_at, user):
        """Upsert the flagged series and clear the flag on series no longer flagged, in one transaction."""
        rows = [{"key": f["key"], "submissionid": f["submissionid"], "siteid": f.get("siteid"),
                 "stationno": f.get("stationno"), "sensortype": f.get("sensortype"), "sensorid": f.get("sensorid"),
                 "parameter": f["parameter"], "unit": f["unit"], "n": int(f["n"]), "mean": f["mean"],
                 "usual_mean": f["usual_mean"], "ratio": f["ratio"], "checked_at": checked_at, "user": user}
                for f in flagged]
        with _guard(), self.engine.begin() as c:
            for i in range(0, len(rows), CHUNK):
                c.execute(text(SQL_UPSERT_UNIT_FLAG), rows[i:i + CHUNK])
            c.execute(text(SQL_UNFLAG_UNIT), {"checked_at": checked_at})

    def save_unit(self, objectid, correct_unit, user):
        """Record (or clear, with None) the correct unit of one series."""
        with _guard(), self.engine.begin() as c:
            c.execute(text(SQL_SAVE_UNIT), {"objectid": objectid, "correct_unit": correct_unit,
                                            "reviewer_email": user if correct_unit else None,
                                            "reviewed_date": now() if correct_unit else None, "user": user})

    def save_units(self, updates, user):
        """Record the correct unit of several series, [(objectid, unit)], in one transaction."""
        rows = [{"objectid": oid, "correct_unit": unit, "reviewer_email": user if unit else None,
                 "reviewed_date": now() if unit else None, "user": user} for oid, unit in updates]
        if not rows:
            return
        with _guard(), self.engine.begin() as c:
            c.execute(text(SQL_SAVE_UNIT), rows)

    # ---- writes

    def _write_review(self, c, sid, review, user, fields, with_column=True):
        params = {
            "sid": sid, "status": fields["status"], "notes": review["notes"], "remove_fails": review["removeFails"],
            "thresholds": None if review["thresholds"] is None else json.dumps(review["thresholds"]),
            "reviewed_parameters": json.dumps(review.get("reviewedParameters") or []),
            "reviewer_email": fields.get("reviewer_email"), "reviewed_date": _dt(fields.get("reviewed_date")),
            "second_reviewer_email": fields.get("second_reviewer_email"),
            "second_reviewed_date": _dt(fields.get("second_reviewed_date")),
            "finalized_date": _dt(fields.get("finalized_date")), "user": user}
        if not with_column:
            del params["reviewed_parameters"]
        c.execute(text(SQL_UPSERT_REVIEW if with_column else SQL_UPSERT_REVIEW_LEGACY), params)
        c.execute(text(SQL_DELETE_PERIODS), {"sid": sid})
        rows = [{"sid": sid, "period_type": t, "period_start": _dt(p["start"]), "period_end": _dt(p["end"]), "user": user}
                for t, key in (("remove", "removedPeriods"), ("good", "keptPeriods")) for p in review[key]]
        if rows:
            c.execute(text(SQL_INSERT_PERIOD), rows)

    def _with_column_fallback(self, write):
        """Run write(with_column). If the database has no reviewed_parameters column yet, the failed
        transaction is rolled back and the write runs again without it."""
        if _column["present"] is not False:
            try:
                return write(True)
            except ProgrammingError as exc:
                if not _is_missing_reviewed_column(exc):
                    raise
                _note_column_missing()
        return write(False)

    def save_draft(self, sid, review, user):
        """Status draft, reviewer fields cleared, and the submission's rows removed from final, in one transaction."""
        def write(with_column):
            with _guard(), self.engine.begin() as c:
                self._write_review(c, sid, review, user, {"status": "draft"}, with_column)
                c.execute(text(SQL_DELETE_FINAL), {"sid": sid})
        self._with_column_fallback(write)

    def save_decision(self, sid, review, user, fields, final_rows=None):
        """Save the decision. final_rows (finalized only) replace the submission's rows in final, in the same transaction."""
        self._with_column_fallback(lambda with_column: self._save_decision(sid, review, user, fields, final_rows, with_column))

    def _save_decision(self, sid, review, user, fields, final_rows, with_column):
        with _guard(), self.engine.begin() as c:
            self._write_review(c, sid, review, user, fields, with_column)
            c.execute(text(SQL_DELETE_FINAL), {"sid": sid})
            if final_rows:
                columns = list(final_rows[0].keys())
                sql = text(final_insert_sql(columns))
                at = fields["finalized_date"]
                for i in range(0, len(final_rows), CHUNK):
                    c.execute(sql, [{**{f"c_{k}": v for k, v in r.items()}, "finalized_at": at}
                                    for r in final_rows[i:i + CHUNK]])


def now():
    return datetime.now(timezone.utc)
