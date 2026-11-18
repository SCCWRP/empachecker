"""In-memory stand-in for app.store.Store. No database, so write paths run without a live connection."""
import copy
from datetime import datetime

from proj.logger_review_lib.store import MIN_DATA_DATE, ReviewTablesMissing


class FakeStore:
    def __init__(self, tracking, raw, sites=None, missing_tables=False):
        self.tracking = tracking          # list of dicts like the list query returns (without review_status)
        self.raw = raw                    # {sid: [row dicts]}
        self.sites = sites or {}
        self.missing_tables = missing_tables
        self.reviews = {}                 # sid -> dict in the get_review shape
        self.final = {}                   # sid -> list of rows
        self.writes = []                  # log of write calls

    def _need_tables(self):
        if self.missing_tables:
            raise ReviewTablesMissing()

    calls = 0

    def eligible_submission_ids(self):
        self.calls += 1
        return sorted((sid for sid, rows in self.raw.items()
                       if any(str(r["samplecollectiontimestamp"]) >= MIN_DATA_DATE for r in rows)), reverse=True)

    def list_submissions(self, status, limit, offset, ids):
        self._need_tables()
        by_id = {}
        for t in self.tracking:
            by_id.setdefault(t["submissionid"], t)   # one tracking row per id
        rows = []
        for sid in sorted(ids, reverse=True):
            t = by_id.get(sid, {})
            r = self.reviews.get(sid)
            row = {"submissionid": sid, **{k: v for k, v in t.items() if k != "submissionid"},
                   "review_status": r["status"] if r else "not_reviewed",
                   "reviewer_email": r["reviewerEmail"] if r else None,
                   "second_reviewer_email": r["secondReviewerEmail"] if r else None}
            if status is None or row["review_status"] == status:
                rows.append(row)
        return rows[offset:offset + limit], len(rows)

    def submission_exists(self, sid):
        return any(str(r["samplecollectiontimestamp"]) >= MIN_DATA_DATE for r in self.raw.get(sid, []))

    def sensor_count(self, sid):
        return len({(r.get("siteid"), r.get("stationno"), r.get("sensortype"), r.get("sensorid")) for r in self.raw.get(sid, [])})

    def fetch_raw(self, sid, limit):
        return copy.deepcopy(self.raw.get(sid, [])[:limit])

    def fetch_sites(self, siteids):
        return {s: self.sites[s] for s in siteids if s in self.sites}

    def get_review(self, sid):
        self._need_tables()
        r = self.reviews.get(sid)
        return copy.deepcopy(r) if r else None

    def _write_review(self, sid, review, user, fields):
        stamp = lambda v: v.isoformat() if isinstance(v, datetime) else v
        self.reviews[sid] = {
            "status": fields["status"], "notes": review["notes"], "removeFails": review["removeFails"],
            "thresholds": copy.deepcopy(review["thresholds"]),
            "removedPeriods": copy.deepcopy(review["removedPeriods"]), "keptPeriods": copy.deepcopy(review["keptPeriods"]),
            "reviewerEmail": fields.get("reviewer_email"), "reviewedDate": stamp(fields.get("reviewed_date")),
            "secondReviewerEmail": fields.get("second_reviewer_email"), "secondReviewedDate": stamp(fields.get("second_reviewed_date")),
            "finalizedDate": stamp(fields.get("finalized_date")), "updatedDate": "2026-11-11T12:00:00", "lastUser": user}

    def save_draft(self, sid, review, user):
        self._need_tables()
        self.writes.append(("draft", sid))
        self._write_review(sid, review, user, {"status": "draft"})
        self.final.pop(sid, None)

    def save_decision(self, sid, review, user, fields, final_rows=None):
        self._need_tables()
        self.writes.append(("decision", sid, fields["status"]))
        self._write_review(sid, review, user, fields)
        self.final.pop(sid, None)
        if final_rows:
            self.final[sid] = copy.deepcopy(final_rows)


def tracking_row(sid, ts, datatype="logger_raw"):
    return {"submissionid": sid, "datatype": datatype, "login_email": "x@example.org", "time_stamp": ts, "login_estuary": "E",
            "login_siteid": "S", "login_stationno": "1", "login_sensortype": "ctd", "login_sensorid": "A", "original_filename": "f.csv"}


class FakeUnitStore(FakeStore):
    """FakeStore plus the unit review table, kept in memory."""

    OPTIONS = {"depth": ["m", "ft", "cm"], "pressure": ["cmH2O", "dbar", "mbar", "psi", "bar", "mmHg", "kPa"],
               "do_pct": ["%"]}

    def __init__(self, *a, means=None, **kw):
        super().__init__(*a, **kw)
        self.means = means or []
        self.units = {}          # objectid -> row
        self.next_id = 1

    def series_means(self):
        return copy.deepcopy(self.means)

    def unit_options(self):
        return copy.deepcopy(self.OPTIONS)

    def list_unit_reviews(self):
        self._need_tables()
        rows = [copy.deepcopy(r) for r in self.units.values() if r["flagged"] or r["correct_unit"]]
        return sorted(rows, key=lambda r: (-r["submissionid"], r["parameter"]))

    def get_unit_review(self, objectid):
        self._need_tables()
        r = self.units.get(objectid)
        return {"objectid": r["objectid"], "parameter": r["parameter"]} if r else None

    def save_unit_flags(self, flagged, checked_at, user):
        self._need_tables()
        by_key = {r["series_key"]: r for r in self.units.values()}
        stamp = checked_at.isoformat()
        for f in flagged:
            r = by_key.get(f["key"])
            if r is None:
                r = {"objectid": self.next_id, "series_key": f["key"], "correct_unit": None, "reviewer_email": None,
                     "reviewed_date": None}
                self.units[self.next_id] = r
                self.next_id += 1
            r.update({"submissionid": f["submissionid"], "siteid": f.get("siteid"), "stationno": f.get("stationno"),
                      "sensortype": f.get("sensortype"), "sensorid": f.get("sensorid"), "parameter": f["parameter"],
                      "recorded_unit": f["unit"], "n_values": f["n"], "series_mean": f["mean"],
                      "usual_mean": f["usual_mean"], "ratio": f["ratio"], "flagged": True, "last_checked_date": stamp})
        for r in self.units.values():
            if r["flagged"] and r["last_checked_date"] < stamp:
                r["flagged"], r["last_checked_date"] = False, stamp
        self.writes.append(("unit_flags", len(flagged)))

    def save_unit(self, objectid, correct_unit, user):
        self._need_tables()
        r = self.units[objectid]
        r["correct_unit"] = correct_unit
        r["reviewer_email"] = user if correct_unit else None
        r["reviewed_date"] = "2026-11-17T12:00:00+00:00" if correct_unit else None
        self.writes.append(("unit", objectid, correct_unit))
