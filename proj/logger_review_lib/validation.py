"""Input validation for the review endpoints."""
import math
import re
from datetime import datetime

STATUSES = ("draft", "needs_another_qa", "finalized", "rejected")
DECISIONS = ("finalized", "needs_another_qa", "rejected")
LIST_STATUSES = STATUSES + ("not_reviewed",)
MAX_PERIODS = 1000
MAX_NOTES = 20000
THRESHOLD_FIELDS = {
    "range": ("fail_min", "suspect_min", "suspect_max", "fail_max"),
    "spike": ("suspect", "fail"),
    "roc": ("suspect_sd", "fail_sd", "min_step"),
    "flatline": ("suspect_n", "fail_n", "tol"),
}
STAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$")


class ValidationError(ValueError):
    pass


def is_number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def submission_id(value):
    try:
        sid = int(value)
    except (TypeError, ValueError):
        raise ValidationError("Submission id must be an integer") from None
    if str(sid) != str(value).strip() or not 0 < sid < 2**31:
        raise ValidationError("Submission id must be a positive integer")
    return sid


def stamp(value, what):
    if not isinstance(value, str) or not STAMP.match(value.strip().replace(" ", "T")):
        raise ValidationError(f"{what} must look like 2024-04-01T08:30:00")
    v = value.strip().replace(" ", "T")
    v = v + ":00" if len(v) == 16 else v
    try:
        datetime.strptime(v, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        raise ValidationError(f"{what} is not a real date and time") from None
    return v


def periods(value, name):
    if not isinstance(value, list):
        raise ValidationError(f"{name} must be a list")
    if len(value) > MAX_PERIODS:
        raise ValidationError(f"{name} has more than {MAX_PERIODS} periods")
    out = []
    for i, p in enumerate(value):
        if not isinstance(p, dict):
            raise ValidationError(f"{name}[{i}] must be an object with start and end")
        start, end = stamp(p.get("start"), f"{name}[{i}].start"), stamp(p.get("end"), f"{name}[{i}].end")
        if start > end:
            raise ValidationError(f"{name}[{i}] starts after it ends")
        out.append({"start": start, "end": end})
    return out


def thresholds(value):
    """None, or {series key: {test: {field: number or null}}}."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValidationError("thresholds must be an object or null")
    out = {}
    for key, block in value.items():
        if not isinstance(key, str) or not key or len(key) > 400:
            raise ValidationError("thresholds has an invalid series key")
        if not isinstance(block, dict):
            raise ValidationError(f"thresholds[{key}] must be an object")
        clean = {}
        for test, fields in block.items():
            if test not in THRESHOLD_FIELDS:
                raise ValidationError(f"thresholds[{key}] has an unknown test {test!r}")
            if fields is None:
                fields = {}
            if not isinstance(fields, dict):
                raise ValidationError(f"thresholds[{key}].{test} must be an object")
            clean[test] = {}
            for name, v in fields.items():
                if name not in THRESHOLD_FIELDS[test]:
                    raise ValidationError(f"thresholds[{key}].{test} has an unknown field {name!r}")
                if v is not None and not is_number(v):
                    raise ValidationError(f"thresholds[{key}].{test}.{name} must be a number or null")
                clean[test][name] = v
        out[key] = clean
    return out


def review_body(body):
    """The draft: notes, removeFails, removedPeriods, keptPeriods and optional thresholds."""
    if not isinstance(body, dict):
        raise ValidationError("The request body must be a JSON object")
    missing = [k for k in ("notes", "removeFails", "removedPeriods", "keptPeriods") if k not in body]
    if missing:
        raise ValidationError("Missing " + ", ".join(missing))
    if not isinstance(body["notes"], str):
        raise ValidationError("notes must be text")
    if len(body["notes"]) > MAX_NOTES:
        raise ValidationError("notes are too long")
    if not isinstance(body["removeFails"], bool):
        raise ValidationError("removeFails must be true or false")
    return {"notes": body["notes"], "removeFails": body["removeFails"],
            "removedPeriods": periods(body["removedPeriods"], "removedPeriods"),
            "keptPeriods": periods(body["keptPeriods"], "keptPeriods"),
            "thresholds": thresholds(body.get("thresholds"))}


def decision_body(body):
    if not isinstance(body, dict):
        raise ValidationError("The request body must be a JSON object")
    d = body.get("decision")
    if d not in DECISIONS:
        raise ValidationError("decision must be one of " + ", ".join(DECISIONS))
    return d
