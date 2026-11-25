"""The app keeps working while sde.tbl_wq_logger_review.reviewed_parameters does not exist yet."""
import re
from contextlib import contextmanager

import pytest
from sqlalchemy.exc import ProgrammingError

from proj.logger_review_lib import store as st


class Missing(Exception):
    def __init__(self):
        super().__init__({"C": "42703", "M": 'column "reviewed_parameters" of relation "tbl_wq_logger_review" does not exist'})


class Result:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row

    def all(self):
        return []


class Conn:
    def __init__(self, engine):
        self.engine = engine

    def execute(self, clause, params=None):
        sql = str(clause)
        self.engine.log.append((sql, params))
        if self.engine.has_column is False and "reviewed_parameters" in sql:
            raise ProgrammingError(sql, params, Missing())
        if sql.lstrip().startswith("SELECT status"):
            row = {"status": "draft", "notes": "", "remove_fails": False, "thresholds": None, "reviewer_email": None,
                   "reviewed_date": None, "second_reviewer_email": None, "second_reviewed_date": None,
                   "finalized_date": None, "last_edited_date": None}
            if "reviewed_parameters" in sql:
                row["reviewed_parameters"] = '["a|b"]'
            return Result(row)
        return Result(None)


class Engine:
    def __init__(self, has_column):
        self.has_column, self.log, self.commits, self.rollbacks = has_column, [], 0, 0

    @contextmanager
    def begin(self):
        try:
            yield Conn(self)
            self.commits += 1
        except BaseException:
            self.rollbacks += 1
            raise

    @contextmanager
    def connect(self):
        yield Conn(self)


REVIEW = {"notes": "n", "removeFails": False, "removedPeriods": [], "keptPeriods": [], "thresholds": None,
          "reviewedParameters": ["a|b"]}


@pytest.fixture(autouse=True)
def fresh_state():
    st.reset_column_state()
    yield
    st.reset_column_state()


def upserts(engine):
    return [(s, p) for s, p in engine.log if "INSERT INTO sde.tbl_wq_logger_review (" in s]


def test_legacy_upsert_is_the_old_statement_without_the_column():
    assert "reviewed_parameters" in st.SQL_UPSERT_REVIEW and "reviewed_parameters" not in st.SQL_UPSERT_REVIEW_LEGACY
    squash = lambda q: re.sub(r"\s+", " ", q)
    assert squash(st.SQL_UPSERT_REVIEW).replace(" reviewed_parameters,", "").replace(" CAST(:reviewed_parameters AS jsonb),", "") \
        .replace(" reviewed_parameters = EXCLUDED.reviewed_parameters,", "") == squash(st.SQL_UPSERT_REVIEW_LEGACY)


def test_column_present_writes_and_reads_it():
    e = Engine(True)
    s = st.Store(e)
    s.save_draft(5, REVIEW, "u")
    (sql, params), = upserts(e)
    assert "reviewed_parameters" in sql and params["reviewed_parameters"] == '["a|b"]'
    assert s.get_review(5)["reviewedParameters"] == ["a|b"]
    assert not st.reviewed_column_missing()


def test_missing_column_falls_back_once_and_warns(caplog):
    e = Engine(False)
    s = st.Store(e)
    with caplog.at_level("WARNING"):
        s.save_draft(5, REVIEW, "u")
    assert e.rollbacks == 1 and e.commits == 1                        # failed attempt rolled back, retry committed
    assert st.reviewed_column_missing()
    assert any("reviewed_parameters" in r.message for r in caplog.records)
    legacy = upserts(e)[-1]
    assert "reviewed_parameters" not in legacy[0] and "reviewed_parameters" not in legacy[1]
    # later calls go straight to the old SQL: no new failed attempt
    before = len(e.log)
    s.save_decision(5, REVIEW, "u", {"status": "rejected"})
    assert not any("reviewed_parameters" in q for q, _ in e.log[before:])
    assert e.rollbacks == 1
    r = s.get_review(5)
    assert r["status"] == "draft" and r["reviewedParameters"] is None


def test_other_programming_errors_are_not_swallowed():
    class Other(Engine):
        @contextmanager
        def begin(self):
            raise ProgrammingError("x", {}, Exception({"C": "42601", "M": "syntax error"}))
            yield
    with pytest.raises(ProgrammingError):
        st.Store(Other(True)).save_draft(5, REVIEW, "u")
    assert not st.reviewed_column_missing()


def test_get_review_falls_back_when_the_select_fails():
    e = Engine(False)
    assert st.Store(e).get_review(5)["reviewedParameters"] is None
    assert st.reviewed_column_missing()


class PsycopgMissing(Exception):
    """psycopg2.errors.UndefinedColumn: the SQLSTATE is only in pgcode, not in the message."""
    pgcode = "42703"

    def __init__(self):
        super().__init__('column "reviewed_parameters" of relation "tbl_wq_logger_review" does not exist')


def test_psycopg2_style_error_is_the_missing_column_not_missing_tables():
    exc = ProgrammingError("SELECT reviewed_parameters", {}, PsycopgMissing())
    assert st._is_missing_reviewed_column(exc)
    assert not st._is_missing_table(exc)
