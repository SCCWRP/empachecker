import re

import pytest

from proj.logger_review_lib import store

NEW_TABLES = {"sde.tbl_wq_logger_review", "sde.tbl_wq_logger_review_period", "sde.tbl_wq_logger_final"}


def tables(sql):
    return set(re.findall(r"\bsde\.[a-z_]+", sql))


def statements():
    cols = ["objectid", "submissionid", "raw_h2otemp", "raw_h2otemp_qcflag_human"]
    return list(store.WRITE_SQL) + [store.final_insert_sql(cols)]


@pytest.mark.parametrize("sql", statements())
def test_write_sql_touches_only_new_tables(sql):
    assert tables(sql) and tables(sql) <= NEW_TABLES
    assert "tbl_wq_logger_raw" not in sql and "submission_tracking" not in sql
    assert re.match(r"\s*(INSERT INTO|DELETE FROM)", sql)
    assert not re.search(r"\b(UPDATE|DROP|ALTER|CREATE|TRUNCATE|GRANT)\b", sql.replace("DO UPDATE SET", ""))


@pytest.mark.parametrize("sql", statements())
def test_write_sql_is_parameterised(sql):
    assert re.search(r":\w+", sql)
    assert not re.search(r"'[^']*'", sql)  # no quoted literals, values only arrive as bound parameters


def test_deletes_are_scoped_to_one_submission():
    for sql in (store.SQL_DELETE_PERIODS, store.SQL_DELETE_FINAL):
        assert sql.strip().endswith("WHERE submissionid = :sid")


def test_final_insert_columns_are_validated():
    sql = store.final_insert_sql(["objectid", "raw_do"])
    assert ":c_objectid" in sql and ":c_raw_do" in sql and ":finalized_at" in sql and "review_finalized_date" in sql
    for bad in ("objectid; drop table x", "Bad", "a b", "x\"y"):
        with pytest.raises(ValueError):
            store.final_insert_sql(["objectid", bad])


def test_read_sql_never_writes():
    import inspect
    src = inspect.getsource(store.Store)
    for fn in ("list_submissions", "submission_exists", "sensor_count", "fetch_raw", "fetch_sites", "get_review"):
        body = inspect.getsource(getattr(store.Store, fn))
        assert not re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER)\b", body), fn
    assert "engine.connect()" in src


def test_list_uses_a_loose_index_scan_not_a_full_distinct():
    import inspect
    body = inspect.getsource(store.Store.eligible_submission_ids)
    assert "WITH RECURSIVE" in body and "ORDER BY ids.submissionid DESC" in body
    assert "r.samplecollectiontimestamp >= :min_date" in body
    listing = inspect.getsource(store.Store.list_submissions)
    assert "unnest(CAST(:ids AS bigint[]))" in listing
    assert "ORDER BY ids.submissionid DESC" in listing
    assert "SELECT DISTINCT submissionid FROM" not in body and "submit =" not in body and "datatype IN" not in body
