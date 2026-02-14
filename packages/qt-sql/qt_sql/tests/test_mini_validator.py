from __future__ import annotations

import os
import shutil
import tempfile

import duckdb

from qt_sql.validation.mini_validator import MiniValidator


def _make_db() -> str:
    temp_dir = tempfile.mkdtemp(prefix="qt_semval_")
    path = os.path.join(temp_dir, "mini.duckdb")
    con = duckdb.connect(path)
    con.execute("create table t as select range as id, range % 10 as g from range(5000)")
    con.close()
    return path


def test_identical_query_passes_on_duckdb_sampled_slice() -> None:
    db_path = _make_db()
    try:
        validator = MiniValidator(db_path=db_path, sample_pct=2.0, dialect="duckdb")
        sql = "select g, count(*) as c from t where id < 4000 group by g order by g"
        result = validator.validate_rewrite(sql, sql, worker_id=1)
        assert result.passed is True
        assert result.tier_passed == 3
    finally:
        try:
            validator.close()
        except Exception:
            pass
        shutil.rmtree(os.path.dirname(db_path), ignore_errors=True)


def test_identical_query_is_stable_across_runs() -> None:
    db_path = _make_db()
    try:
        validator = MiniValidator(db_path=db_path, sample_pct=2.0, dialect="duckdb")
        sql = "select g, count(*) as c from t group by g"
        outcomes = [validator.validate_rewrite(sql, sql, worker_id=1).passed for _ in range(5)]
        assert all(outcomes)
    finally:
        try:
            validator.close()
        except Exception:
            pass
        shutil.rmtree(os.path.dirname(db_path), ignore_errors=True)


def test_structural_column_mismatch_is_reported() -> None:
    validator = MiniValidator(db_path=":memory:", sample_pct=2.0, dialect="duckdb")
    try:
        result = validator.validate_rewrite(
            "select a from x",
            "select a, b from x",
            worker_id=1,
        )
        assert result.passed is False
        assert result.column_mismatch is not None
        assert result.tier_passed == 1
    finally:
        validator.close()


def test_execute_falls_back_when_timeout_kw_not_supported() -> None:
    class StubExecutor:
        def execute(self, sql: str):
            return [{"x": 1}]

    validator = MiniValidator(db_path=":memory:")
    rows = validator._execute_sql(StubExecutor(), "select 1", with_timeout=True)
    assert rows == [{"x": 1}]
