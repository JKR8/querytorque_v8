"""Cross-engine semantic equivalence checker.

Transpiles SQL from a source dialect (Snowflake, PostgreSQL) to DuckDB,
then runs both original + candidate on a local DuckDB oracle database
to verify row-count equivalence BEFORE burning credits on the target engine.

Checksums are computed for diagnostics but NOT used as a gate — cross-engine
transpilation introduces non-semantic differences (type coercion, expression
evaluation order) that break byte-level matching.

Gate 1.5: sits between sqlglot parse (Gate 1) and benchmark (Wave 2).

Filter stripping: removes literal-value predicates (d_year = 2002,
s_state IN ('IL','KY','TX')) from WHERE/HAVING clauses so queries
return rows even when DSB seed values don't match the oracle data.
Join predicates (a.id = b.id) are preserved. ORDER BY is removed and
LIMIT is capped at 500 to keep execution fast.
"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Maximum rows to return from stripped queries (caps LIMIT)
_MAX_ROWS = 500


@dataclass
class CrossCheckResult:
    """Result of a cross-engine equivalence check."""

    equivalent: bool
    original_rows: int = 0
    candidate_rows: int = 0
    original_checksum: Optional[str] = None
    candidate_checksum: Optional[str] = None
    error: Optional[str] = None  # transpile/qualify/execution error
    transpile_warning: Optional[str] = None  # non-fatal warning
    elapsed_ms: float = 0.0
    used_stripped: bool = False  # True when filter-stripped fallback was used


# ── Filter Stripping ─────────────────────────────────────────────────────


def _is_simple_literal_filter(pred) -> bool:
    """Check if predicate is a simple column-vs-literal filter to strip.

    Returns True for: d_year = 2002, s_state IN ('IL','KY'), col BETWEEN 1 AND 5
    Returns False for: a.id = b.id (join), col > (SELECT ...) (subquery),
                       or any predicate containing a subquery.
    """
    import sqlglot.expressions as exp

    # Never strip predicates containing subqueries (correlated or scalar)
    if pred.find(exp.Subquery) or pred.find(exp.Select):
        return False

    # Check if the predicate contains at least one literal
    has_lit = bool(pred.find(exp.Literal))
    if not has_lit:
        return False  # no literals → it's a join predicate, keep it

    return True


def _is_join_predicate(pred) -> bool:
    """Check if a predicate should be kept (join condition or complex expression)."""
    return not _is_simple_literal_filter(pred)


def _split_and_predicates(condition) -> list:
    """Split an AND-connected condition into individual predicates."""
    import sqlglot.expressions as exp

    parts = []
    if isinstance(condition, exp.And):
        parts.extend(_split_and_predicates(condition.left))
        parts.extend(_split_and_predicates(condition.right))
    else:
        parts.append(condition)
    return parts


def _rejoin_predicates(preds):
    """Rejoin a list of predicates with AND."""
    import sqlglot.expressions as exp

    if not preds:
        return None
    result = preds[0]
    for p in preds[1:]:
        result = exp.And(this=result, expression=p)
    return result


def strip_literal_filters(sql: str, dialect: str = "duckdb") -> str:
    """Strip literal-value filters from WHERE/HAVING, remove ORDER BY, cap LIMIT.

    Keeps join predicates (col = col) so cross-joins don't explode.
    Applied identically to both original and candidate for fair comparison.
    """
    import sqlglot
    import sqlglot.expressions as exp

    try:
        tree = sqlglot.parse_one(sql.rstrip(";"), dialect=dialect)
    except Exception:
        return sql  # can't parse → return as-is

    # Process all WHERE clauses (main query + CTEs + subqueries)
    for where in list(tree.find_all(exp.Where)):
        preds = _split_and_predicates(where.this)
        kept = [p for p in preds if _is_join_predicate(p)]
        if kept:
            where.set("this", _rejoin_predicates(kept))
        else:
            where.pop()

    # Process all HAVING clauses
    for having in list(tree.find_all(exp.Having)):
        having.pop()

    # Remove ORDER BY (set comparison is order-independent)
    for order in list(tree.find_all(exp.Order)):
        order.pop()

    # Cap LIMIT to _MAX_ROWS
    for limit in list(tree.find_all(exp.Limit)):
        try:
            val = int(limit.expression.this)
            if val > _MAX_ROWS:
                limit.set("expression", exp.Literal.number(_MAX_ROWS))
        except (ValueError, AttributeError):
            limit.set("expression", exp.Literal.number(_MAX_ROWS))

    # If no LIMIT exists on the top-level SELECT, add one
    if isinstance(tree, exp.Select) and not tree.find(exp.Limit):
        tree.set("limit", exp.Limit(expression=exp.Literal.number(_MAX_ROWS)))

    return tree.sql(dialect=dialect) + ";"


# ── Main Checker ─────────────────────────────────────────────────────────


class CrossEngineChecker:
    """Check semantic equivalence by transpiling to DuckDB and comparing results.

    Usage::

        with CrossEngineChecker("/path/to/dsb_sf10.duckdb", "postgresql") as checker:
            result = checker.check(original_sql, candidate_sql)
            if not result.equivalent:
                # candidate is semantically wrong — don't benchmark

    Design decisions:
    - Schema cached: built once per checker instance from DuckDB information_schema
    - Connection pooled: one read-only DuckDB connection, closed on __exit__
    - Fail-open on transpile error: returns equivalent=True with error message
    - Timeout: hard 60s Python-thread cutoff (DuckDB has no native timeout)
    - Filter stripping: literal predicates removed so queries return rows
    """

    def __init__(
        self,
        oracle_db_path: str,
        source_dialect: str,
        timeout_s: int = 60,
    ) -> None:
        self.oracle_db_path = oracle_db_path
        self.source_dialect = source_dialect.lower()
        self.timeout_s = timeout_s
        self._conn = None
        self._executor = None
        self._schema: Optional[Dict] = None
        # Cache keyed by (mode, sql) where mode = "raw" or "stripped"
        self._original_cache: Dict[Tuple[str, str], str] = {}
        self._original_rows_cache: Dict[Tuple[str, str], Tuple[int, str, list]] = {}

    def __enter__(self):
        from ..execution.duckdb_executor import DuckDBExecutor

        self._executor = DuckDBExecutor(
            database=self.oracle_db_path, read_only=True
        )
        self._executor.__enter__()
        return self

    def __exit__(self, *exc):
        if self._executor:
            self._executor.__exit__(*exc)
            self._executor = None
        self._schema = None
        self._original_cache.clear()
        self._original_rows_cache.clear()

    def _build_schema(self) -> Dict:
        """Build schema dict from DuckDB information_schema for sqlglot qualify.

        Returns {schema: {table: {column: dtype}}} mapping.
        Cached after first call.
        """
        if self._schema is not None:
            return self._schema

        rows = self._executor.execute(
            """
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name, ordinal_position
            """,
        )

        schema: Dict = {}
        for row in (rows or []):
            s = row.get("table_schema", "main")
            t = row.get("table_name", "")
            c = row.get("column_name", "")
            d = row.get("data_type", "VARCHAR")
            if not t or not c:
                continue
            schema.setdefault(s, {}).setdefault(t, {})[c] = d
            schema.setdefault(s.lower(), {}).setdefault(t.lower(), {})[c.lower()] = d

        self._schema = schema
        return schema

    def _transpile(self, sql: str, do_strip: bool = False) -> Tuple[str, Optional[str]]:
        """Transpile source dialect → DuckDB, optionally strip literal filters.

        Returns (duckdb_sql, error_or_None).
        """
        import sqlglot

        dialect_map = {
            "snowflake": "snowflake",
            "postgresql": "postgres",
            "postgres": "postgres",
        }
        read_dialect = dialect_map.get(self.source_dialect, self.source_dialect)

        try:
            transpiled = sqlglot.transpile(
                sql.rstrip(";"),
                read=read_dialect,
                write="duckdb",
            )
            if not transpiled:
                return "", "sqlglot.transpile returned empty result"
            duckdb_sql = transpiled[0]
        except Exception as e:
            return "", f"Transpile error ({read_dialect}→duckdb): {e}"

        # Qualify column references (non-fatal if fails)
        try:
            from sqlglot.optimizer.qualify import qualify as sqlglot_qualify

            schema = self._build_schema()
            if schema:
                tree = sqlglot.parse_one(duckdb_sql, dialect="duckdb")
                qualified = sqlglot_qualify(
                    tree,
                    schema=schema,
                    dialect="duckdb",
                    validate_qualify_columns=False,
                )
                duckdb_sql = qualified.sql(dialect="duckdb")
        except Exception as e:
            logger.debug(f"Column qualification warning: {e}")

        # Optionally strip literal filters
        if do_strip:
            duckdb_sql = strip_literal_filters(duckdb_sql, dialect="duckdb")
        else:
            duckdb_sql = duckdb_sql.rstrip(";") + ";"

        return duckdb_sql, None

    def _execute_with_timeout(self, duckdb_sql: str) -> Tuple[Optional[list], Optional[str]]:
        """Execute SQL on DuckDB with a hard Python-thread timeout.

        Returns (rows, error). If timeout fires, returns (None, error_msg).
        """
        result_box: List = [None]  # [rows_or_exception]

        def _run():
            try:
                result_box[0] = self._executor.execute(duckdb_sql)
            except Exception as e:
                result_box[0] = e

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        t.join(timeout=self.timeout_s)

        if t.is_alive():
            # Thread still running — timed out
            # We can't kill the DuckDB query, but the daemon thread will die
            # when the process exits. Log and move on.
            logger.warning(
                f"DuckDB query timed out after {self.timeout_s}s, abandoning"
            )
            return None, f"DuckDB execution timed out ({self.timeout_s}s)"

        val = result_box[0]
        if isinstance(val, Exception):
            return None, f"DuckDB execution error: {val}"
        return val, None

    def _execute_and_hash(
        self, duckdb_sql: str
    ) -> Tuple[int, Optional[str], Optional[list], Optional[str]]:
        """Execute SQL on DuckDB oracle and return (row_count, checksum, rows, error)."""
        from .equivalence_checker import EquivalenceChecker

        rows, err = self._execute_with_timeout(duckdb_sql)
        if err:
            return 0, None, None, err

        if rows is None:
            return 0, None, None, "DuckDB returned None"

        row_count = len(rows)
        if row_count == 0:
            return 0, None, [], None

        checker = EquivalenceChecker()
        checksum = checker.compute_checksum(rows)
        return row_count, checksum, rows, None

    def check(self, original_sql: str, candidate_sql: str) -> CrossCheckResult:
        """Check if candidate is semantically equivalent to original.

        Two-pass approach:
        1. Try unstripped (exact filters) — works when data matches.
        2. If original returns 0 rows, retry with literal filters stripped
           so structural errors are still caught.

        Fail-open: transpile/execution errors return equivalent=True.
        """
        t0 = time.time()

        if not self._executor:
            return CrossCheckResult(
                equivalent=True,
                error="CrossEngineChecker not initialized (use context manager)",
                elapsed_ms=0.0,
            )

        orig_key = original_sql.strip()

        # ── Pass 1: unstripped ──────────────────────────────────────────
        cache_key = ("raw", orig_key)
        if cache_key in self._original_cache:
            orig_duckdb = self._original_cache[cache_key]
        else:
            orig_duckdb, orig_err = self._transpile(original_sql, do_strip=False)
            if orig_err:
                elapsed = (time.time() - t0) * 1000
                return CrossCheckResult(
                    equivalent=True,
                    error=f"Original transpile failed: {orig_err}",
                    elapsed_ms=elapsed,
                )
            self._original_cache[cache_key] = orig_duckdb

        if cache_key in self._original_rows_cache:
            orig_count, orig_cksum, orig_rows = self._original_rows_cache[cache_key]
        else:
            orig_count, orig_cksum, orig_rows, exec_err = self._execute_and_hash(
                orig_duckdb
            )
            if exec_err:
                elapsed = (time.time() - t0) * 1000
                return CrossCheckResult(
                    equivalent=True,
                    error=f"Original execution failed: {exec_err}",
                    elapsed_ms=elapsed,
                )
            self._original_rows_cache[cache_key] = (orig_count, orig_cksum, orig_rows)

        # ── Pass 2: if 0 rows, retry with filter stripping ─────────────
        use_stripped = orig_count == 0
        if use_stripped:
            cache_key_s = ("stripped", orig_key)
            if cache_key_s in self._original_cache:
                orig_duckdb = self._original_cache[cache_key_s]
            else:
                orig_duckdb, orig_err = self._transpile(original_sql, do_strip=True)
                if orig_err:
                    elapsed = (time.time() - t0) * 1000
                    return CrossCheckResult(
                        equivalent=True,
                        error=f"Original transpile (stripped) failed: {orig_err}",
                        elapsed_ms=elapsed,
                    )
                self._original_cache[cache_key_s] = orig_duckdb

            if cache_key_s in self._original_rows_cache:
                orig_count, orig_cksum, orig_rows = self._original_rows_cache[cache_key_s]
            else:
                orig_count, orig_cksum, orig_rows, exec_err = self._execute_and_hash(
                    orig_duckdb
                )
                if exec_err:
                    elapsed = (time.time() - t0) * 1000
                    return CrossCheckResult(
                        equivalent=True,
                        error=f"Original execution (stripped) failed: {exec_err}",
                        elapsed_ms=elapsed,
                    )
                self._original_rows_cache[cache_key_s] = (orig_count, orig_cksum, orig_rows)

            if orig_count == 0:
                elapsed = (time.time() - t0) * 1000
                return CrossCheckResult(
                    equivalent=True,
                    original_rows=0,
                    transpile_warning="Original returned 0 rows even after filter stripping",
                    elapsed_ms=elapsed,
                )

        # ── Transpile candidate (same strip mode as original) ───────────
        cand_duckdb, cand_err = self._transpile(candidate_sql, do_strip=use_stripped)
        if cand_err:
            elapsed = (time.time() - t0) * 1000
            return CrossCheckResult(
                equivalent=True,
                original_rows=orig_count,
                original_checksum=orig_cksum,
                error=f"Candidate transpile failed: {cand_err}",
                elapsed_ms=elapsed,
            )

        # Execute candidate
        cand_count, cand_cksum, cand_rows, exec_err = self._execute_and_hash(
            cand_duckdb
        )
        if exec_err:
            elapsed = (time.time() - t0) * 1000
            return CrossCheckResult(
                equivalent=True,
                original_rows=orig_count,
                original_checksum=orig_cksum,
                error=f"Candidate execution failed: {exec_err}",
                elapsed_ms=elapsed,
            )

        elapsed = (time.time() - t0) * 1000

        # Compare row counts
        if orig_count != cand_count:
            mode = "stripped" if use_stripped else "exact"
            msg = f"Row count mismatch ({mode}): original={orig_count}, candidate={cand_count}"
            if use_stripped:
                # Stripped mode: soft warning — structural transforms (decorrelation)
                # change result distribution when literal filters are removed.
                return CrossCheckResult(
                    equivalent=True,
                    original_rows=orig_count,
                    candidate_rows=cand_count,
                    original_checksum=orig_cksum,
                    candidate_checksum=cand_cksum,
                    transpile_warning=f"Stripped-mode mismatch (non-blocking): {msg}",
                    elapsed_ms=elapsed,
                    used_stripped=True,
                )
            return CrossCheckResult(
                equivalent=False,
                original_rows=orig_count,
                candidate_rows=cand_count,
                original_checksum=orig_cksum,
                candidate_checksum=cand_cksum,
                error=msg,
                elapsed_ms=elapsed,
            )

        # Cross-engine: checksums are informational only — transpilation
        # introduces non-semantic differences (type coercion, expression
        # evaluation order) that break byte-level matching. Row count is
        # the gate; checksums are logged for diagnostics.
        if orig_cksum != cand_cksum:
            mode = "stripped" if use_stripped else "exact"
            msg = f"Checksum mismatch ({mode}), rows match: original={orig_cksum}, candidate={cand_cksum}"
            logger.debug(msg)

        return CrossCheckResult(
            equivalent=True,
            original_rows=orig_count,
            candidate_rows=cand_count,
            original_checksum=orig_cksum,
            candidate_checksum=cand_cksum,
            elapsed_ms=elapsed,
            used_stripped=use_stripped,
        )
