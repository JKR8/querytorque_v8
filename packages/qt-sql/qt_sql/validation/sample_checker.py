"""Sample-based semantic equivalence checker for timeout recovery.

When a candidate returns 0 rows (likely timeout), run both original and
candidate on a small DuckDB copy (3% TABLESAMPLE) to check whether
the rewrite is semantically equivalent.

Usage:
    checker = SampleChecker("/path/to/sample_3pct.duckdb")
    result = checker.check_semantic_equivalence(original_sql, candidate_sql)
    if result.equivalent:
        # Candidate is semantically correct, just slow on full data
"""

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class SampleCheckResult:
    """Result of a sample-based equivalence check."""

    equivalent: bool
    original_sample_rows: int = 0
    candidate_sample_rows: int = 0
    error: Optional[str] = None


class SampleChecker:
    """Check semantic equivalence using a pre-created sample database.

    The sample DB is a DuckDB file with 3% TABLESAMPLE of the full dataset.
    It must be created ahead of time (not at benchmark time).

    This uses its OWN DuckDB connection (cheap, local file — NOT Snowflake/PG).
    """

    def __init__(self, sample_db_path: str):
        self.sample_db_path = sample_db_path

    def check_semantic_equivalence(
        self,
        original_sql: str,
        candidate_sql: str,
        timeout_ms: int = 30_000,
    ) -> SampleCheckResult:
        """Run both queries on sample DB, compare row counts + checksums.

        Args:
            original_sql: Original SQL query.
            candidate_sql: Candidate rewrite SQL.
            timeout_ms: Per-query timeout on sample DB.

        Returns:
            SampleCheckResult with equivalence verdict.
        """
        from ..execution.factory import create_executor_from_dsn
        from .equivalence_checker import EquivalenceChecker

        checker = EquivalenceChecker()

        try:
            with create_executor_from_dsn(self.sample_db_path) as executor:
                # Run original on sample
                orig_rows = executor.execute(original_sql, timeout_ms=timeout_ms)
                orig_count = len(orig_rows) if orig_rows else 0

                # Run candidate on sample
                cand_rows = executor.execute(candidate_sql, timeout_ms=timeout_ms)
                cand_count = len(cand_rows) if cand_rows else 0

                # Compare row counts
                if orig_count != cand_count:
                    return SampleCheckResult(
                        equivalent=False,
                        original_sample_rows=orig_count,
                        candidate_sample_rows=cand_count,
                        error=(
                            f"Sample row count mismatch: "
                            f"original={orig_count}, candidate={cand_count}"
                        ),
                    )

                # Compare checksums
                if orig_rows and cand_rows:
                    orig_cksum = checker.compute_checksum(orig_rows)
                    cand_cksum = checker.compute_checksum(cand_rows)
                    if orig_cksum != cand_cksum:
                        return SampleCheckResult(
                            equivalent=False,
                            original_sample_rows=orig_count,
                            candidate_sample_rows=cand_count,
                            error=(
                                f"Sample checksum mismatch: "
                                f"original={orig_cksum}, candidate={cand_cksum}"
                            ),
                        )

                return SampleCheckResult(
                    equivalent=True,
                    original_sample_rows=orig_count,
                    candidate_sample_rows=cand_count,
                )

        except Exception as e:
            logger.warning(f"Sample check failed: {e}")
            return SampleCheckResult(
                equivalent=False,
                error=f"Sample check error: {e}",
            )
