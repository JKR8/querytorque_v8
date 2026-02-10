#!/usr/bin/env python3
"""
DuckDB TPC-DS Validation Script
Validates speedups from retry_neutrals and retry_collect collections
Uses 5-run trimmed mean validation (CRITICAL: remove min/max outliers)
"""

import os
import sys
import csv
import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
from statistics import mean
import statistics

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    import duckdb
except ImportError:
    logger.error("DuckDB not installed. Run: pip install duckdb")
    sys.exit(1)


@dataclass
class ValidationResult:
    """Result of validating a single query"""
    query_id: str
    original_speedup: float  # from CSV
    best_speedup: float  # from CSV
    best_worker: str  # from CSV (W1, W2, W3, W4)

    # Actual measurements
    original_times: List[float]  # 5 runs
    optimized_times: List[float]  # 5 runs
    actual_speedup: float  # trimmed mean calculation

    # Status
    passed: bool
    tolerance: float  # 10% by default
    error: Optional[str] = None
    notes: str = ""


class DuckDBValidator:
    """Validates DuckDB TPC-DS optimization results"""

    def __init__(self,
                 duckdb_path: str = ":memory:",
                 data_dir: Optional[str] = None,
                 tpcds_scale: float = 0.1):
        """
        Initialize validator

        Args:
            duckdb_path: Path to DuckDB database or :memory:
            data_dir: Optional path to TPC-DS data directory
            tpcds_scale: Scale factor for TPC-DS (0.1 = 100MB, default)
        """
        self.duckdb_path = duckdb_path
        self.data_dir = data_dir
        self.tpcds_scale = tpcds_scale
        self.conn = None
        self.results: List[ValidationResult] = []

    def connect(self):
        """Connect to DuckDB and initialize"""
        try:
            self.conn = duckdb.connect(self.duckdb_path)
            logger.info(f"Connected to DuckDB: {self.duckdb_path}")

            # Load TPC-DS extension if available
            try:
                self.conn.execute("INSTALL tpcds")
                self.conn.execute("LOAD tpcds")
                logger.info("TPC-DS extension loaded")
            except Exception as e:
                logger.warning(f"TPC-DS extension not available: {e}")

        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise

    def setup_tpcds(self):
        """Set up TPC-DS schema and data"""
        try:
            # Check if data already loaded
            tables = self.conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()

            if any('store_sales' in str(t) for t in tables):
                logger.info("TPC-DS tables already loaded")
                return

            logger.info(f"Setting up TPC-DS with scale factor {self.tpcds_scale}...")
            self.conn.execute(f"CALL duckdb_tpcds.load_tpcds({self.tpcds_scale})")
            logger.info("TPC-DS setup complete")

        except Exception as e:
            logger.error(f"Failed to set up TPC-DS: {e}")
            raise

    def load_query_files(self, collection_dir: str) -> Dict[str, Dict[str, str]]:
        """
        Load query files from collection directory

        Returns: Dict mapping query_id -> {original, w1, w2, w3, w4}
        """
        queries = {}
        path = Path(collection_dir)

        if not path.exists():
            logger.warning(f"Directory not found: {collection_dir}")
            return queries

        # Iterate through q* subdirectories
        for query_dir in sorted(path.glob('q*')):
            if not query_dir.is_dir():
                continue

            query_id = query_dir.name
            query_files = {'original': None}

            # Load original.sql
            orig_file = query_dir / 'original.sql'
            if orig_file.exists():
                with open(orig_file) as f:
                    query_files['original'] = f.read().strip()

            # Load worker optimized variants
            for worker in ['w1', 'w2', 'w3', 'w4']:
                opt_file = query_dir / f'{worker}_optimized.sql'
                if opt_file.exists():
                    with open(opt_file) as f:
                        query_files[worker] = f.read().strip()

            if query_files['original']:
                queries[query_id] = query_files
                logger.debug(f"Loaded query {query_id}")

        logger.info(f"Loaded {len(queries)} queries from {collection_dir}")
        return queries

    def execute_query(self, sql: str, timeout: int = 300) -> Tuple[bool, float, Optional[str]]:
        """
        Execute a query and return (success, elapsed_time_ms, error_msg)
        """
        try:
            start = time.time()
            result = self.conn.execute(sql).fetchall()
            elapsed = (time.time() - start) * 1000  # Convert to ms

            # Ensure we got results
            if result is None:
                return False, 0, "No results returned"

            return True, elapsed, None

        except Exception as e:
            error_msg = str(e)
            logger.debug(f"Query execution failed: {error_msg}")
            return False, 0, error_msg

    def validate_query_pair(self,
                           query_id: str,
                           original_sql: str,
                           optimized_sql: str,
                           expected_speedup: float,
                           best_worker: str,
                           tolerance: float = 0.15) -> ValidationResult:
        """
        Validate a query pair (original vs optimized) using 5-run trimmed mean

        CRITICAL: Use trimmed mean (remove min/max from 5 runs, average remaining 3)

        Args:
            tolerance: Maximum allowed deviation (default 15%)
        """
        result = ValidationResult(
            query_id=query_id,
            original_speedup=expected_speedup,
            best_speedup=expected_speedup,
            best_worker=best_worker,
            original_times=[],
            optimized_times=[],
            actual_speedup=0.0,
            passed=False,
            tolerance=tolerance
        )

        try:
            # Run original query 5 times
            logger.info(f"Validating {query_id}... (baseline)")
            for i in range(5):
                success, elapsed, error = self.execute_query(original_sql)
                if not success:
                    result.error = f"Original query failed: {error}"
                    return result
                result.original_times.append(elapsed)
                logger.debug(f"  Run {i+1}: {elapsed:.2f}ms")

            # Calculate trimmed mean for original (remove min/max)
            sorted_orig = sorted(result.original_times)
            original_mean = mean(sorted_orig[1:-1])  # Remove min and max
            logger.info(f"Original (trimmed mean of 3): {original_mean:.2f}ms")

            # Run optimized query 5 times
            logger.info(f"Validating {query_id}... (optimized)")
            for i in range(5):
                success, elapsed, error = self.execute_query(optimized_sql)
                if not success:
                    result.error = f"Optimized query failed: {error}"
                    return result
                result.optimized_times.append(elapsed)
                logger.debug(f"  Run {i+1}: {elapsed:.2f}ms")

            # Calculate trimmed mean for optimized
            sorted_opt = sorted(result.optimized_times)
            optimized_mean = mean(sorted_opt[1:-1])  # Remove min and max
            logger.info(f"Optimized (trimmed mean of 3): {optimized_mean:.2f}ms")

            # Calculate actual speedup
            if optimized_mean > 0:
                result.actual_speedup = original_mean / optimized_mean
            else:
                result.error = "Optimized mean is zero"
                return result

            logger.info(f"Actual speedup: {result.actual_speedup:.2f}x vs expected {expected_speedup:.2f}x")

            # Check if speedup is within tolerance
            deviation = abs(result.actual_speedup - expected_speedup) / expected_speedup
            result.passed = deviation <= tolerance

            if result.passed:
                logger.info(f"✓ {query_id} PASSED (deviation: {deviation*100:.1f}%)")
            else:
                logger.warning(f"✗ {query_id} FAILED (deviation: {deviation*100:.1f}% > {tolerance*100:.1f}%)")
                result.notes = f"Deviation: {deviation*100:.1f}%"

        except Exception as e:
            result.error = str(e)
            logger.error(f"Validation error for {query_id}: {e}")

        return result

    def validate_collection(self, collection_dir: str, master_csv: str) -> None:
        """
        Validate all queries in a collection against master CSV
        """
        # Load queries from collection
        queries = self.load_query_files(collection_dir)
        if not queries:
            logger.error(f"No queries found in {collection_dir}")
            return

        # Load expected speedups from master CSV
        expected = {}
        try:
            with open(master_csv) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Try to get the best worker speedup
                    query_num = row.get('Query_Num', '').lstrip('q')
                    if query_num:
                        expected[f"q{query_num}"] = float(row.get('Expected_Speedup', 1.0))
        except Exception as e:
            logger.warning(f"Could not load master CSV: {e}")

        # Validate each query
        for query_id in sorted(queries.keys()):
            query_files = queries[query_id]

            original = query_files.get('original')
            # Find best optimized variant
            best_worker = None
            best_sql = None
            for worker in ['w4', 'w3', 'w2', 'w1']:  # Priority order
                if query_files.get(worker):
                    best_worker = worker.upper()
                    best_sql = query_files[worker]
                    break

            if not original or not best_sql:
                logger.warning(f"Missing SQL files for {query_id}")
                continue

            expected_speedup = expected.get(query_id, 1.0)

            result = self.validate_query_pair(
                query_id,
                original,
                best_sql,
                expected_speedup,
                best_worker
            )

            self.results.append(result)

    def generate_report(self, output_file: str = "validation_report.json") -> None:
        """Generate validation report"""
        # Summary statistics
        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed

        summary = {
            "total": len(self.results),
            "passed": passed,
            "failed": failed,
            "pass_rate": f"{100*passed/len(self.results):.1f}%" if self.results else "N/A",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Detailed results
        details = []
        for result in sorted(self.results, key=lambda r: r.query_id):
            details.append({
                "query_id": result.query_id,
                "expected_speedup": round(result.original_speedup, 3),
                "actual_speedup": round(result.actual_speedup, 3),
                "best_worker": result.best_worker,
                "passed": result.passed,
                "tolerance": f"{result.tolerance*100:.0f}%",
                "original_times_ms": [f"{t:.1f}" for t in result.original_times],
                "optimized_times_ms": [f"{t:.1f}" for t in result.optimized_times],
                "error": result.error,
                "notes": result.notes,
            })

        report = {
            "summary": summary,
            "details": details,
        }

        # Write JSON report
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)

        logger.info(f"Report written to {output_file}")

        # Print summary
        print("\n" + "="*70)
        print("VALIDATION SUMMARY")
        print("="*70)
        print(f"Total Queries: {summary['total']}")
        print(f"Passed: {summary['passed']} ✓")
        print(f"Failed: {summary['failed']} ✗")
        print(f"Pass Rate: {summary['pass_rate']}")
        print("="*70 + "\n")

        # Print failed queries
        if failed > 0:
            print("FAILED QUERIES:")
            print("-" * 70)
            for result in sorted(self.results, key=lambda r: r.query_id):
                if not result.passed:
                    print(f"\n{result.query_id}:")
                    print(f"  Expected: {result.original_speedup:.2f}x")
                    print(f"  Actual:   {result.actual_speedup:.2f}x")
                    if result.error:
                        print(f"  Error:    {result.error}")
                    if result.notes:
                        print(f"  Notes:    {result.notes}")
            print("-" * 70 + "\n")

        # Print top performers
        print("TOP PERFORMERS (≥2.0x):")
        print("-" * 70)
        top_results = [r for r in self.results if r.passed and r.actual_speedup >= 2.0]
        for result in sorted(top_results, key=lambda r: r.actual_speedup, reverse=True)[:10]:
            print(f"{result.query_id:5s}: {result.actual_speedup:6.2f}x ({result.best_worker})")
        print("-" * 70 + "\n")


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate DuckDB TPC-DS optimization results"
    )
    parser.add_argument(
        "--collection",
        default="retry_neutrals",
        choices=["retry_neutrals", "retry_collect", "both"],
        help="Which collection to validate"
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=0.1,
        help="TPC-DS scale factor (default: 0.1 = 100MB)"
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.15,
        help="Tolerance for speedup deviation (default: 0.15 = 15%%)"
    )
    parser.add_argument(
        "--output",
        default="validation_report.json",
        help="Output report file"
    )
    parser.add_argument(
        "--duckdb-path",
        default=":memory:",
        help="DuckDB path (default: :memory:)"
    )

    args = parser.parse_args()

    # Initialize validator
    validator = DuckDBValidator(
        duckdb_path=args.duckdb_path,
        tpcds_scale=args.scale
    )

    try:
        validator.connect()
        validator.setup_tpcds()

        # Get master CSV path
        master_csv = "research/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Master_v2_20260206.csv"
        if not Path(master_csv).exists():
            logger.warning(f"Master CSV not found at {master_csv}")
            master_csv = None

        # Validate collections
        if args.collection in ["retry_neutrals", "both"]:
            logger.info("\n=== Validating retry_neutrals ===")
            validator.validate_collection("retry_neutrals", master_csv or "")

        if args.collection in ["retry_collect", "both"]:
            logger.info("\n=== Validating retry_collect ===")
            validator.validate_collection("retry_collect", master_csv or "")

        # Generate report
        validator.generate_report(args.output)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
