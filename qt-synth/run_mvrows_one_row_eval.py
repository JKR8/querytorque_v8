#!/usr/bin/env python3
"""Evaluate MVROWS one-row witness detection against SF100 truth labels.

This script rebuilds synthetic databases query-by-query using deterministic
MVROWS-style seeding, executes original + optimized SQL, and compares the
synthetic EQ/NEQ prediction against the SF100 oracle labels stored in
`equivalence_results.json`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "packages" / "qt-shared"))
sys.path.insert(0, str(PROJECT_ROOT / "packages" / "qt-sql"))

from qt_sql.validation.synthetic_validator import SyntheticValidator  # noqa: E402
from qt_sql.validation.build_dsb76_synthetic_db import (  # noqa: E402
    _apply_mvrows_recipe,
    _build_query_context,
    _count_query_rows,
    _force_seed_for_query,
    _is_obviously_unsat,
    _merge_fk,
    _merge_filters,
    _merge_table_schemas,
    _tables_in_anti_patterns,
    _top_up_for_query,
)


@dataclass
class Metrics:
    n: int
    tp: int
    fn: int
    tn: int
    fp: int
    recall: float
    precision: float
    accuracy: float


def _hash_rows(rows: List[Any]) -> str:
    return hashlib.md5("\n".join(sorted(str(r) for r in rows)).encode()).hexdigest()


def _compute_metrics(records: List[Dict[str, Any]]) -> Metrics:
    tp = fp = tn = fn = 0
    for row in records:
        gt_neq = row["gt_sf100_eq"] is False
        pred_neq = row["pred"] == "NEQ"
        if gt_neq and pred_neq:
            tp += 1
        elif gt_neq and not pred_neq:
            fn += 1
        elif (not gt_neq) and pred_neq:
            fp += 1
        else:
            tn += 1
    n = tp + fn + tn + fp
    recall = (tp / (tp + fn)) if (tp + fn) else 0.0
    precision = (tp / (tp + fp)) if (tp + fp) else 0.0
    accuracy = ((tp + tn) / n) if n else 0.0
    return Metrics(
        n=n,
        tp=tp,
        fn=fn,
        tn=tn,
        fp=fp,
        recall=recall,
        precision=precision,
        accuracy=accuracy,
    )


def _load_truth_map(path: Path) -> Dict[str, bool]:
    data = json.loads(path.read_text(encoding="utf-8"))
    out: Dict[str, bool] = {}
    for row in data:
        match = row.get("sf100_match")
        if isinstance(match, bool):
            out[row["query"]] = match
    return out


def _seed_for_witness(
    conn: duckdb.DuckDBPyConnection,
    validator: SyntheticValidator,
    qctx: Dict[str, Any],
    global_tables: Dict[str, Dict[str, Any]],
    fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
    *,
    count_timeout_s: int,
    seed_attempts: int,
) -> Tuple[int, bool]:
    sql = qctx["sql_duckdb"]
    rows_probe = 0
    unsat = False

    try:
        rows_probe = _count_query_rows(conn, sql, count_timeout_s, probe_limit=1)
    except Exception:
        rows_probe = 0

    if rows_probe < 1:
        try:
            _top_up_for_query(
                conn=conn,
                validator=validator,
                qctx=qctx,
                global_tables=global_tables,
                fact_rows=64,
                dim_rows=16,
            )
            rows_probe = _count_query_rows(conn, sql, count_timeout_s, probe_limit=1)
        except Exception:
            rows_probe = 0

    if rows_probe < 1:
        anti_tables = _tables_in_anti_patterns(sql)
        for attempt in range(1, seed_attempts + 1):
            try:
                _force_seed_for_query(
                    conn,
                    qctx,
                    global_tables,
                    fk_relationships,
                    seed_variant=attempt,
                    seed_rows=1,
                    skip_tables=anti_tables,
                )
            except Exception:
                pass
            try:
                rows_probe = _count_query_rows(conn, sql, count_timeout_s, probe_limit=1)
            except Exception:
                rows_probe = 0
            if rows_probe >= 1:
                break

    if rows_probe < 1:
        try:
            _apply_mvrows_recipe(conn, qctx, global_tables)
            rows_probe = _count_query_rows(conn, sql, count_timeout_s, probe_limit=1)
        except Exception:
            rows_probe = 0

    if rows_probe < 1:
        unsat = _is_obviously_unsat(qctx, global_tables)

    return rows_probe, unsat


def _run_eval(
    truth_file: Path,
    baseline_dir: Path,
    optimized_dir: Path,
    *,
    schema_mode: str,
    count_timeout_s: int,
    seed_attempts: int,
) -> Dict[str, Any]:
    truth = _load_truth_map(truth_file)
    validator = SyntheticValidator(reference_db=None, dialect="postgres")
    records: List[Dict[str, Any]] = []

    candidate_names = [
        name
        for name in sorted(truth)
        if (baseline_dir / f"{name}.sql").exists()
        and (optimized_dir / name / "swarm2_final" / "optimized.sql").exists()
    ]

    for query_name in candidate_names:
        orig_file = baseline_dir / f"{query_name}.sql"
        opt_file = optimized_dir / query_name / "swarm2_final" / "optimized.sql"
        q_orig = _build_query_context(validator, orig_file, "postgres")
        q_opt = _build_query_context(validator, opt_file, "postgres")

        # "original": schema derived from baseline only.
        # "merged": schema merged from baseline + optimized parse trees.
        global_tables: Dict[str, Dict[str, Any]]
        fk_relationships: Dict[str, Dict[str, Tuple[str, str]]]
        if schema_mode == "merged":
            global_tables = {}
            fk_relationships = {}
            merged_filters: Dict[str, Dict[str, List[Any]]] = {}
            _merge_table_schemas(global_tables, q_orig["tables"])
            _merge_table_schemas(global_tables, q_opt["tables"])
            _merge_fk(fk_relationships, q_orig["fk_relationships"])
            _merge_fk(fk_relationships, q_opt["fk_relationships"])
            _merge_filters(merged_filters, q_orig["filter_values"])
            _merge_filters(merged_filters, q_opt["filter_values"])
        else:
            global_tables = q_orig["tables"]
            fk_relationships = q_orig["fk_relationships"]

        conn = duckdb.connect(":memory:")
        validator.conn = conn

        fallback_schema_mode = schema_mode
        try:
            validator._create_schema(global_tables)
        except Exception as schema_exc:
            # Some optimized SQL aliases can parse into invalid synthetic column
            # names; fallback to baseline-only schema to keep eval runnable.
            fallback_schema_mode = "original_fallback"
            global_tables = q_orig["tables"]
            fk_relationships = q_orig["fk_relationships"]
            conn.close()
            conn = duckdb.connect(":memory:")
            validator.conn = conn
            validator._create_schema(global_tables)

        validator._create_indexes(global_tables, q_orig["sql_duckdb"])
        validator._create_indexes(global_tables, q_opt["sql_duckdb"])

        rows_probe, unsat = _seed_for_witness(
            conn,
            validator,
            q_orig,
            global_tables,
            fk_relationships,
            count_timeout_s=count_timeout_s,
            seed_attempts=seed_attempts,
        )

        pred = "ERR"
        reason = ""
        orig_rows = 0
        opt_rows = 0
        try:
            orig_result = conn.execute(q_orig["sql_duckdb"]).fetchall()
            opt_result = conn.execute(q_opt["sql_duckdb"]).fetchall()
            orig_rows = len(orig_result)
            opt_rows = len(opt_result)
            pred = (
                "EQ"
                if (orig_rows == opt_rows and _hash_rows(orig_result) == _hash_rows(opt_result))
                else "NEQ"
            )
            reason = "hash_match" if pred == "EQ" else "hash_mismatch"
        except Exception as exec_exc:
            pred = "ERR"
            reason = str(exec_exc)[:240]
        finally:
            conn.close()

        records.append(
            {
                "query": query_name,
                "gt_sf100_eq": truth[query_name],
                "pred": pred,
                "reason": reason,
                "orig_rows": orig_rows,
                "opt_rows": opt_rows,
                "rows_probe": rows_probe,
                "unsat": unsat,
                "schema_mode_used": fallback_schema_mode,
            }
        )

    comparable = [r for r in records if r["pred"] in ("EQ", "NEQ") and isinstance(r["gt_sf100_eq"], bool)]
    one_row = [r for r in comparable if r["orig_rows"] == 1]

    summary = {
        "total_records": len(records),
        "comparable_records": len(comparable),
        "one_row_records": len(one_row),
        "comparable_metrics": asdict(_compute_metrics(comparable)),
        "one_row_metrics": asdict(_compute_metrics(one_row)),
        "one_row_missed_non_equivalent": [
            r["query"] for r in one_row if r["gt_sf100_eq"] is False and r["pred"] != "NEQ"
        ],
        "one_row_detected_non_equivalent": [
            r["query"] for r in one_row if r["gt_sf100_eq"] is False and r["pred"] == "NEQ"
        ],
        "error_queries": [r["query"] for r in records if r["pred"] == "ERR"],
    }
    return {"summary": summary, "results": records}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run MVROWS one-row synthetic EQ/NEQ detection eval vs SF100 truth."
    )
    parser.add_argument(
        "--truth-file",
        default=str(PROJECT_ROOT / "qt-synth" / "equivalence_results.json"),
        help="Path to SF100 truth file with `sf100_match` labels.",
    )
    parser.add_argument(
        "--baseline-dir",
        default=str(
            PROJECT_ROOT / "packages" / "qt-sql" / "qt_sql" / "benchmarks" / "postgres_dsb_76" / "baseline_queries"
        ),
        help="Directory containing baseline SQL files (query_name.sql).",
    )
    parser.add_argument(
        "--optimized-dir",
        default=str(PROJECT_ROOT / "research" / "ALL_OPTIMIZATIONS" / "postgres_dsb"),
        help="Directory containing optimized SQL at query_name/swarm2_final/optimized.sql.",
    )
    parser.add_argument(
        "--output-file",
        default=str(PROJECT_ROOT / "qt-synth" / "mvrows_one_row_equiv_eval.json"),
        help="Destination JSON report path.",
    )
    parser.add_argument(
        "--schema-mode",
        choices=["original", "merged"],
        default="original",
        help="Schema extraction mode for synthetic DB creation.",
    )
    parser.add_argument(
        "--count-timeout-s",
        type=int,
        default=4,
        help="Timeout seconds for row-probe checks.",
    )
    parser.add_argument(
        "--seed-attempts",
        type=int,
        default=6,
        help="Number of `_force_seed_for_query` attempts before recipe fallback.",
    )
    args = parser.parse_args()

    report = _run_eval(
        truth_file=Path(args.truth_file),
        baseline_dir=Path(args.baseline_dir),
        optimized_dir=Path(args.optimized_dir),
        schema_mode=args.schema_mode,
        count_timeout_s=args.count_timeout_s,
        seed_attempts=args.seed_attempts,
    )
    report["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    report["config"] = {
        "truth_file": args.truth_file,
        "baseline_dir": args.baseline_dir,
        "optimized_dir": args.optimized_dir,
        "schema_mode": args.schema_mode,
        "count_timeout_s": args.count_timeout_s,
        "seed_attempts": args.seed_attempts,
    }

    out_path = Path(args.output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    one_row = report["summary"]["one_row_metrics"]
    print(
        "ONE_ROW_METRICS",
        f"n={one_row['n']}",
        f"recall={one_row['recall']:.3f}",
        f"precision={one_row['precision']:.3f}",
        f"accuracy={one_row['accuracy']:.3f}",
    )
    print("OUTPUT", str(out_path))


if __name__ == "__main__":
    main()
