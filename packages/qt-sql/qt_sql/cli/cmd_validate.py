"""qt validate — validate candidate SQL against original."""

from __future__ import annotations

from pathlib import Path

import click


@click.command()
@click.argument("benchmark")
@click.option("-q", "--query", required=True, help="Query ID to validate.")
@click.option("-f", "--sql-file", required=True, type=click.Path(exists=True),
              help="Path to candidate SQL file.")
@click.option("--config-commands", multiple=True,
              help="SET LOCAL commands (repeatable).")
@click.pass_context
def validate(
    ctx: click.Context,
    benchmark: str,
    query: str,
    sql_file: str,
    config_commands: tuple,
) -> None:
    """Validate a candidate SQL rewrite against the original query.

    Checks row-count equivalence and benchmarks timing (3-run or 5-run).
    """
    from ._common import (
        console,
        resolve_benchmark,
        load_benchmark_config,
        print_header,
        print_error,
        print_success,
    )

    bench_dir = resolve_benchmark(benchmark)
    cfg = load_benchmark_config(bench_dir)

    # Find original SQL
    original_path = bench_dir / "queries" / f"{query}.sql"
    if not original_path.exists():
        # Try prefix match
        matches = sorted(bench_dir.glob(f"queries/{query}*.sql"))
        if len(matches) == 1:
            original_path = matches[0]
        else:
            print_error(f"Original query not found: {query}")
            raise SystemExit(1)

    original_sql = original_path.read_text().strip()
    candidate_sql = Path(sql_file).read_text().strip()
    dsn = cfg.get("benchmark_dsn") or cfg.get("dsn") or cfg.get("db_path", "")

    print_header(f"Validating {original_path.stem} [{bench_dir.name}]")
    console.print(f"  Original:  {len(original_sql)} chars")
    console.print(f"  Candidate: {len(candidate_sql)} chars")

    from ..validate import Validator

    validator = Validator(dsn)

    if config_commands:
        console.print(f"  Config: {list(config_commands)}")
        # Need baseline first
        baseline = validator.benchmark_baseline(original_sql)
        result = validator.validate_with_config(
            baseline=baseline,
            sql=candidate_sql,
            config_commands=list(config_commands),
            worker_id=0,
        )
    else:
        result = validator.validate(
            original_sql=original_sql,
            candidate_sql=candidate_sql,
            worker_id=0,
        )

    # Display result
    console.print()
    status_color = {"pass": "green", "fail": "red", "error": "red"}.get(
        str(result.status.value).lower(), "yellow"
    )
    console.print(f"  Status:  [{status_color}]{result.status.value}[/{status_color}]")

    if hasattr(result, "speedup") and result.speedup:
        console.print(f"  Speedup: {result.speedup:.2f}x")
    if hasattr(result, "original_time_ms") and result.original_time_ms:
        console.print(f"  Original:  {result.original_time_ms:.1f} ms")
    if hasattr(result, "optimized_time_ms") and result.optimized_time_ms:
        console.print(f"  Optimized: {result.optimized_time_ms:.1f} ms")
    if hasattr(result, "rows_match"):
        console.print(f"  Rows match: {result.rows_match}")

    if result.status.value.lower() != "pass":
        raise SystemExit(1)
    print_success("Validation passed.")
