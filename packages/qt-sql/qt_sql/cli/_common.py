"""Shared CLI helpers: benchmark resolution, query filtering, Rich output."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from rich.console import Console

console = Console()

# Root of the benchmarks directory
BENCHMARKS_DIR = Path(__file__).resolve().parent.parent / "benchmarks"


def resolve_benchmark(name: str) -> Path:
    """Resolve a short benchmark name or path to an absolute benchmark directory.

    Accepts:
    - Short name: "postgres_dsb_76" → benchmarks/postgres_dsb_76/
    - Relative path: "./my_bench"
    - Absolute path: "/tmp/bench"

    Raises click.BadParameter if the directory or config.json is missing.
    """
    import click

    # Try as a short name first
    candidate = BENCHMARKS_DIR / name
    if candidate.is_dir() and (candidate / "config.json").exists():
        return candidate

    # Try as a path
    p = Path(name).resolve()
    if p.is_dir() and (p / "config.json").exists():
        return p

    # List available benchmarks for the error message
    available = sorted(
        d.name
        for d in BENCHMARKS_DIR.iterdir()
        if d.is_dir() and (d / "config.json").exists()
    ) if BENCHMARKS_DIR.exists() else []

    msg = f"Benchmark not found: {name!r}"
    if available:
        msg += f"\nAvailable: {', '.join(available)}"
    raise click.BadParameter(msg, param_hint="'BENCHMARK'")


def load_benchmark_config(benchmark_dir: Path) -> dict:
    """Load config.json from a benchmark directory."""
    return json.loads((benchmark_dir / "config.json").read_text())


def parse_query_filter(
    query_tuple: Tuple[str, ...],
    benchmark_dir: Path,
) -> List[str]:
    """Convert --query arguments to a sorted list of query IDs.

    If query_tuple is empty, returns all query IDs from queries/*.sql.
    Accepts partial matches: "query001" matches "query001_multi_i1".
    """
    queries_dir = benchmark_dir / "queries"
    if not queries_dir.exists():
        return []

    all_ids = sorted(p.stem for p in queries_dir.glob("*.sql"))

    if not query_tuple:
        return all_ids

    matched = []
    for q in query_tuple:
        # Exact match
        if q in all_ids:
            matched.append(q)
            continue
        # Prefix match
        prefix_matches = [qid for qid in all_ids if qid.startswith(q)]
        if prefix_matches:
            matched.extend(prefix_matches)
        else:
            console.print(f"[yellow]Warning: no match for query filter {q!r}[/yellow]")

    return sorted(set(matched))


def print_header(text: str) -> None:
    """Print a styled header."""
    console.print(f"\n[bold cyan]{text}[/bold cyan]")


def print_error(text: str) -> None:
    """Print an error message."""
    console.print(f"[bold red]Error:[/bold red] {text}")


def print_success(text: str) -> None:
    """Print a success message."""
    console.print(f"[bold green]{text}[/bold green]")


def engine_from_config(benchmark_dir: Path) -> str:
    """Get the engine name from benchmark config. Returns 'duckdb' or 'postgresql'."""
    cfg = load_benchmark_config(benchmark_dir)
    return cfg["engine"]


def dialect_from_engine(engine: str) -> str:
    """Map engine name to sqlglot dialect."""
    return {"postgresql": "postgres", "duckdb": "duckdb", "snowflake": "snowflake"}.get(
        engine, engine
    )
