"""qt leaderboard — show/build benchmark leaderboards."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click


@click.command()
@click.argument("benchmark")
@click.option("--format", "fmt", type=click.Choice(["table", "json", "csv"]),
              default="table", show_default=True, help="Output format.")
@click.option("--top", type=int, default=None, help="Show only top N entries.")
@click.option("--build-strategy", is_flag=True,
              help="Build strategy leaderboard from global knowledge.")
@click.pass_context
def leaderboard(
    ctx: click.Context,
    benchmark: str,
    fmt: str,
    top: int | None,
    build_strategy: bool,
) -> None:
    """Show benchmark leaderboard or build strategy leaderboard."""
    from ._common import (
        console,
        resolve_benchmark,
        print_header,
        print_error,
    )

    bench_dir = resolve_benchmark(benchmark)

    if build_strategy:
        _build_strategy(bench_dir)
        return

    _show_leaderboard(bench_dir, fmt, top)


def _show_leaderboard(bench_dir: Path, fmt: str, top: int | None) -> None:
    """Display the benchmark leaderboard."""
    from rich.table import Table
    from ._common import console, print_header, print_error

    lb_path = bench_dir / "leaderboard.json"
    if not lb_path.exists():
        print_error(f"No leaderboard.json in {bench_dir.name}")
        raise SystemExit(1)

    data = json.loads(lb_path.read_text())
    queries = data.get("queries", [])

    if top:
        queries = queries[:top]

    if fmt == "json":
        console.print_json(json.dumps(queries, indent=2))
        return

    if fmt == "csv":
        print("query_id,status,speedup,original_ms,optimized_ms,transforms")
        for q in queries:
            transforms = "+".join(q.get("transforms", []))
            print(f"{q['query_id']},{q.get('status','')},{q.get('speedup','')},{q.get('original_ms','')},{q.get('optimized_ms','')},{transforms}")
        return

    # Rich table
    print_header(f"Leaderboard: {bench_dir.name}")

    summary = data.get("summary", {})
    console.print(
        f"  Total: {summary.get('total', '?')}  "
        f"WIN: {summary.get('wins', 0)}  "
        f"IMP: {summary.get('improved', 0)}  "
        f"NEU: {summary.get('neutral', 0)}  "
        f"REG: {summary.get('regression', 0)}  "
        f"ERR: {summary.get('errors', 0)}"
    )

    table = Table(show_header=True, header_style="bold")
    table.add_column("#", justify="right", width=4)
    table.add_column("Query", min_width=20)
    table.add_column("Status", justify="center", min_width=8)
    table.add_column("Speedup", justify="right", min_width=8)
    table.add_column("Orig (ms)", justify="right", min_width=10)
    table.add_column("Opt (ms)", justify="right", min_width=10)
    table.add_column("Transforms")

    status_styles = {
        "WIN": "bold green",
        "IMPROVED": "green",
        "NEUTRAL": "dim",
        "REGRESSION": "red",
        "ERROR": "bold red",
        "FAIL": "red",
    }

    for i, q in enumerate(queries, 1):
        status = q.get("status", "?")
        style = status_styles.get(status.upper(), "")
        speedup = q.get("speedup", "")
        speedup_str = f"{speedup:.2f}x" if isinstance(speedup, (int, float)) else str(speedup)
        orig = q.get("original_ms", "")
        orig_str = f"{orig:.1f}" if isinstance(orig, (int, float)) else str(orig)
        opt = q.get("optimized_ms", "")
        opt_str = f"{opt:.1f}" if isinstance(opt, (int, float)) else str(opt)
        transforms = ", ".join(q.get("transforms", []))

        table.add_row(
            str(i),
            q["query_id"],
            f"[{style}]{status}[/{style}]" if style else status,
            speedup_str,
            orig_str,
            opt_str,
            transforms,
        )

    console.print(table)


def _build_strategy(bench_dir: Path) -> None:
    """Build strategy leaderboard from global knowledge."""
    from ._common import console, print_header, print_success, print_error

    print_header(f"Building strategy leaderboard [{bench_dir.name}]")

    # Look for build_strategy_leaderboard.py in the benchmark directory
    script = bench_dir / "build_strategy_leaderboard.py"
    if script.exists():
        # Import and run it
        import importlib.util
        spec = importlib.util.spec_from_file_location("build_strategy", script)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        if hasattr(mod, "build_leaderboard"):
            result = mod.build_leaderboard()
            out_path = bench_dir / "strategy_leaderboard.json"
            out_path.write_text(json.dumps(result, indent=2))
            print_success(f"Strategy leaderboard → {out_path}")
        else:
            print_error("build_strategy_leaderboard.py has no build_leaderboard() function")
            raise SystemExit(1)
    else:
        print_error(f"No build_strategy_leaderboard.py in {bench_dir.name}")
        raise SystemExit(1)
