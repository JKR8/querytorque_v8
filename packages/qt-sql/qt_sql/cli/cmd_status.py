"""qt status — benchmark readiness check."""

from __future__ import annotations

import json
from pathlib import Path

import click


@click.command()
@click.argument("benchmark")
@click.pass_context
def status(ctx: click.Context, benchmark: str) -> None:
    """Show benchmark readiness: data, explains, knowledge, examples."""
    from rich.table import Table

    from ._common import (
        console,
        resolve_benchmark,
        load_benchmark_config,
        print_header,
    )

    bench_dir = resolve_benchmark(benchmark)
    cfg = load_benchmark_config(bench_dir)

    print_header(f"Benchmark: {bench_dir.name}")
    console.print(f"  Engine: {cfg['engine']}  Benchmark: {cfg['benchmark']}  SF: {cfg.get('scale_factor', '?')}")

    table = Table(show_header=True, header_style="bold")
    table.add_column("Check", min_width=30)
    table.add_column("Status", justify="center", min_width=8)
    table.add_column("Detail")

    # config.json
    _add_row(table, "config.json", True, f"engine={cfg['engine']}")

    # queries/
    queries_dir = bench_dir / "queries"
    query_files = sorted(queries_dir.glob("*.sql")) if queries_dir.exists() else []
    n_queries = len(query_files)
    _add_row(table, "queries/", n_queries > 0, f"{n_queries} queries")

    # explains/
    explains_dir = bench_dir / "explains"
    explain_files = set()
    if explains_dir.exists():
        for p in explains_dir.rglob("*.json"):
            explain_files.add(p.stem)
    query_ids = {p.stem for p in query_files}
    missing_explains = sorted(query_ids - explain_files)
    n_explains = len(explain_files & query_ids)
    detail = f"{n_explains}/{n_queries}"
    if missing_explains:
        detail += f" (missing: {', '.join(missing_explains[:5])}{'...' if len(missing_explains) > 5 else ''})"
    _add_row(table, "explains/", n_explains == n_queries, detail)

    # knowledge/*.json
    knowledge_dir = bench_dir / "knowledge"
    knowledge_files = sorted(knowledge_dir.glob("*.json")) if knowledge_dir.exists() else []
    _add_row(table, "knowledge/", len(knowledge_files) > 0, f"{len(knowledge_files)} files")

    # PG-specific: plan_scanner/ and plan_explore/
    if cfg["engine"] in ("postgresql", "postgres"):
        for subdir in ("plan_scanner", "plan_explore"):
            d = bench_dir / subdir
            n = len(list(d.glob("*.json"))) if d.exists() else 0
            _add_row(table, f"{subdir}/", n > 0, f"{n} files")

        # exploit_algorithm.yaml
        exploit = bench_dir / "exploit_algorithm.yaml"
        _add_row(table, "exploit_algorithm.yaml", exploit.exists(), "")

    # gold examples
    examples_dir = Path(__file__).resolve().parent.parent / "examples"
    engine_ex_dir = examples_dir / cfg["engine"]
    if not engine_ex_dir.exists():
        engine_ex_dir = examples_dir / ("duckdb" if cfg["engine"] == "duckdb" else "postgres")
    n_examples = len(list(engine_ex_dir.glob("*.json"))) if engine_ex_dir.exists() else 0
    _add_row(table, "gold examples", n_examples > 0, f"{n_examples} examples ({engine_ex_dir.name}/)")

    # semantic_intents.json
    intents = bench_dir / "semantic_intents.json"
    _add_row(table, "semantic_intents.json", intents.exists(), "")

    # strategy_leaderboard.json
    strat = bench_dir / "strategy_leaderboard.json"
    _add_row(table, "strategy_leaderboard.json", strat.exists(), "")

    # leaderboard.json
    lb = bench_dir / "leaderboard.json"
    lb_detail = ""
    if lb.exists():
        try:
            data = json.loads(lb.read_text())
            summary = data.get("summary", {})
            lb_detail = (
                f"{summary.get('wins', 0)} WIN, "
                f"{summary.get('improved', 0)} IMP, "
                f"{summary.get('neutral', 0)} NEU, "
                f"{summary.get('regression', 0)} REG"
            )
        except Exception:
            lb_detail = "present"
    _add_row(table, "leaderboard.json", lb.exists(), lb_detail)

    console.print(table)


def _add_row(table, check: str, ok: bool, detail: str) -> None:
    mark = "[green]OK[/green]" if ok else "[red]--[/red]"
    table.add_row(check, mark, detail)
