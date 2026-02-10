"""qt blackboard — knowledge collation chain (no LLM)."""

from __future__ import annotations

import json
from pathlib import Path

import click


@click.command()
@click.argument("benchmark", required=False, default=None)
@click.option("--from", "from_dir", type=click.Path(exists=True),
              help="Specific swarm batch or run directory.")
@click.option("--global", "global_mode", is_flag=True,
              help="Build global blackboard from all historical sources.")
@click.option("--promote-only", is_flag=True,
              help="Skip phases 1-3, run only phase 4 (gold example promotion).")
@click.option("--dry-run", is_flag=True,
              help="Preview promotions without writing files.")
@click.option("--min-speedup", type=float, default=2.0, show_default=True,
              help="Minimum speedup for auto-promotion.")
@click.pass_context
def blackboard(
    ctx: click.Context,
    benchmark: str | None,
    from_dir: str | None,
    global_mode: bool,
    promote_only: bool,
    dry_run: bool,
    min_speedup: float,
) -> None:
    """Run the full knowledge collation chain: extract → collate → knowledge → promote.

    \b
    Phases (all deterministic, no LLM):
      1. Extract: Scan swarm batch → raw blackboard entries
      2. Collate: Group by transform → principles + anti-patterns
      3. Global:  Merge into benchmark/knowledge/*.json
      4. Promote: Auto-promote winners to gold examples
      5. Reindex: Rebuild tag index after promotion
    """
    from ._common import (
        console,
        resolve_benchmark,
        print_header,
        print_error,
        print_success,
    )

    if global_mode:
        print_header("Building global blackboard")
        from ..build_blackboard import build_global_blackboard
        out_path = build_global_blackboard()
        print_success(f"Global blackboard → {out_path}")
        return

    if not benchmark:
        print_error("BENCHMARK argument required (or use --global).")
        raise SystemExit(1)

    bench_dir = resolve_benchmark(benchmark)

    # Find batch directory
    if from_dir:
        batch_dir = Path(from_dir).resolve()
    else:
        # Find the latest swarm_batch_* or runs/run_* directory
        batch_dir = _find_latest_batch(bench_dir)
        if not batch_dir:
            print_error("No swarm batch or run directory found. Use --from to specify one.")
            raise SystemExit(1)

    print_header(f"Blackboard pipeline [{bench_dir.name}]")
    console.print(f"  Source: {batch_dir}")

    from ..build_blackboard import (
        phase1_extract,
        phase2_collate,
        phase3_global,
        phase4_promote_winners,
    )

    if not promote_only:
        # Phase 1: Extract
        console.print("\n[bold]Phase 1: Extract[/bold]")
        entries = phase1_extract(batch_dir)
        console.print(f"  {len(entries)} blackboard entries extracted")

        if not entries:
            print_error("No entries found in batch directory.")
            raise SystemExit(1)

        # Phase 2: Collate
        console.print("\n[bold]Phase 2: Collate[/bold]")
        principles, anti_patterns = phase2_collate(entries, batch_dir)
        console.print(f"  {len(principles)} principles, {len(anti_patterns)} anti-patterns")

        # Phase 3: Global knowledge
        console.print("\n[bold]Phase 3: Global knowledge[/bold]")
        from ._common import load_benchmark_config
        cfg = load_benchmark_config(bench_dir)
        dataset = f"{cfg['engine']}_{cfg['benchmark']}"
        phase3_global(principles, anti_patterns, batch_dir,
                      dataset=dataset, benchmark_dir=bench_dir)
        console.print(f"  Merged into {bench_dir.name}/knowledge/")

    # Phase 4: Promote
    console.print("\n[bold]Phase 4: Promote winners[/bold]")
    promoted = phase4_promote_winners(batch_dir, min_speedup=min_speedup, dry_run=dry_run)
    if dry_run:
        console.print(f"  [yellow]DRY RUN[/yellow]: {len(promoted)} candidates would be promoted")
        for p in promoted:
            console.print(f"    {p.get('query_id', '?')} → {p.get('transform', '?')} ({p.get('speedup', '?')}x)")
    else:
        console.print(f"  {len(promoted)} examples promoted")

    # Phase 5: Rebuild tag index
    if not dry_run and promoted:
        console.print("\n[bold]Phase 5: Rebuild tag index[/bold]")
        from ..tag_index import rebuild_index
        ok = rebuild_index()
        if ok:
            console.print("  Tag index rebuilt")
        else:
            console.print("  [yellow]Warning: tag index rebuild failed[/yellow]")

    print_success("Blackboard pipeline complete.")


def _find_latest_batch(bench_dir: Path) -> Path | None:
    """Find the most recently modified swarm_batch_* or runs/run_* directory."""
    candidates = []

    # swarm_batch_* directories (legacy layout)
    for d in bench_dir.glob("swarm_batch_*"):
        if d.is_dir():
            candidates.append(d)

    # runs/run_* directories (new layout)
    runs_dir = bench_dir / "runs"
    if runs_dir.exists():
        for d in runs_dir.glob("run_*"):
            if d.is_dir():
                candidates.append(d)

    if not candidates:
        return None

    return max(candidates, key=lambda d: d.stat().st_mtime)
