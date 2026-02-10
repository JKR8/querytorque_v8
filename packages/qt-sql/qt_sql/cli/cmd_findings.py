"""qt findings — scanner findings extraction (PG only, LLM-powered)."""

from __future__ import annotations

import click


@click.command()
@click.argument("benchmark")
@click.option("--prompt-only", is_flag=True,
              help="Print LLM prompt without calling the API.")
@click.option("--force", is_flag=True,
              help="Re-extract even if findings already exist.")
@click.option("--provider", default=None, help="LLM provider override.")
@click.option("--model", default=None, help="LLM model override.")
@click.pass_context
def findings(
    ctx: click.Context,
    benchmark: str,
    prompt_only: bool,
    force: bool,
    provider: str | None,
    model: str | None,
) -> None:
    """Extract scanner findings from plan-space data (PostgreSQL only, requires LLM).

    \b
    Two steps:
      1. populate_blackboard(): scanner_blackboard.jsonl (deterministic)
      2. extract_findings():    scanner_findings.json   (2-pass LLM)
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

    if cfg["engine"] not in ("postgresql", "postgres"):
        print_error("Scanner findings are PostgreSQL only.")
        raise SystemExit(1)

    print_header(f"Scanner findings [{bench_dir.name}]")

    from ..scanner_knowledge.blackboard import populate_blackboard
    from ..scanner_knowledge.findings import extract_findings

    # Step 1: Populate blackboard (deterministic)
    console.print("[bold]Step 1: Populate scanner blackboard[/bold]")
    bb_path = populate_blackboard(bench_dir)
    console.print(f"  → {bb_path}")

    if prompt_only:
        console.print("\n[bold]Step 2: LLM prompt (dry run)[/bold]")
        # Build the prompt but don't call the API
        from ..scanner_knowledge.findings import build_findings_prompt
        prompt = build_findings_prompt(bb_path)
        console.print(prompt)
        return

    # Step 2: Extract findings (LLM)
    findings_path = bench_dir / "scanner_findings.json"
    if findings_path.exists() and not force:
        console.print(f"  [yellow]scanner_findings.json already exists. Use --force to re-extract.[/yellow]")
        return

    console.print("\n[bold]Step 2: Extract findings (LLM)[/bold]")
    results = extract_findings(
        blackboard_path=bb_path,
        output_path=findings_path,
        provider=provider,
        model=model,
    )
    console.print(f"  {len(results)} findings extracted → {findings_path}")
    print_success("Done.")
