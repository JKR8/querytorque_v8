"""qt playbook — generate knowledge playbook from gold examples.

Usage:
    qt playbook duckdb
    qt playbook postgresql --output knowledge/postgresql_DRAFT.md
    qt playbook duckdb --overwrite
"""

from __future__ import annotations

from pathlib import Path

import click

from ._common import console


# Engine name → examples subdirectory
_ENGINE_TO_DIR = {
    "duckdb": "duckdb",
    "postgresql": "postgres",
    "postgres": "postgres",
    "snowflake": "snowflake",
}


@click.command()
@click.argument("dialect")
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Output path (default: knowledge/{dialect}_DRAFT.md).",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite output if it exists.",
)
@click.pass_context
def playbook(ctx: click.Context, dialect: str, output: str | None, overwrite: bool) -> None:
    """Generate knowledge playbook from gold examples."""
    from qt_sql.knowledge.gen_playbook import (
        _CONSTRAINTS_DIR,
        _EXAMPLES_DIR,
        _ENGINE_TO_EXAMPLE_DIR,
        _HERE,
        generate_playbook_from_gold_examples,
    )

    # Normalize dialect aliases for consistent file resolution
    _DIALECT_CANONICAL = {"postgres": "postgresql"}
    canonical_dialect = _DIALECT_CANONICAL.get(dialect, dialect)

    # Resolve paths
    subdir = _ENGINE_TO_EXAMPLE_DIR.get(dialect, dialect)
    examples_dir = _EXAMPLES_DIR / subdir

    if not examples_dir.exists():
        console.print(f"[bold red]Examples directory not found: {examples_dir}[/bold red]")
        console.print(f"[dim]Available: {', '.join(d.name for d in _EXAMPLES_DIR.iterdir() if d.is_dir())}[/dim]")
        raise SystemExit(1)

    ep_path = _CONSTRAINTS_DIR / f"engine_profile_{canonical_dialect}.json"
    if not ep_path.exists():
        console.print(f"[bold red]Engine profile not found: {ep_path}[/bold red]")
        raise SystemExit(1)

    # Output path
    out_path = Path(output) if output else (_HERE / f"{canonical_dialect}_DRAFT.md")

    if out_path.exists() and not overwrite:
        console.print(f"[bold red]Output exists: {out_path} (use --overwrite)[/bold red]")
        raise SystemExit(1)

    console.print(f"\n[bold cyan]Generating playbook for {canonical_dialect}[/bold cyan]")
    console.print(f"  Examples: {examples_dir}")
    console.print(f"  Engine profile: {ep_path}")
    console.print(f"  Output: {out_path}")
    console.print()

    # Generate
    result = generate_playbook_from_gold_examples(examples_dir, ep_path, canonical_dialect)

    # Write
    out_path.write_text(result, encoding="utf-8")

    n_lines = result.count("\n")
    n_chars = len(result)

    console.print(f"\n{'=' * 60}")
    console.print(f"  [green]Generated: {out_path.name}[/green]")
    console.print(f"  Lines:     {n_lines}")
    console.print(f"  Chars:     {n_chars}")
    console.print(f"{'=' * 60}")
    console.print(f"\nReview the DRAFT and compare with the existing {canonical_dialect}.md.")
    console.print("[bold]Next steps:[/bold]")
    console.print(f"  diff knowledge/{canonical_dialect}.md knowledge/{canonical_dialect}_DRAFT.md")
    console.print(f"  cp knowledge/{canonical_dialect}_DRAFT.md knowledge/{canonical_dialect}.md")
