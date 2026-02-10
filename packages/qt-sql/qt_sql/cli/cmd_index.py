"""qt index — tag-based example index management."""

from __future__ import annotations

import click


@click.command()
@click.option("--rebuild", is_flag=True, help="Rebuild the tag index from gold examples.")
@click.option("--stats", is_flag=True, help="Show current index statistics.")
@click.pass_context
def index(ctx: click.Context, rebuild: bool, stats: bool) -> None:
    """Manage the tag-based example index.

    Without flags, shows basic index info.
    """
    from ._common import console, print_error, print_success

    if not rebuild and not stats:
        # Default: show stats
        stats = True

    from ..tag_index import rebuild_index, show_index_stats

    if rebuild:
        console.print("Rebuilding tag index...")
        ok = rebuild_index()
        if ok:
            print_success("Tag index rebuilt.")
        else:
            print_error("Tag index rebuild failed.")
            raise SystemExit(1)

    if stats:
        show_index_stats()
