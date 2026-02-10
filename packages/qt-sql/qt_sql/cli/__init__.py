"""QueryTorque SQL CLI — unified interface for the optimization pipeline.

Usage: qt <command> [options]
"""

from __future__ import annotations

import click


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
@click.option("-q", "--quiet", is_flag=True, help="Suppress non-essential output.")
@click.pass_context
def main(ctx: click.Context, verbose: bool, quiet: bool) -> None:
    """QueryTorque SQL — benchmark-driven query optimization."""
    import logging

    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["quiet"] = quiet

    if verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(name)s %(message)s")
    elif quiet:
        logging.basicConfig(level=logging.WARNING)
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


# --- Lazy command registration (keeps `qt --help` fast) ---

def _register_commands() -> None:
    """Import and register all sub-commands."""
    from .cmd_status import status
    from .cmd_prepare import prepare
    from .cmd_run import run
    from .cmd_validate import validate
    from .cmd_scan import scan
    from .cmd_index import index
    from .cmd_blackboard import blackboard
    from .cmd_findings import findings
    from .cmd_leaderboard import leaderboard

    main.add_command(status)
    main.add_command(prepare)
    main.add_command(run)
    main.add_command(validate)
    main.add_command(scan)
    main.add_command(index)
    main.add_command(blackboard)
    main.add_command(findings)
    main.add_command(leaderboard)


_register_commands()
