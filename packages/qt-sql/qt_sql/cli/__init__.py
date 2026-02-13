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

@click.command()
@click.argument("benchmark")
@click.option("--port", default=8765, show_default=True, help="HTTP port for the dashboard.")
def dashboard(benchmark: str, port: int) -> None:
    """Open swarm session dashboard in browser."""
    from ._common import resolve_benchmark
    from .dashboard_cmd import serve_dashboard

    bench_dir = resolve_benchmark(benchmark)
    serve_dashboard(bench_dir, port)


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
    from .cmd_config_boost import config_boost
    from .cmd_refresh_explains import refresh_explains
    from .cmd_collect_explains import cmd_collect_explains
    from .cmd_config_coach import config_coach

    main.add_command(status)
    main.add_command(prepare)
    main.add_command(run)
    main.add_command(validate)
    main.add_command(scan)
    main.add_command(index)
    main.add_command(blackboard)
    main.add_command(findings)
    main.add_command(leaderboard)
    main.add_command(dashboard)
    main.add_command(config_boost)
    main.add_command(refresh_explains)
    main.add_command(cmd_collect_explains)
    main.add_command(config_coach)


_register_commands()
