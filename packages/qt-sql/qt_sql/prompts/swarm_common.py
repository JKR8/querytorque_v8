"""Shared prompt helpers for swarm mode."""

from __future__ import annotations


def build_worker_strategy_header(strategy: str, hint: str) -> str:
    """Build the strategy preamble used for a swarm worker prompt."""
    safe_strategy = strategy or "fallback_strategy"
    safe_hint = hint or "Apply relevant optimizations with semantic safety."
    return (
        f"## Optimization Strategy: {safe_strategy}\n"
        "\n"
        f"**Your approach**: {safe_hint}\n"
        "\n"
        "**Focus**: Apply the examples below in service of this strategy. "
        "Prioritize this specific approach over generic optimizations.\n"
        "\n"
        "---\n"
        "\n"
    )
