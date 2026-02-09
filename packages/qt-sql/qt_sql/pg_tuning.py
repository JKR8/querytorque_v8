"""Per-query PostgreSQL tuning via SET LOCAL.

Whitelist of safe, session-scoped parameters that can be tuned per-query.
SET LOCAL changes settings only for the current transaction — they revert
on COMMIT/ROLLBACK, affecting no other connections.

This module provides:
  - PG_TUNABLE_PARAMS: whitelist of tunable parameters with types and ranges
  - TuningConfig: dataclass for a tuning recommendation
  - validate_tuning_config(): strips disallowed params, validates ranges
  - build_set_local_sql(): generates SET LOCAL statements
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

# Whitelist of safe, session-scoped PostgreSQL parameters.
# Each entry: param_name -> (type, min, max, description)
# type is one of: "bytes", "int", "float", "bool"
# For "bytes", min/max are in MB. For "bool", min/max are ignored.
PG_TUNABLE_PARAMS: Dict[str, Tuple[str, Any, Any, str]] = {
    "work_mem": (
        "bytes", 64, 2048,
        "Memory for sorts/hashes per operation (MB). Allocated PER-OPERATION, "
        "not per-query. Count hash/sort ops in EXPLAIN before sizing."
    ),
    "max_parallel_workers_per_gather": (
        "int", 0, 8,
        "Max parallel workers per Gather node."
    ),
    "parallel_tuple_cost": (
        "float", 0.0, 1.0,
        "Planner estimate of cost to transfer a tuple to parallel worker."
    ),
    "parallel_setup_cost": (
        "float", 0.0, 10000.0,
        "Planner estimate of cost to launch parallel workers."
    ),
    "random_page_cost": (
        "float", 1.0, 10.0,
        "Planner estimate of cost of a random page fetch (1.0 = SSD, 4.0 = HDD)."
    ),
    "effective_cache_size": (
        "bytes", 1024, 65536,
        "Advisory: how much OS cache to expect (MB). Safe to set aggressively."
    ),
    "join_collapse_limit": (
        "int", 1, 20,
        "Max FROM items before planner stops trying all join orders."
    ),
    "from_collapse_limit": (
        "int", 1, 20,
        "Max FROM items before subqueries stop being flattened."
    ),
    "geqo_threshold": (
        "int", 2, 20,
        "Number of FROM items that triggers genetic query optimizer."
    ),
    "enable_hashjoin": (
        "bool", None, None,
        "Enable hash join plan type."
    ),
    "enable_mergejoin": (
        "bool", None, None,
        "Enable merge join plan type."
    ),
    "enable_nestloop": (
        "bool", None, None,
        "Enable nested-loop join plan type."
    ),
    "enable_seqscan": (
        "bool", None, None,
        "Enable sequential scan plan type."
    ),
    "jit": (
        "bool", None, None,
        "Enable JIT compilation."
    ),
    "jit_above_cost": (
        "float", 0.0, 1000000.0,
        "Query cost above which JIT is activated."
    ),
    "hash_mem_multiplier": (
        "float", 1.0, 10.0,
        "Multiplier applied to work_mem for hash-based operations."
    ),
}


@dataclass
class TuningConfig:
    """A per-query tuning recommendation from the LLM."""
    params: Dict[str, str]  # param_name -> value as string
    reasoning: str = ""


def _parse_bytes_value(val: str) -> int:
    """Parse a bytes value like '512MB' or '2GB' to MB."""
    val = val.strip().upper()
    if val.endswith("GB"):
        return int(float(val[:-2]) * 1024)
    if val.endswith("MB"):
        return int(float(val[:-2]))
    if val.endswith("KB"):
        return max(1, int(float(val[:-2]) / 1024))
    # Bare number = assume MB
    return int(float(val))


def _format_bytes_value(mb: int) -> str:
    """Format MB value to PostgreSQL-style string."""
    if mb >= 1024 and mb % 1024 == 0:
        return f"{mb // 1024}GB"
    return f"{mb}MB"


def validate_tuning_config(config: Dict[str, Any]) -> Dict[str, str]:
    """Validate tuning config against the whitelist.

    Strips disallowed params, validates types and ranges.
    Returns cleaned dict of param_name -> validated value string.
    """
    cleaned: Dict[str, str] = {}

    for param, value in config.items():
        if param not in PG_TUNABLE_PARAMS:
            continue  # Strip non-whitelisted params

        ptype, pmin, pmax, _ = PG_TUNABLE_PARAMS[param]
        str_val = str(value).strip()

        if ptype == "bool":
            lower = str_val.lower()
            if lower in ("true", "on", "1", "yes"):
                cleaned[param] = "on"
            elif lower in ("false", "off", "0", "no"):
                cleaned[param] = "off"
            # else skip invalid bool

        elif ptype == "bytes":
            try:
                mb = _parse_bytes_value(str_val)
                mb = max(pmin, min(pmax, mb))
                cleaned[param] = _format_bytes_value(mb)
            except (ValueError, TypeError):
                pass  # Skip unparseable

        elif ptype == "int":
            try:
                ival = int(float(str_val))
                ival = max(pmin, min(pmax, ival))
                cleaned[param] = str(ival)
            except (ValueError, TypeError):
                pass

        elif ptype == "float":
            try:
                fval = float(str_val)
                fval = max(pmin, min(pmax, fval))
                cleaned[param] = str(fval)
            except (ValueError, TypeError):
                pass

    return cleaned


def build_set_local_sql(config: Dict[str, str]) -> List[str]:
    """Generate SET LOCAL statements from a validated config.

    Returns list of SQL strings like ["SET LOCAL work_mem = '512MB'", ...].
    Each should be executed as a separate statement within a transaction.
    """
    statements: List[str] = []
    for param, value in sorted(config.items()):
        if param not in PG_TUNABLE_PARAMS:
            continue
        # Quote the value for safety
        statements.append(f"SET LOCAL {param} = '{value}'")
    return statements


# =========================================================================
# System Introspection + Caching + Resource Envelope
# =========================================================================


@dataclass
class PGSystemProfile:
    """Cached snapshot of pg_settings and connection state."""
    settings: List[Dict[str, Any]]   # Full pg_settings rows
    active_connections: int
    collected_at: str                # ISO timestamp


def collect_system_profile(dsn: str) -> PGSystemProfile:
    """Connect to PG and collect full settings + connection count."""
    from .execution.factory import create_executor_from_dsn

    executor = create_executor_from_dsn(dsn)
    executor.connect()
    try:
        settings, active_connections = executor.get_full_settings()
        return PGSystemProfile(
            settings=settings,
            active_connections=active_connections,
            collected_at=datetime.now().isoformat(),
        )
    finally:
        executor.close()


def load_or_collect_profile(dsn: str, cache_dir: Path) -> PGSystemProfile:
    """Load cached system profile from disk, or collect and cache it.

    Cache file: {cache_dir}/pg_system_profile.json
    """
    cache_dir = Path(cache_dir)
    cache_path = cache_dir / "pg_system_profile.json"

    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text())
            return PGSystemProfile(
                settings=data["settings"],
                active_connections=data["active_connections"],
                collected_at=data["collected_at"],
            )
        except Exception as e:
            logger.warning(f"Failed to load cached PG profile: {e}")

    # Collect fresh
    profile = collect_system_profile(dsn)

    # Cache to disk
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps({
            "settings": profile.settings,
            "active_connections": profile.active_connections,
            "collected_at": profile.collected_at,
        }, indent=2))
        logger.info(f"Cached PG system profile to {cache_path}")
    except Exception as e:
        logger.warning(f"Failed to cache PG profile: {e}")

    return profile


def _format_setting_value(setting: str, unit: Optional[str]) -> str:
    """Format a pg_settings value with its unit."""
    if not unit:
        return setting
    # Convert kB values to human-readable
    if unit == "kB":
        try:
            kb = int(setting)
            if kb >= 1_048_576:
                return f"{kb // 1_048_576}GB"
            if kb >= 1024:
                return f"{kb // 1024}MB"
            return f"{kb}kB"
        except ValueError:
            return f"{setting}{unit}"
    if unit == "8kB":
        try:
            blocks = int(setting)
            mb = (blocks * 8) // 1024
            if mb >= 1024:
                return f"{mb // 1024}GB"
            return f"{mb}MB"
        except ValueError:
            return f"{setting} x 8kB"
    return f"{setting}{unit}"


def build_resource_envelope(profile: PGSystemProfile) -> str:
    """Produce a text block for worker prompts describing system resources.

    Shows memory budget, parallelism, storage hints, and SET LOCAL permissions.
    """
    # Index settings by name for quick lookup
    by_name: Dict[str, Dict[str, Any]] = {}
    for s in profile.settings:
        by_name[s["name"]] = s

    def _get(name: str, default: str = "?") -> str:
        s = by_name.get(name)
        if not s:
            return default
        return _format_setting_value(s["setting"], s.get("unit"))

    def _get_raw(name: str, default: str = "0") -> str:
        s = by_name.get(name)
        return s["setting"] if s else default

    # Categorize by context
    user_params = []
    superuser_params = []
    postmaster_params = []
    for s in profile.settings:
        ctx = s.get("context", "")
        name = s["name"]
        if name not in PG_TUNABLE_PARAMS:
            continue
        if ctx == "user":
            user_params.append(name)
        elif ctx == "superuser":
            superuser_params.append(name)
        elif ctx in ("postmaster", "sighup"):
            postmaster_params.append(name)

    # Work_mem headroom calculation
    active = profile.active_connections
    try:
        work_mem_kb = int(_get_raw("work_mem", "4096"))
        work_mem_mb = work_mem_kb // 1024
    except ValueError:
        work_mem_mb = 4

    lines = [
        f"Memory budget: shared_buffers={_get('shared_buffers')}, "
        f"effective_cache_size={_get('effective_cache_size')}",
        f"Global work_mem: {_get('work_mem')} (per-operation)",
        f"Active connections: ~{active} "
        f"(work_mem headroom: safe up to {min(work_mem_mb * 4, 2048)}MB per-op)",
    ]

    # Storage hint
    try:
        rpc = float(_get_raw("random_page_cost", "4.0"))
        storage = "SSD" if rpc <= 1.5 else "HDD"
        lines.append(
            f"Storage: {storage} (random_page_cost={rpc})"
        )
    except ValueError:
        pass

    # Parallel capacity
    lines.append(
        f"Parallel capacity: max_parallel_workers={_get_raw('max_parallel_workers', '?')}, "
        f"per_gather={_get_raw('max_parallel_workers_per_gather', '?')}"
    )

    # SET LOCAL permissions
    lines.append("")
    lines.append("SET LOCAL permissions:")
    if user_params:
        lines.append(f"  user-level (always available): {', '.join(sorted(user_params))}")
    if superuser_params:
        lines.append(f"  superuser-required: {', '.join(sorted(superuser_params))}")
    if postmaster_params:
        lines.append(f"  postmaster (off-limits): {', '.join(sorted(postmaster_params))}")

    return "\n".join(lines)
