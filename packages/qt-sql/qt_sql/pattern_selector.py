"""Pattern selector — classify SQL → pattern IDs using a small model.

Uses a local Ollama or any OpenAI-compatible endpoint to select the most
relevant gold optimization patterns for a given SQL query. Falls back
gracefully (returns []) on any error so tag-based matching takes over.

Fallback chain:
  1. Pattern selector model → parse JSON array of IDs
  2. Tag overlap matching (zero-cost deterministic fallback in knowledge.py)
  3. Analyst/main LLM picks during optimization (existing)
"""

from __future__ import annotations

import json
import logging
import re
import ssl
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "You are a SQL pattern matcher. Given a SQL query, rank the top 12 "
    "most relevant optimization patterns from the numbered list. Return "
    "ONLY a JSON array of 12 numbers ordered best match first. Example: "
    "[5,2,11,1,8,3,9,7,4,6,10,12]. No explanation."
)

# Structural signal tags (NOT table names) — used in the pattern table
_SIGNAL_TAGS = {
    "intersect", "except", "union", "rollup", "cube", "grouping",
    "exists", "case", "having", "distinct", "lateral", "recursive",
    "between", "like", "in", "window", "rank", "row_number",
    "subquery", "cte", "join", "aggregate", "group_by", "order_by",
    "self_join", "repeated_scan", "multi_cte", "correlated_subquery",
    "left_join", "outer_join", "cross_join", "or_predicate",
    "decorrelate", "pushdown", "prefetch", "materialize", "correlated",
}

# ---------------------------------------------------------------------------
# Tag entries cache
# ---------------------------------------------------------------------------

_TAG_CACHE: list[dict] | None = None


def _load_tag_entries() -> list[dict]:
    """Load and cache tag entries from similarity_tags.json."""
    global _TAG_CACHE
    if _TAG_CACHE is not None:
        return _TAG_CACHE

    tags_file = Path(__file__).resolve().parent / "models" / "similarity_tags.json"
    if not tags_file.exists():
        _TAG_CACHE = []
        return _TAG_CACHE

    try:
        data = json.loads(tags_file.read_text())
        _TAG_CACHE = data.get("examples", [])
    except Exception as e:
        logger.warning(f"Failed to load tag entries: {e}")
        _TAG_CACHE = []

    return _TAG_CACHE


# ---------------------------------------------------------------------------
# Pattern table builder
# ---------------------------------------------------------------------------

def build_pattern_table(
    tag_entries: list[dict],
    engine: str,
    include_seed: bool = False,
) -> tuple[str, dict[int, str]]:
    """Build a numbered pattern table for the selector prompt.

    Filters entries by engine (duckdb/postgres). By default shows only
    gold examples for the target engine. When include_seed=True, also
    includes seed catalog rules (engine="seed").

    Args:
        tag_entries: List of tag entry dicts from similarity_tags.json.
        engine: Target engine — "duckdb" or "postgres".
        include_seed: If True, include seed catalog rules alongside gold.

    Returns:
        Tuple of (table_string, num_to_id_mapping).
        Table uses numbered rows (1, 2, 3...) for compact model output.
        num_to_id maps those numbers back to pattern IDs.
    """
    # Allowed engine values: always include target engine
    allowed_engines = {engine}
    if include_seed:
        allowed_engines.add("seed")

    lines = ["# | Signals", "---|---"]
    num_to_id: dict[int, str] = {}
    num = 0
    for ex in tag_entries:
        ex_engine = ex.get("engine", "")
        ex_type = ex.get("type", "gold")

        # Skip regressions
        if ex_type == "regression":
            continue
        # Engine filter: must match target engine (or "seed" when included)
        if ex_engine not in allowed_engines:
            continue

        num += 1
        num_to_id[num] = ex["id"]
        meta = ex.get("metadata", {})
        desc = meta.get("description", "")[:60]

        # Extract structural signal tags only (exclude table names)
        tags = ex.get("tags", [])
        signals = sorted(t for t in tags if t in _SIGNAL_TAGS)

        signals_str = "; ".join(filter(None, [desc, ", ".join(signals)]))
        lines.append(f"{num} | {signals_str}")

    return "\n".join(lines), num_to_id


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

_FEW_SHOT = """### Example

SQL with correlated subquery, OR predicates, and star-schema date/store joins:
Answer: [4,13,3,6,7,15,11,14,2,9,18,1]"""


def build_selector_prompt(sql: str, pattern_table: str) -> str:
    """Assemble the user message for the pattern selector model.

    Args:
        sql: The SQL query to classify.
        pattern_table: Output of ``build_pattern_table()``.

    Returns:
        Full user-message string ready for chat completion.
    """
    return (
        f"### Patterns\n\n{pattern_table}\n\n"
        f"{_FEW_SHOT}\n\n"
        f"### SQL:\n{sql}\n\nAnswer:"
    )


# ---------------------------------------------------------------------------
# Model caller (generic OpenAI-compatible)
# ---------------------------------------------------------------------------

def call_model(
    prompt: str,
    system: str,
    url: str,
    model: str,
    api_key: str = "",
    timeout: int = 10,
) -> str:
    """Call an OpenAI-compatible chat completion endpoint.

    Works with Ollama (localhost, no key), cloud Qwen, or any
    OpenAI-compatible API.

    Args:
        prompt: User message content.
        system: System message content.
        url: Base URL (e.g. ``http://localhost:11434``).
        model: Model name/ID.
        api_key: Bearer token. Omitted from headers if empty.
        timeout: Request timeout in seconds.

    Returns:
        The assistant's response text.

    Raises:
        Exception: On HTTP or parsing errors.
    """
    endpoint = f"{url.rstrip('/')}/v1/chat/completions"
    payload = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "max_tokens": 100,
        "stream": False,
    }).encode()

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = urllib.request.Request(endpoint, data=payload, headers=headers)

    # Allow self-signed certs for localhost
    ctx = None
    if "localhost" in url or "127.0.0.1" in url:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
        body = json.loads(resp.read().decode())

    return body["choices"][0]["message"]["content"]


# ---------------------------------------------------------------------------
# Response parser
# ---------------------------------------------------------------------------

def parse_pattern_ids(raw: str, num_to_id: dict[int, str]) -> list[str]:
    """Parse numbered model response into a ranked list of pattern IDs.

    Model outputs numbers like [4,13,3,6,...]. Maps back to pattern IDs
    using the num_to_id lookup from build_pattern_table().

    Args:
        raw: Raw model response text (JSON array of ints).
        num_to_id: Mapping from row number → pattern ID.

    Returns:
        List of pattern IDs in ranked order (best first).
    """
    nums: list[int] = []

    # Try clean JSON array
    try:
        parsed = json.loads(raw.strip())
        if isinstance(parsed, list):
            nums = [int(x) for x in parsed if str(x).isdigit()]
    except (json.JSONDecodeError, ValueError):
        pass

    # Fallback: extract bare numbers
    if not nums:
        nums = [int(x) for x in re.findall(r'\b(\d+)\b', raw)]

    # Map to IDs, deduplicate, skip invalid numbers
    seen: set[int] = set()
    result: list[str] = []
    for n in nums:
        if n in num_to_id and n not in seen:
            seen.add(n)
            result.append(num_to_id[n])

    return result


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------

def select_patterns(
    sql: str,
    dialect: str = "duckdb",
    engine: str = "duckdb",
    k: int = 5,
    include_seed: bool = False,
) -> list[str]:
    """Select optimization patterns for a SQL query using a small model.

    Gracefully returns [] on any failure (timeout, model error, etc.)
    so the caller falls back to tag-based matching.

    Args:
        sql: The SQL query to classify.
        dialect: SQL dialect for context.
        engine: Target engine — "duckdb" or "postgres".
        k: Maximum number of patterns to return.
        include_seed: If True, include seed catalog rules alongside gold.

    Returns:
        List of pattern IDs, or [] on failure / disabled.
    """
    try:
        from qt_shared.config import get_settings
        settings = get_settings()
    except Exception:
        return []

    url = settings.pattern_selector_url
    model = settings.pattern_selector_model
    api_key = settings.pattern_selector_api_key

    # Disabled when URL is empty
    if not url:
        return []

    try:
        tag_entries = _load_tag_entries()
        if not tag_entries:
            return []

        table, num_to_id = build_pattern_table(tag_entries, engine, include_seed=include_seed)
        prompt = build_selector_prompt(sql, table)

        raw = call_model(prompt, _SYSTEM_PROMPT, url, model, api_key, timeout=15)
        result = parse_pattern_ids(raw, num_to_id)
        logger.info(f"Pattern selector returned {len(result)} patterns: {result}")
        return result[:k]

    except Exception as e:
        logger.debug(f"Pattern selector failed: {e}")
        return []
