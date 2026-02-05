"""DSB benchmark query loading utilities.

This module provides utilities for loading DSB (and TPC-DS) benchmark
queries from file directories.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def normalize_query_id(query_id: str) -> str:
    """Normalize query ID to 'qN' format.

    Args:
        query_id: Query ID in various formats (q1, query1, 1, etc.)

    Returns:
        Normalized ID like 'q1', 'q15', etc.
    """
    # Extract numeric part
    match = re.search(r'(\d+)', query_id)
    if match:
        num = int(match.group(1))
        return f"q{num}"

    return query_id.lower()


def load_dsb_query(query_id: str, queries_dir: Path) -> Optional[str]:
    """Load a single DSB query by ID.

    Args:
        query_id: Query ID (e.g., 'q1', 'query1', '1')
        queries_dir: Directory containing query files

    Returns:
        SQL query text, or None if not found
    """
    # Normalize to numeric
    normalized = normalize_query_id(query_id)
    match = re.search(r'(\d+)', normalized)
    if not match:
        logger.warning(f"Invalid query ID: {query_id}")
        return None

    num = int(match.group(1))

    # Try multiple naming patterns (DSB uses query_N.sql format)
    patterns = [
        f"query_{num}.sql",
        f"query{num:02d}.sql",
        f"query{num}.sql",
        f"q{num}.sql",
        f"q{num:02d}.sql",
    ]

    for pattern in patterns:
        path = queries_dir / pattern
        if path.exists():
            logger.debug(f"Found query file: {path}")
            return path.read_text(encoding="utf-8")

    logger.warning(f"Query file not found for {query_id} in {queries_dir}")
    return None


def load_dsb_queries(query_ids: List[str], queries_dir: Path) -> Dict[str, str]:
    """Load multiple DSB queries.

    Args:
        query_ids: List of query IDs to load
        queries_dir: Directory containing query files

    Returns:
        Dict of {normalized_query_id: sql}
    """
    queries = {}

    for qid in query_ids:
        sql = load_dsb_query(qid, queries_dir)
        if sql:
            normalized = normalize_query_id(qid)
            queries[normalized] = sql

    logger.info(f"Loaded {len(queries)}/{len(query_ids)} queries from {queries_dir}")
    return queries


def get_all_dsb_query_ids(queries_dir: Path) -> List[str]:
    """Get all available DSB query IDs from a directory.

    Args:
        queries_dir: Directory containing query files

    Returns:
        List of query IDs in 'qN' format, sorted numerically
    """
    if not queries_dir.exists():
        logger.warning(f"Queries directory not found: {queries_dir}")
        return []

    query_ids = []

    # Look for query_N.sql pattern
    for path in queries_dir.glob("query_*.sql"):
        match = re.search(r'query_(\d+)\.sql', path.name)
        if match:
            num = int(match.group(1))
            query_ids.append(f"q{num}")

    # Also look for qN.sql pattern
    for path in queries_dir.glob("q*.sql"):
        match = re.search(r'q(\d+)\.sql', path.name)
        if match:
            num = int(match.group(1))
            qid = f"q{num}"
            if qid not in query_ids:
                query_ids.append(qid)

    # Sort numerically
    query_ids.sort(key=lambda x: int(re.search(r'\d+', x).group()))

    logger.info(f"Found {len(query_ids)} queries in {queries_dir}")
    return query_ids


def load_all_dsb_queries(queries_dir: Path) -> Dict[str, str]:
    """Load all DSB queries from a directory.

    Args:
        queries_dir: Directory containing query files

    Returns:
        Dict of {query_id: sql}
    """
    query_ids = get_all_dsb_query_ids(queries_dir)
    return load_dsb_queries(query_ids, queries_dir)


def parse_query_list(query_spec: str) -> List[str]:
    """Parse a query specification string.

    Supports:
    - 'all' - all queries
    - 'q1,q2,q3' - comma-separated list
    - '1,2,3' - numeric list
    - 'q1-q10' - range (inclusive)
    - '1-10' - numeric range

    Args:
        query_spec: Query specification string

    Returns:
        List of normalized query IDs
    """
    if query_spec.lower() == 'all':
        return ['all']

    # Handle range syntax
    if '-' in query_spec and ',' not in query_spec:
        match = re.match(r'q?(\d+)-q?(\d+)', query_spec, re.IGNORECASE)
        if match:
            start, end = int(match.group(1)), int(match.group(2))
            return [f"q{i}" for i in range(start, end + 1)]

    # Handle comma-separated list
    parts = [p.strip() for p in query_spec.split(',')]
    return [normalize_query_id(p) for p in parts if p]
