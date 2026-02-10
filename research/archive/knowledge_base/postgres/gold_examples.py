"""
PostgreSQL Gold Examples for Few-Shot Learning

TEMPLATE FILE - Copy patterns from duckdb/gold_examples.py and validate on PostgreSQL.

PostgreSQL may behave differently for:
- UNION ALL (no parallel branch execution like DuckDB)
- CTE materialization (may need MATERIALIZED hint)
- EXISTS vs IN (planner behaves differently)

Validate each pattern before adding here.
"""

import dspy
from typing import List


def get_gold_examples(num_examples: int = 3) -> List[dspy.Example]:
    """Get PostgreSQL gold examples for few-shot learning.

    Currently empty - populate as patterns are validated on PostgreSQL.

    Args:
        num_examples: Number of examples to return

    Returns:
        List of dspy.Example objects (empty for now)
    """
    # TODO: Validate DuckDB patterns on PostgreSQL and add confirmed ones here
    return []
