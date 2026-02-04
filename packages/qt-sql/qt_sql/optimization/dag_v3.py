"""
DAG V3 - File-based gold examples with KB pattern matching.

Loads examples from qt_sql/optimization/examples/*.json
Rotates examples on failure based on KB pattern detection.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Optional, Set
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Examples directory
EXAMPLES_DIR = Path(__file__).parent / "examples"


@dataclass
class GoldExample:
    """A verified gold example for a specific transform."""
    id: str
    name: str
    description: str
    benchmark_queries: List[str]
    verified_speedup: str
    example: dict  # Contains opportunity, input_slice, output, key_insight
    example_class: str = "standard"


def load_example(example_id: str) -> Optional[GoldExample]:
    """Load a single example by ID."""
    path = EXAMPLES_DIR / f"{example_id}.json"
    if not path.exists():
        logger.warning(f"Example file not found: {path}")
        return None

    try:
        with open(path) as f:
            data = json.load(f)
        return GoldExample(
            id=data["id"],
            name=data["name"],
            description=data["description"],
            example_class=data.get("example_class", "standard"),
            benchmark_queries=data.get("benchmark_queries", []),
            verified_speedup=data.get("verified_speedup", "unknown"),
            example=data["example"]
        )
    except Exception as e:
        logger.error(f"Failed to load example {example_id}: {e}")
        return None


def load_all_examples() -> List[GoldExample]:
    """Load all examples from the examples directory."""
    examples = []
    if not EXAMPLES_DIR.exists():
        logger.warning(f"Examples directory not found: {EXAMPLES_DIR}")
        return examples

    for path in EXAMPLES_DIR.glob("*.json"):
        example = load_example(path.stem)
        if example:
            examples.append(example)

    return examples


def get_example_ids() -> List[str]:
    """Get list of available example IDs."""
    if not EXAMPLES_DIR.exists():
        return []
    return [p.stem for p in EXAMPLES_DIR.glob("*.json")]


# KB pattern ID -> example ID mapping
KB_TO_EXAMPLE = {
    # High-value patterns (proven 2x+)
    "or_to_union": "or_to_union",
    "correlated_to_cte": "decorrelate",
    "date_cte_isolate": "date_cte_isolate",
    "push_pred": "quantity_range_pushdown",
    "consolidate_scans": "early_filter",

    # Standard patterns
    "multi_push_pred": "multi_push_predicate",
    "materialize_cte": "materialize_cte",
    "flatten_subq": "flatten_subquery",
    "reorder_join": "reorder_join",
    "inline_cte": "inline_cte",
    "remove_redundant": "remove_redundant",
}


def get_matching_examples(sql: str) -> List[GoldExample]:
    """Get gold examples prioritized by KB pattern matches, then all others.

    Strategy:
    1. First: examples matching detected KB patterns (sorted by avg_speedup)
    2. Then: all remaining examples (sorted by avg_speedup)

    This ensures 5 retries can cover 5 different strategies.

    Args:
        sql: The SQL query to analyze

    Returns:
        List of GoldExample objects - matched first, then unmatched
    """
    from .knowledge_base import detect_opportunities, TRANSFORM_REGISTRY

    # Detect KB patterns
    kb_hits = detect_opportunities(sql)

    # Map to example IDs with their scores
    matched_example_ids: Set[str] = set()
    for hit in kb_hits:
        # DetectedOpportunity has pattern.id, not id directly
        pattern_id = hit.pattern.id if hasattr(hit, 'pattern') else str(hit)
        example_id = KB_TO_EXAMPLE.get(pattern_id)
        if example_id:
            matched_example_ids.add(example_id)
            logger.info(f"KB pattern '{pattern_id}' -> example '{example_id}'")

    # Load ALL examples
    all_examples = load_all_examples()

    # Get avg_speedup scores from KB for sorting
    def get_score(ex: GoldExample) -> float:
        # Find matching KB pattern by example ID
        for pattern in TRANSFORM_REGISTRY.values():
            if KB_TO_EXAMPLE.get(pattern.id) == ex.id or KB_TO_EXAMPLE.get(pattern.id.value) == ex.id:
                return pattern.avg_speedup
        return 0.0

    # Split into matched and unmatched
    matched = [ex for ex in all_examples if ex.id in matched_example_ids]
    unmatched = [ex for ex in all_examples if ex.id not in matched_example_ids]

    # Sort both by avg_speedup (highest first)
    matched.sort(key=get_score, reverse=True)
    unmatched.sort(key=get_score, reverse=True)

    # Combine: matched first, then unmatched
    result = matched + unmatched

    logger.info(f"Example order: {[e.id for e in result[:5]]} (matched: {len(matched)}, total: {len(result)})")
    return result


def format_example_for_prompt(example: GoldExample) -> str:
    """Format a single example for inclusion in the prompt."""
    ex = example.example
    output_json = json.dumps(ex["output"], indent=2)

    text = f"## Example: {example.name} ({example.id.upper()})\n"
    text += f"Verified speedup: {example.verified_speedup}\n\n"
    if example.example_class and example.example_class != "standard":
        text += f"Class: {example.example_class}\n\n"
    text += f"### Input:\n{ex['input_slice']}\n\n"
    text += f"### Output:\n```json\n{output_json}\n```\n"

    if "key_insight" in ex:
        text += f"\n**Key insight:** {ex['key_insight']}\n"

    return text


def build_prompt_with_examples(
    base_prompt: str,
    examples: List[GoldExample],
    execution_plan: str = "",
    history_section: str = ""
) -> str:
    """Build full prompt with multiple gold examples.

    Args:
        base_prompt: The DAG structure prompt
        examples: The gold examples to include
        execution_plan: Optional execution plan
        history_section: Optional history of previous attempts

    Returns:
        Complete prompt string
    """
    parts = []

    # Example sections
    for example in examples:
        parts.append(format_example_for_prompt(example))
        parts.append("\n---\n")

    # Base DAG prompt
    parts.append(base_prompt)

    # Execution plan
    if execution_plan and execution_plan != "(execution plan not available)":
        parts.append(f"\n## Execution Plan\n```\n{execution_plan}\n```\n")

    # History
    if history_section:
        parts.append(f"\n{history_section}")
        if examples:
            tried_ids = ", ".join(ex.id for ex in examples)
            parts.append(f"\n**IMPORTANT:** The following patterns were provided: {tried_ids}. Consider a DIFFERENT approach!\n")

    return "\n".join(parts)


class DagV3ExampleSelector:
    """Manages example selection and rotation for adaptive rewriting.

    Examples are ordered: KB-matched first (by score), then unmatched (by score).
    Each retry uses a different example - no cycling until all are tried.
    """

    def __init__(self, sql: str, examples_per_prompt: int = 3):
        """Initialize with the SQL query to optimize.

        Args:
            sql: The SQL query - used to detect matching KB patterns
            examples_per_prompt: Number of examples to include per prompt
        """
        self.sql = sql
        self.examples = get_matching_examples(sql)
        self.current_index = 0
        self.examples_per_prompt = max(1, examples_per_prompt)

    @property
    def current_example(self) -> Optional[GoldExample]:
        """Get the current example."""
        if not self.examples or self.current_index >= len(self.examples):
            return None
        return self.examples[self.current_index]

    @property
    def current_examples(self) -> List[GoldExample]:
        """Get the current batch of examples."""
        if not self.examples or self.current_index >= len(self.examples):
            return []
        end_index = min(self.current_index + self.examples_per_prompt, len(self.examples))
        return self.examples[self.current_index:end_index]

    @property
    def remaining_examples(self) -> int:
        """Number of untried examples remaining."""
        return max(0, len(self.examples) - self.current_index)

    def rotate(self) -> Optional[GoldExample]:
        """Move to the next example after a failure.

        Returns:
            The new current example, or None if all exhausted
        """
        if not self.examples:
            return None

        self.current_index += self.examples_per_prompt

        if self.current_index >= len(self.examples):
            logger.info(f"All {len(self.examples)} examples exhausted")
            return None

        logger.info(
            "Rotated to examples %d-%d/%d: %s",
            self.current_index + 1,
            min(self.current_index + self.examples_per_prompt, len(self.examples)),
            len(self.examples),
            ", ".join(ex.id for ex in self.current_examples)
        )
        return self.current_example

    def get_prompt(self, base_prompt: str, execution_plan: str = "", history: str = "") -> str:
        """Get prompt with current example.

        Args:
            base_prompt: The DAG structure prompt
            execution_plan: Optional execution plan
            history: Optional attempt history

        Returns:
            Complete prompt string
        """
        examples = self.current_examples
        if not examples:
            return base_prompt

        return build_prompt_with_examples(base_prompt, examples, execution_plan, history)


# Convenience function for backwards compatibility
def get_dag_v3_examples() -> List[dict]:
    """Get all examples in dict format (for backwards compatibility)."""
    examples = load_all_examples()
    return [
        {
            "opportunity": ex.example.get("opportunity", ex.name),
            "input_slice": ex.example.get("input_slice", ""),
            "output": ex.example.get("output", {}),
            "key_insight": ex.example.get("key_insight", ""),
            "speedup": ex.verified_speedup,
            "example_class": ex.example_class,
        }
        for ex in examples
    ]
