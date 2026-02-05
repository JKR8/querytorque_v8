"""Prompt building for ADO - matches V3 structure exactly.

Uses DagV2Pipeline for DAG parsing (Target Nodes, Subgraph Slice, Node Contracts).
Loads examples and constraints from ado/ directories.

Structure:
1. Examples (knowledge base)
2. Constraints (knowledge base)
3. System prompt + OUTPUT FORMAT
4. DAG structure (Target Nodes, Subgraph Slice, Node Contracts)
5. Execution Plan
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Directory paths
BASE_DIR = Path(__file__).resolve().parent
EXAMPLES_DIR = BASE_DIR / "examples"
CONSTRAINTS_DIR = BASE_DIR / "constraints"


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class GoldExample:
    """A verified gold example for a specific transform."""
    id: str
    name: str
    description: str
    verified_speedup: str
    example: Dict[str, Any]  # Contains before_sql/input_slice, output, key_insight
    benchmark_queries: List[str] = field(default_factory=list)
    example_class: str = "standard"


@dataclass
class Constraint:
    """A constraint learned from benchmark failures."""
    id: str
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW
    description: str
    prompt_instruction: str
    observed_failures: List[Dict[str, Any]] = field(default_factory=list)


# =============================================================================
# Loading Functions
# =============================================================================

def load_example(example_id: str) -> Optional[GoldExample]:
    """Load a single example by ID from ado/examples/."""
    path = EXAMPLES_DIR / f"{example_id}.json"
    if not path.exists():
        # Try with different extensions/patterns
        for p in EXAMPLES_DIR.glob(f"*{example_id}*.json"):
            path = p
            break
        else:
            return None

    try:
        data = json.loads(path.read_text())
        return GoldExample(
            id=data.get("id", example_id),
            name=data.get("name", example_id),
            description=data.get("description", ""),
            verified_speedup=data.get("verified_speedup", ""),
            example=data.get("example", {}),
            benchmark_queries=data.get("benchmark_queries", []),
            example_class=data.get("example_class", "standard"),
        )
    except Exception as e:
        logger.warning(f"Failed to load example {example_id}: {e}")
        return None


def load_all_examples() -> List[GoldExample]:
    """Load all examples from ado/examples/ directory."""
    if not EXAMPLES_DIR.exists():
        return []

    examples = []
    for path in sorted(EXAMPLES_DIR.glob("*.json")):
        example = load_example(path.stem)
        if example:
            examples.append(example)

    return examples


def load_all_constraints() -> List[Constraint]:
    """Load all constraints from ado/constraints/ directory."""
    if not CONSTRAINTS_DIR.exists():
        return []

    constraints = []
    for path in sorted(CONSTRAINTS_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text())
            if data.get("prompt_instruction"):  # Only include if has instruction
                constraints.append(Constraint(
                    id=data.get("id", path.stem),
                    severity=data.get("severity", "MEDIUM"),
                    description=data.get("description", ""),
                    prompt_instruction=data.get("prompt_instruction", ""),
                    observed_failures=data.get("observed_failures", []),
                ))
        except Exception as e:
            logger.warning(f"Failed to load constraint {path}: {e}")

    # Sort by severity: CRITICAL > HIGH > MEDIUM > LOW
    order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    constraints.sort(key=lambda c: order.get(c.severity, 9))

    return constraints


# =============================================================================
# Formatting Functions (match V3 exactly)
# =============================================================================

def format_example_for_prompt(example: GoldExample) -> str:
    """Format a single example for inclusion in the prompt (V3 format)."""
    ex = example.example

    lines = [f"## Example: {example.name} ({example.id.upper()})"]
    lines.append(f"Verified speedup: {example.verified_speedup}")

    if example.example_class and example.example_class != "standard":
        lines.append(f"Class: {example.example_class}")

    lines.append("")

    # Input section - use input_slice if available, else before_sql
    input_slice = ex.get("input_slice") or ex.get("before_sql", "")
    if input_slice:
        # Format as [node_id]: SQL if not already formatted
        if not input_slice.strip().startswith("["):
            input_slice = f"[main_query]:\n{input_slice}"
        lines.append(f"### Input:\n{input_slice}")
        lines.append("")

    # Output section - JSON rewrite_sets
    if "output" in ex:
        output_json = json.dumps(ex["output"], indent=2)
        lines.append(f"### Output:\n```json\n{output_json}\n```")

    # Key insight
    if "key_insight" in ex:
        lines.append(f"\n**Key insight:** {ex['key_insight']}")

    return "\n".join(lines)


def format_constraints_for_prompt(constraints: List[Constraint]) -> str:
    """Format constraints for inclusion in the prompt (V3 format)."""
    if not constraints:
        return ""

    lines = ["## CONSTRAINTS (Learned from Benchmark Failures)\n"]
    lines.append("The following constraints are MANDATORY based on observed failures:\n")

    for c in constraints:
        lines.append(f"### {c.id} [{c.severity}]")
        lines.append(c.prompt_instruction)
        lines.append("")

    return "\n".join(lines)


# =============================================================================
# DAG Structure (uses DagV2Pipeline)
# =============================================================================

def get_dag_base_prompt(sql: str, dialect: str = "postgres") -> str:
    """Get DAG structure prompt using DagV2Pipeline.

    Returns the base prompt with:
    - System prompt + rules + OUTPUT FORMAT
    - Target Nodes
    - Subgraph Slice
    - Node Contracts
    - Downstream Usage
    - Cost Attribution
    """
    try:
        from qt_sql.optimization.dag_v2 import DagV2Pipeline
        pipeline = DagV2Pipeline(sql)
        return pipeline.get_prompt()
    except ImportError:
        logger.warning("DagV2Pipeline not available, using fallback")
        return _fallback_base_prompt(sql)
    except Exception as e:
        logger.warning(f"DagV2Pipeline failed: {e}, using fallback")
        return _fallback_base_prompt(sql)


def _fallback_base_prompt(sql: str) -> str:
    """Fallback base prompt if DagV2Pipeline unavailable."""
    return f"""{SYSTEM_PROMPT}

## Target Nodes
  [main_query]

## Subgraph Slice
[main_query] type=main
```sql
{sql}
```

## Node Contracts
[main_query]:
  output_columns: (not parsed)
  required_predicates: (not parsed)

Now output your rewrite_sets:"""


# =============================================================================
# System Prompt (matches V3)
# =============================================================================

ALLOWED_TRANSFORMS = [
    "pushdown", "decorrelate", "or_to_union", "early_filter",
    "date_cte_isolate", "materialize_cte", "flatten_subquery",
    "reorder_join", "multi_push_predicate", "inline_cte",
    "remove_redundant", "semantic_rewrite",
]

SYSTEM_PROMPT = f"""You are an autonomous Query Rewrite Engine. Your goal is to maximize execution speed while strictly preserving semantic invariants.

Output atomic rewrite sets in JSON.

RULES:
- Primary Goal: Maximize execution speed while strictly preserving semantic invariants.
- Allowed Transforms: Use the provided list. If a standard SQL optimization applies that is not listed, label it "semantic_rewrite".
- Atomic Sets: Group dependent changes (e.g., creating a CTE and joining it) into a single rewrite_set.
- Contracts: Output columns, grain, and total result rows must remain invariant.
- Naming: Use descriptive CTE names (e.g., `filtered_returns` vs `cte1`).
- Column Aliasing: Permitted only for aggregations or disambiguation.

ALLOWED TRANSFORMS: {", ".join(ALLOWED_TRANSFORMS)}

OUTPUT FORMAT:
```json
{{
  "rewrite_sets": [
    {{
      "id": "rs_01",
      "transform": "transform_name",
      "nodes": {{
        "node_id": "new SQL..."
      }},
      "invariants_kept": ["list of preserved semantics"],
      "expected_speedup": "2x",
      "risk": "low"
    }}
  ],
  "explanation": "what was changed and why"
}}
```"""


# =============================================================================
# Main Prompt Builder (V3 structure)
# =============================================================================

def build_prompt_with_examples(
    base_prompt: str,
    examples: List[GoldExample],
    execution_plan: str = "",
    history_section: str = "",
    include_constraints: bool = True,
) -> str:
    """Build full prompt with examples and constraints (V3 structure).

    Order:
    1. Examples (knowledge base)
    2. Constraints (knowledge base)
    3. Base DAG prompt (system prompt + DAG structure)
    4. Execution plan
    5. History (if any)
    """
    parts = []

    # 1. Examples first (knowledge base)
    for example in examples:
        parts.append(format_example_for_prompt(example))
        parts.append("\n---\n")

    # 2. Constraints (knowledge base, after examples)
    if include_constraints:
        constraints = load_all_constraints()
        constraints_section = format_constraints_for_prompt(constraints)
        if constraints_section:
            parts.append(constraints_section)
            parts.append("\n---\n")

    # 3. Base DAG prompt (system prompt + Target Nodes + Subgraph Slice + Contracts)
    parts.append(base_prompt)

    # 4. Execution plan
    if execution_plan and execution_plan.strip():
        parts.append(f"\n## Execution Plan\n```\n{execution_plan}\n```\n")

    # 5. History
    if history_section:
        parts.append(f"\n{history_section}")
        if examples:
            tried_ids = ", ".join(ex.id for ex in examples)
            parts.append(f"\n**IMPORTANT:** The following patterns were provided: {tried_ids}. Consider a DIFFERENT approach!\n")

    return "\n".join(parts)


def build_optimization_prompt(
    original_sql: str,
    execution_plan: str = "",
    examples: Optional[List[GoldExample]] = None,
    constraints: Optional[List[Constraint]] = None,
    history: str = "",
    dialect: str = "postgres",
    max_examples: int = 3,
) -> str:
    """Build complete optimization prompt (convenience function).

    Args:
        original_sql: The SQL query to optimize
        execution_plan: EXPLAIN output
        examples: Gold examples (loads all if None)
        constraints: Constraints (loads all if None)
        history: Previous attempt history
        dialect: SQL dialect
        max_examples: Maximum examples to include

    Returns:
        Complete prompt string matching V3 structure
    """
    # Load examples if not provided
    if examples is None:
        examples = load_all_examples()

    # Get DAG base prompt
    base_prompt = get_dag_base_prompt(original_sql, dialect=dialect)

    # Build full prompt
    return build_prompt_with_examples(
        base_prompt=base_prompt,
        examples=examples[:max_examples],
        execution_plan=execution_plan,
        history_section=history,
        include_constraints=(constraints is None),  # Load if not provided
    )


# =============================================================================
# PromptBuilder Class (for ADO runner compatibility)
# =============================================================================

class PromptBuilder:
    """Prompt builder with example rotation support."""

    def __init__(self, examples_per_prompt: int = 3):
        self.examples = load_all_examples()
        self.constraints = load_all_constraints()
        self.examples_per_prompt = examples_per_prompt
        self.current_index = 0

    @property
    def current_examples(self) -> List[GoldExample]:
        """Get current batch of examples."""
        end = min(self.current_index + self.examples_per_prompt, len(self.examples))
        return self.examples[self.current_index:end]

    def rotate_examples(self) -> None:
        """Rotate to next batch of examples."""
        self.current_index += self.examples_per_prompt
        if self.current_index >= len(self.examples):
            self.current_index = 0

    def build(
        self,
        original_sql: str,
        execution_plan: str = "",
        history: str = "",
        use_specific_examples: Optional[List[GoldExample]] = None,
        dialect: str = "postgres",
    ) -> str:
        """Build optimization prompt.

        Args:
            original_sql: SQL to optimize
            execution_plan: EXPLAIN output
            history: Previous attempt history
            use_specific_examples: Override current examples
            dialect: SQL dialect

        Returns:
            Complete prompt string (V3 structure)
        """
        examples = use_specific_examples or self.current_examples

        # Get DAG base prompt
        base_prompt = get_dag_base_prompt(original_sql, dialect=dialect)

        return build_prompt_with_examples(
            base_prompt=base_prompt,
            examples=examples,
            execution_plan=execution_plan,
            history_section=history,
            include_constraints=True,
        )
