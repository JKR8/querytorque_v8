"""
DAG v1 Optimizer - ARCHIVED FOR RESEARCH

This is the original DAG-based optimizer that was replaced by DAG v2.
Kept for reference and research purposes.

Key differences from DAG v2:
- Uses DSPy ChainOfThought for optimization
- Relies on sql_dag.py for DAG construction
- No node contracts or rewrite sets
- Had issues with assembling new CTEs added by LLM

Replaced by: qt_sql/optimization/dag_v2.py
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Callable
import json

# Note: This requires dspy to be installed
# import dspy


@dataclass
class DagOptimizationResult:
    """Result of a DAG-based optimization attempt."""
    original_sql: str
    optimized_sql: str
    rewrites: Dict[str, str]  # node_id -> new SQL
    explanation: str
    correct: Optional[bool] = None
    attempts: int = 1
    error: Optional[str] = None


def load_dag_gold_examples(num_examples: int = 3):
    """Load DAG-format gold examples for few-shot learning.

    Args:
        num_examples: Number of examples to load (default 3)

    Returns:
        List of dspy.Example objects
    """
    try:
        from research.knowledge_base.examples.dag_gold_examples import get_dag_gold_examples
        return get_dag_gold_examples(num_examples)
    except ImportError:
        return []


# class DagOptimizationPipeline(dspy.Module):
#     """DAG-based optimization pipeline with validation and retries.
#
#     Key difference from ValidatedOptimizationPipeline:
#     - Uses DAG structure for targeted node rewrites
#     - Outputs node-level changes, not full SQL
#     - Better for large queries (less token usage)
#     - Preserves unchanged parts exactly
#     - Uses DAG-format few-shot examples for consistent output
#     """
#
#     def __init__(
#         self,
#         validator_fn: Callable = None,
#         max_retries: int = 2,
#         model_name: str = None,
#         db_name: str = None,
#         use_few_shot: bool = True,
#         num_examples: int = 3,
#     ):
#         """
#         Args:
#             validator_fn: Function(original_sql, optimized_sql) -> (correct, error)
#             max_retries: Maximum retry attempts
#             model_name: Model name for constraints
#             db_name: Database name for hints
#             use_few_shot: Whether to use few-shot examples (default True)
#             num_examples: Number of few-shot examples to use (default 3)
#         """
#         super().__init__()
#         self.optimizer = dspy.ChainOfThought(SQLDagOptimizer)
#         self.retry_optimizer = dspy.ChainOfThought(SQLDagOptimizerWithFeedback)
#         self.validator_fn = validator_fn
#         self.max_retries = max_retries
#         self.constraints = build_system_prompt(model_name, db_name)
#
#         # Load DAG-format few-shot examples
#         if use_few_shot:
#             examples = load_dag_gold_examples(num_examples)
#             if examples:
#                 if hasattr(self.optimizer, 'predict') and hasattr(self.optimizer.predict, 'demos'):
#                     self.optimizer.predict.demos = examples
#                 elif hasattr(self.optimizer, 'demos'):
#                     self.optimizer.demos = examples
#
#     def _parse_rewrites(self, rewrites_str: str) -> Dict[str, str]:
#         """Parse rewrites JSON from LLM output."""
#         text = rewrites_str.strip()
#
#         # Remove markdown code blocks
#         if text.startswith("```"):
#             lines = text.split("\n")
#             text = "\n".join(lines[1:])
#             if text.endswith("```"):
#                 text = text[:-3].strip()
#
#         try:
#             return json.loads(text)
#         except json.JSONDecodeError:
#             # Try to extract JSON object
#             start = text.find("{")
#             end = text.rfind("}") + 1
#             if start != -1 and end > start:
#                 try:
#                     return json.loads(text[start:end])
#                 except json.JSONDecodeError:
#                     pass
#             return {}
#
#     def forward(
#         self,
#         sql: str,
#         plan: str = "",
#         dag = None,
#     ) -> DagOptimizationResult:
#         """Run DAG-based optimization with validation and retries.
#
#         Args:
#             sql: Original SQL query
#             plan: Execution plan summary
#             dag: Pre-built SQLDag (optional, will build if not provided)
#
#         Returns:
#             DagOptimizationResult with optimized SQL and rewrites
#         """
#         # Build DAG if not provided
#         if dag is None:
#             from .sql_dag import SQLDag
#             dag = SQLDag.from_sql(sql)
#
#         # Build DAG prompt components
#         dag_structure = []
#         dag_structure.append("Nodes:")
#         for node_id in dag.topological_order():
#             node = dag.nodes[node_id]
#             parts = [f"  [{node_id}]", f"type={node.node_type}"]
#             if node.tables:
#                 parts.append(f"tables={node.tables}")
#             if node.cte_refs:
#                 parts.append(f"refs={node.cte_refs}")
#             if node.is_correlated:
#                 parts.append("CORRELATED")
#             dag_structure.append(" ".join(parts))
#
#         dag_structure.append("\nEdges:")
#         for edge in dag.edges:
#             dag_structure.append(f"  {edge.source} → {edge.target}")
#
#         query_dag = "\n".join(dag_structure)
#
#         # Build node SQL
#         node_sql_parts = []
#         for node_id in dag.topological_order():
#             node = dag.nodes[node_id]
#             if node.sql:
#                 node_sql_parts.append(f"### {node_id}\n```sql\n{node.sql.strip()}\n```")
#
#         node_sql = "\n\n".join(node_sql_parts)
#
#         # Detect relevant optimization patterns
#         hints = detect_knowledge_patterns(sql)
#
#         attempts = 0
#         last_rewrites_str = ""
#         last_error = None
#
#         # First attempt
#         attempts += 1
#         result = self.optimizer(
#             query_dag=query_dag,
#             node_sql=node_sql,
#             execution_plan=plan,
#             optimization_hints=hints,
#             constraints=self.constraints
#         )
#
#         rewrites = self._parse_rewrites(result.rewrites)
#         explanation = result.explanation
#
#         # Apply rewrites
#         if rewrites:
#             optimized_sql = dag.apply_rewrites(rewrites)
#         else:
#             optimized_sql = sql
#
#         # Validate if validator provided
#         if self.validator_fn:
#             correct, error = self.validator_fn(sql, optimized_sql)
#
#             if correct:
#                 return DagOptimizationResult(
#                     original_sql=sql,
#                     optimized_sql=optimized_sql,
#                     rewrites=rewrites,
#                     explanation=explanation,
#                     correct=True,
#                     attempts=attempts
#                 )
#
#             # Validation failed - retry with feedback
#             last_rewrites_str = result.rewrites
#             last_error = error or "Results don't match original query"
#
#             while attempts < self.max_retries + 1:
#                 attempts += 1
#
#                 retry_result = self.retry_optimizer(
#                     query_dag=query_dag,
#                     node_sql=node_sql,
#                     execution_plan=plan,
#                     optimization_hints=hints,
#                     constraints=self.constraints,
#                     previous_rewrites=last_rewrites_str,
#                     failure_reason=last_error
#                 )
#
#                 rewrites = self._parse_rewrites(retry_result.rewrites)
#                 explanation = retry_result.explanation
#
#                 if rewrites:
#                     optimized_sql = dag.apply_rewrites(rewrites)
#                 else:
#                     optimized_sql = sql
#
#                 correct, error = self.validator_fn(sql, optimized_sql)
#
#                 if correct:
#                     return DagOptimizationResult(
#                         original_sql=sql,
#                         optimized_sql=optimized_sql,
#                         rewrites=rewrites,
#                         explanation=f"[After {attempts} attempts] {explanation}",
#                         correct=True,
#                         attempts=attempts
#                     )
#
#                 last_rewrites_str = retry_result.rewrites
#                 last_error = error or "Results don't match"
#
#             # All retries exhausted
#             return DagOptimizationResult(
#                 original_sql=sql,
#                 optimized_sql=optimized_sql,
#                 rewrites=rewrites,
#                 explanation=explanation,
#                 correct=False,
#                 attempts=attempts,
#                 error=f"Validation failed after {attempts} attempts: {last_error}"
#             )
#
#         # No validator - return unvalidated result
#         return DagOptimizationResult(
#             original_sql=sql,
#             optimized_sql=optimized_sql,
#             rewrites=rewrites,
#             explanation=explanation,
#             attempts=attempts
#         )
