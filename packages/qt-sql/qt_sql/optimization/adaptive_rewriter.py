"""
Adaptive SQL Rewriter

LLM-powered SQL optimizer using DAG-based node rewrites with history tracking.
This is the main entry point for SQL optimization.

Architecture:
- Uses SQLDag for structured query representation
- DSPy signatures for structured LLM interaction
- Full history tracking with compact summaries
- Rich error categorization with actionable hints
- Best-so-far tracking with speedup measurement

Usage:
    from qt_sql.optimization.adaptive_rewriter import AdaptiveRewriter, optimize_with_history

    # Simple usage
    result = optimize_with_history(sql, db_path, provider="deepseek")
    print(f"Speedup: {result.speedup:.2f}x in {result.total_attempts} attempts")

    # Advanced usage
    rewriter = AdaptiveRewriter(
        db_path="tpcds.duckdb",
        provider="deepseek",
        max_iterations=5,
        target_speedup=2.0
    )
    result = rewriter.optimize(sql)
"""

from typing import Optional, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum
import json
import time
import logging
from pathlib import Path

try:
    import dspy
    DSPY_AVAILABLE = True
except ImportError:
    DSPY_AVAILABLE = False
    dspy = None

if TYPE_CHECKING:
    from .sql_dag import SQLDag
    from .plan_analyzer import OptimizationContext

logger = logging.getLogger(__name__)


# ============================================================
# Error Categories
# ============================================================

class ErrorCategory(Enum):
    """Categories of optimization failures with hints."""
    SYNTAX_ERROR = "syntax_error"
    EXECUTION_ERROR = "execution_error"
    ROW_COUNT_MISMATCH = "row_count_mismatch"
    VALUE_MISMATCH = "value_mismatch"
    REGRESSION = "regression"
    NO_CHANGE = "no_change"


@dataclass
class ValidationError:
    """Detailed validation error with category and hints."""
    category: ErrorCategory
    message: str
    hint: str

    def to_compact(self) -> str:
        """Format for LLM prompt."""
        return f"{self.category.value}: {self.message}\nHint: {self.hint}"


def categorize_error(
    original_sql: str,
    optimized_sql: str,
    error_msg: str,
    original_rows: int = 0,
    optimized_rows: int = 0,
    speedup: float = 0.0
) -> ValidationError:
    """Categorize validation error with actionable hints."""

    error_lower = error_msg.lower()

    # Syntax errors
    if "syntax" in error_lower or "parse" in error_lower:
        return ValidationError(
            category=ErrorCategory.SYNTAX_ERROR,
            message=error_msg,
            hint="Check SQL syntax - missing parentheses, keywords, or clause order"
        )

    # Execution errors (table not found, etc.)
    if "not exist" in error_lower or "catalog" in error_lower or "unknown" in error_lower:
        return ValidationError(
            category=ErrorCategory.EXECUTION_ERROR,
            message=error_msg,
            hint="Check table/column names - CTE must be defined before use"
        )

    # Row count mismatch
    if "row" in error_lower and ("mismatch" in error_lower or "expected" in error_lower or "missing" in error_lower):
        if optimized_rows > original_rows:
            hint = "Query returns extra rows - check for: UNION ALL vs UNION, missing WHERE conditions, incorrect JOINs"
        else:
            hint = "Query missing rows - check for: overly restrictive filters, incorrect JOIN type, missing UNION branches"
        return ValidationError(
            category=ErrorCategory.ROW_COUNT_MISMATCH,
            message=f"Expected {original_rows} rows, got {optimized_rows}",
            hint=hint
        )

    # Value mismatch (same row count but different values)
    if "differ" in error_lower or "value" in error_lower:
        return ValidationError(
            category=ErrorCategory.VALUE_MISMATCH,
            message=error_msg,
            hint="Values differ - check aggregation order, floating point precision, or column selection"
        )

    # Regression (slower)
    if "regression" in error_lower or speedup < 1.0:
        return ValidationError(
            category=ErrorCategory.REGRESSION,
            message=f"Query is {speedup:.2f}x slower",
            hint="Optimization made query slower - try a different approach"
        )

    # No change
    if original_sql.strip() == optimized_sql.strip():
        return ValidationError(
            category=ErrorCategory.NO_CHANGE,
            message="Query unchanged",
            hint="No optimization applied - try a different technique"
        )

    # Default
    return ValidationError(
        category=ErrorCategory.EXECUTION_ERROR,
        message=error_msg,
        hint="Check query structure and semantics"
    )


# ============================================================
# Attempt History
# ============================================================

@dataclass
class OptimizationAttempt:
    """Record of a single optimization attempt."""
    attempt_num: int
    strategy: str  # What the LLM said it would try
    changes: list[str]  # List of changes made
    nodes_rewritten: list[str]  # DAG node IDs that were changed
    result: str  # "success", "invalid", "regression"
    speedup: float  # 0.0 if invalid
    error: Optional[ValidationError] = None

    def to_compact(self) -> str:
        """Format for LLM prompt - compact summary."""
        lines = [f"### Attempt {self.attempt_num}: {self.result.upper()}"]

        if self.speedup > 0:
            lines.append(f"Speedup: {self.speedup:.2f}x")

        lines.append(f"Strategy: {self.strategy}")

        if self.nodes_rewritten:
            lines.append(f"Nodes changed: {', '.join(self.nodes_rewritten)}")

        if self.changes:
            lines.append(f"Changes: {', '.join(self.changes)}")

        if self.error:
            lines.append(f"Error: {self.error.message}")
            lines.append(f"Hint: {self.error.hint}")

        return "\n".join(lines)


@dataclass
class AttemptHistory:
    """Full history of optimization attempts."""
    attempts: list[OptimizationAttempt] = field(default_factory=list)
    best_sql: Optional[str] = None
    best_speedup: float = 1.0
    best_attempt: int = 0
    best_rewrites: Optional[dict] = None

    def add_attempt(self, attempt: OptimizationAttempt, sql: str, rewrites: dict = None) -> None:
        """Add attempt and update best if improved."""
        self.attempts.append(attempt)

        if attempt.result == "success" and attempt.speedup > self.best_speedup:
            self.best_sql = sql
            self.best_speedup = attempt.speedup
            self.best_attempt = attempt.attempt_num
            self.best_rewrites = rewrites

    def to_prompt(self) -> str:
        """Format full history for LLM prompt."""
        if not self.attempts:
            return ""

        lines = ["## Previous Attempts (Learn From These!)"]
        for attempt in self.attempts:
            lines.append("")
            lines.append(attempt.to_compact())

        if self.best_speedup > 1.0:
            lines.append("")
            lines.append(f"## Current Best: {self.best_speedup:.2f}x (Attempt {self.best_attempt})")
            lines.append("You must BEAT this to improve the result.")

        return "\n".join(lines)

    @property
    def consecutive_failures(self) -> int:
        """Count consecutive failures from end."""
        count = 0
        for attempt in reversed(self.attempts):
            if attempt.result != "success":
                count += 1
            else:
                break
        return count


# ============================================================
# Result Types
# ============================================================

@dataclass
class RewriteResult:
    """Result of adaptive rewriting."""
    original_sql: str
    optimized_sql: str
    valid: bool
    speedup: float
    total_attempts: int
    successful_attempts: int
    history: AttemptHistory
    elapsed_time: float
    rewrites: Optional[dict] = None  # Final DAG rewrites
    error: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "valid": self.valid,
            "speedup": self.speedup,
            "total_attempts": self.total_attempts,
            "successful_attempts": self.successful_attempts,
            "elapsed_time": self.elapsed_time,
            "error": self.error,
            "rewrites": self.rewrites,
            "attempts": [
                {
                    "num": a.attempt_num,
                    "strategy": a.strategy,
                    "changes": a.changes,
                    "nodes_rewritten": a.nodes_rewritten,
                    "result": a.result,
                    "speedup": a.speedup,
                    "error": a.error.message if a.error else None
                }
                for a in self.history.attempts
            ]
        }


# ============================================================
# DSPy Signatures
# ============================================================

if DSPY_AVAILABLE:
    class AdaptiveRewriteSignature(dspy.Signature):
        """Optimize SQL by rewriting specific DAG nodes.

        You receive the query structure as a DAG and must output targeted rewrites.
        Learn from previous attempts - do NOT repeat failed strategies.
        """

        # Inputs
        query_dag: str = dspy.InputField(
            desc="DAG structure showing nodes (CTEs, subqueries, main_query) with their SQL"
        )
        execution_plan: str = dspy.InputField(
            desc="Execution plan showing operator costs, row counts, and scans"
        )
        knowledge_patterns: str = dspy.InputField(
            desc="Optimization patterns that have worked on similar queries",
            default=""
        )
        attempt_history: str = dspy.InputField(
            desc="Previous attempts with what worked and what failed - AVOID repeating failures",
            default=""
        )

        # Outputs
        reasoning: str = dspy.OutputField(
            desc="Brief analysis: why previous attempts failed (if any) and what different strategy to try"
        )
        strategy: str = dspy.OutputField(
            desc="One sentence describing your optimization approach"
        )
        changes: str = dspy.OutputField(
            desc="Comma-separated list of changes being made (e.g., 'pushed filter to CTE, removed redundant join')"
        )
        rewrites: str = dspy.OutputField(
            desc='JSON object: {"node_id": "SELECT ...", ...} for nodes to rewrite. Empty {} to keep original.'
        )
else:
    AdaptiveRewriteSignature = None


# ============================================================
# Main Optimizer
# ============================================================

# Use dspy.Module as base if available, otherwise object
_BaseClass = dspy.Module if DSPY_AVAILABLE else object


class AdaptiveRewriter(_BaseClass):
    """Adaptive SQL rewriter with history tracking.

    Uses DAG-based node rewrites with full history of attempts.
    Each iteration sees what worked and what failed.

    Requires dspy-ai package. Install with: pip install dspy-ai
    """

    def __init__(
        self,
        db_path: str,
        provider: str = "deepseek",
        max_iterations: int = 5,
        min_speedup: float = 1.0,
        target_speedup: float = 2.0,
        max_consecutive_failures: int = 3,
        log_dir: Optional[str] = None,
        benchmark_runs: int = 5,
    ):
        """
        Args:
            db_path: Path to database for validation
            provider: LLM provider name
            max_iterations: Maximum optimization attempts
            min_speedup: Minimum speedup to accept (reject regressions)
            target_speedup: Stop early if this speedup achieved
            max_consecutive_failures: Stop after N failures in a row
            log_dir: Optional directory to write prompts/outputs per attempt
            benchmark_runs: Number of timing runs (5-run: drop min/max, avg middle 3)
        """
        if not DSPY_AVAILABLE:
            raise ImportError(
                "AdaptiveRewriter requires dspy-ai. Install with: pip install dspy-ai"
            )

        super().__init__()

        self.db_path = db_path
        self.provider = provider
        self.max_iterations = max_iterations
        self.min_speedup = min_speedup
        self.target_speedup = target_speedup
        self.max_consecutive_failures = max_consecutive_failures
        self.benchmark_runs = benchmark_runs
        self.log_dir = log_dir

    def _write_log_file(self, filename: str, content: str) -> None:
        """Write content to a log file if log_dir is set."""
        if not self.log_dir:
            return
        try:
            path = Path(self.log_dir)
            path.mkdir(parents=True, exist_ok=True)
            (path / filename).write_text(content)
        except Exception as e:
            logger.debug(f"Failed to write log file {filename}: {e}")

        # DSPy predictor
        self.predictor = dspy.ChainOfThought(AdaptiveRewriteSignature)

        # Configure LLM
        self._configure_lm()

    def _configure_lm(self):
        """Configure DSPy language model."""
        from .dspy_optimizer import configure_lm
        configure_lm(provider=self.provider)

    def _parse_rewrites(self, rewrites_str: str) -> dict[str, str]:
        """Parse rewrites JSON from LLM output."""
        text = rewrites_str.strip()

        # Remove markdown code blocks
        if "```json" in text:
            start = text.find("```json") + 7
            end = text.find("```", start)
            if end > start:
                text = text[start:end].strip()
        elif "```" in text:
            start = text.find("```") + 3
            end = text.find("```", start)
            if end > start:
                text = text[start:end].strip()

        # Find JSON object
        start = text.find("{")
        end = text.rfind("}") + 1
        if start != -1 and end > start:
            text = text[start:end]

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse rewrites JSON: {text[:100]}")
            return {}

    def _validate_and_time(
        self,
        original_sql: str,
        optimized_sql: str
    ) -> tuple[bool, float, int, int, Optional[ValidationError]]:
        """Validate optimized SQL and measure speedup.

        Returns:
            (is_valid, speedup, original_rows, optimized_rows, error)
        """
        import duckdb

        try:
            conn = duckdb.connect(self.db_path, read_only=True)

            # Execute original
            try:
                orig_result = conn.execute(original_sql).fetchall()
            except Exception as e:
                conn.close()
                return False, 0.0, 0, 0, ValidationError(
                    category=ErrorCategory.EXECUTION_ERROR,
                    message=f"Original query error: {e}",
                    hint="Original query failed - check database"
                )

            # Execute optimized
            try:
                opt_result = conn.execute(optimized_sql).fetchall()
            except Exception as e:
                conn.close()
                error = categorize_error(
                    original_sql, optimized_sql, str(e),
                    len(orig_result), 0, 0.0
                )
                return False, 0.0, len(orig_result), 0, error

            orig_rows = len(orig_result)
            opt_rows = len(opt_result)

            # Compare results
            orig_set = set(tuple(r) for r in orig_result)
            opt_set = set(tuple(r) for r in opt_result)

            if orig_set != opt_set:
                conn.close()
                error = categorize_error(
                    original_sql, optimized_sql,
                    f"Row mismatch: expected {orig_rows}, got {opt_rows}",
                    orig_rows, opt_rows, 0.0
                )
                return False, 0.0, orig_rows, opt_rows, error

            # Benchmark timing (5 runs, drop min/max, average middle 3)
            orig_times = []
            opt_times = []

            for _ in range(self.benchmark_runs):
                start = time.perf_counter()
                conn.execute(original_sql).fetchall()
                orig_times.append((time.perf_counter() - start) * 1000)

                start = time.perf_counter()
                conn.execute(optimized_sql).fetchall()
                opt_times.append((time.perf_counter() - start) * 1000)

            conn.close()

            # Drop min/max, average middle 3
            if len(orig_times) >= 3:
                orig_times.sort()
                opt_times.sort()
                orig_avg = sum(orig_times[1:-1]) / (len(orig_times) - 2)
                opt_avg = sum(opt_times[1:-1]) / (len(opt_times) - 2)
            else:
                orig_avg = sum(orig_times) / len(orig_times)
                opt_avg = sum(opt_times) / len(opt_times)

            speedup = orig_avg / opt_avg if opt_avg > 0 else 1.0

            # Check for regression
            if speedup < self.min_speedup:
                error = ValidationError(
                    category=ErrorCategory.REGRESSION,
                    message=f"Speedup {speedup:.2f}x < {self.min_speedup}x minimum",
                    hint="Query is slower - try a different optimization"
                )
                return False, speedup, orig_rows, opt_rows, error

            return True, speedup, orig_rows, opt_rows, None

        except Exception as e:
            return False, 0.0, 0, 0, ValidationError(
                category=ErrorCategory.EXECUTION_ERROR,
                message=str(e),
                hint="Validation error - check database connection"
            )

    def _get_kb_patterns(self, sql: str, dag: "SQLDag" = None) -> str:
        """Get relevant KB patterns for the query."""
        try:
            from .dspy_optimizer import detect_knowledge_patterns
            return detect_knowledge_patterns(sql, dag=dag)
        except Exception:
            return ""

    def _format_plan_summary(self, ctx: "OptimizationContext") -> str:
        """Format a compact plan summary from parsed EXPLAIN analysis."""
        lines = []

        # Build a quick table->scan map for labeling operators
        scan_counts: dict[str, int] = {}
        scan_by_table: dict[str, list] = {}
        for scan in ctx.table_scans:
            scan_counts[scan.table] = scan_counts.get(scan.table, 0) + 1
            scan_by_table.setdefault(scan.table, []).append(scan)

        # Sort scans by rows_scanned (desc), then rows_out (desc)
        for table in scan_by_table:
            scan_by_table[table].sort(
                key=lambda s: (s.rows_scanned, s.rows_out),
                reverse=True,
            )

        top_ops = ctx.get_top_operators(5)
        if top_ops:
            lines.append("Operators by cost:")
            # Try to annotate SEQ_SCAN with the largest scan table
            # when we have repeated scans (common in Q9-style CTEs).
            for op in top_ops:
                label = op["operator"]
                if "SCAN" in label.upper() and scan_by_table:
                    # Pick the largest scan table for context
                    top_table = max(
                        scan_by_table.items(),
                        key=lambda kv: (kv[1][0].rows_scanned, kv[1][0].rows_out),
                    )[0]
                    label = f"{label}({top_table})"
                lines.append(
                    f"- {label}: {op['cost_pct']}% cost, {op['rows']:,} rows"
                )
            lines.append("")

        if ctx.table_scans:
            lines.append("Scans:")
            # Deduplicate by table and show count + worst-case scan
            for table, scans in sorted(
                scan_by_table.items(),
                key=lambda kv: (kv[1][0].rows_scanned, kv[1][0].rows_out),
                reverse=True,
            )[:8]:
                s = scans[0]
                count = scan_counts[table]
                if s.has_filter:
                    lines.append(
                        f"- {table} x{count}: {s.rows_scanned:,} → {s.rows_out:,} rows (filtered)"
                    )
                else:
                    lines.append(f"- {table} x{count}: {s.rows_scanned:,} rows (no filter)")
            lines.append("")

        if ctx.cardinality_misestimates:
            lines.append("Misestimates:")
            for mis in ctx.cardinality_misestimates:
                lines.append(
                    f"- {mis['operator']}: est {mis['estimated']:,} vs actual {mis['actual']:,} ({mis['ratio']}x)"
                )
            lines.append("")

        if ctx.joins:
            lines.append("Joins:")
            for j in ctx.joins[:5]:
                late = " (late)" if j.is_late else ""
                lines.append(
                    f"- {j.join_type}: {j.left_table} x {j.right_table} -> {j.output_rows:,} rows{late}"
                )
            lines.append("")

        return "\n".join(lines).strip()

    def _get_plan_analysis(self, sql: str) -> tuple[str, Optional[dict]]:
        """Get parsed execution plan summary and raw plan JSON (if available)."""
        try:
            from qt_sql.execution.database_utils import run_explain_analyze
            from qt_sql.optimization.plan_analyzer import analyze_plan_for_optimization

            result = run_explain_analyze(self.db_path, sql) or {}
            plan_json = result.get("plan_json")
            if plan_json:
                ctx = analyze_plan_for_optimization(plan_json, sql)
                summary = self._format_plan_summary(ctx)
                return summary if summary else "(execution plan not available)", plan_json
        except Exception as e:
            logger.debug(f"Could not get execution plan: {e}")

        return "(execution plan not available)", None

    def _init_example_selector(self, sql: str):
        """Initialize the example selector for this query."""
        from .dag_v3 import DagV3ExampleSelector
        self._example_selector = DagV3ExampleSelector(sql, examples_per_prompt=3)

    def _build_prompt_with_example(
        self,
        base_prompt: str,
        history_section: str,
        execution_plan: str = "",
    ) -> tuple[str, str]:
        """Build full prompt with current gold example.

        Returns:
            (prompt, example_id) - the prompt and which example was used
        """
        if not hasattr(self, '_example_selector') or not self._example_selector.current_examples:
            return base_prompt, "none"

        examples = self._example_selector.current_examples
        prompt = self._example_selector.get_prompt(base_prompt, execution_plan, history_section)
        example_ids = ",".join(ex.id for ex in examples)
        return prompt, example_ids

    def _rotate_example(self):
        """Rotate to a different example after failure."""
        if hasattr(self, '_example_selector'):
            self._example_selector.rotate()

    def optimize(
        self,
        original_sql: str,
        execution_plan: Optional[str] = None
    ) -> RewriteResult:
        """Run adaptive optimization using DAG v2 format.

        Args:
            original_sql: SQL query to optimize
            execution_plan: Pre-computed execution plan (optional)

        Returns:
            RewriteResult with best optimization found
        """
        from .dag_v2 import DagV2Pipeline

        start_time = time.time()

        # Always fetch parsed plan JSON for cost attribution; use summary if not provided
        plan_summary, plan_json = self._get_plan_analysis(original_sql)
        if execution_plan is None:
            execution_plan = plan_summary

        # Build DAG v2 pipeline (includes contracts, usage, costs)
        pipeline = DagV2Pipeline(original_sql, plan_json=plan_json)

        # Get base prompt from DagV2
        base_prompt = pipeline.get_prompt()

        # Initialize example selector (uses dag_v3 with KB pattern matching)
        self._init_example_selector(original_sql)
        if hasattr(self, '_example_selector') and self._example_selector.examples:
            logger.info(f"Found {len(self._example_selector.examples)} matching gold examples: {[e.id for e in self._example_selector.examples]}")

        # Initialize history
        history = AttemptHistory()
        history.best_sql = original_sql
        history.best_speedup = 1.0

        successful_attempts = 0

        for iteration in range(1, self.max_iterations + 1):
            current_examples = self._example_selector.current_examples if hasattr(self, '_example_selector') else []
            example_label = ", ".join(ex.id for ex in current_examples) if current_examples else "none"
            logger.info(f"Iteration {iteration}/{self.max_iterations}, using examples: {example_label}")

            # Build full prompt with current example (rotates on failure)
            history_section = history.to_prompt()
            full_prompt, example_used = self._build_prompt_with_example(
                base_prompt, history_section, execution_plan
            )
            self._write_log_file(
                f"attempt_{iteration:02d}_prompt.txt",
                full_prompt
            )

            # Call LLM directly (DagV2 format, not DSPy signature)
            try:
                lm = dspy.settings.lm
                response = lm(full_prompt)

                # Extract response text
                if isinstance(response, list) and len(response) > 0:
                    response_text = str(response[0])
                elif hasattr(response, 'text'):
                    response_text = response.text
                else:
                    response_text = str(response)

                self._write_log_file(
                    f"attempt_{iteration:02d}_response.txt",
                    response_text
                )

            except Exception as e:
                logger.error(f"LLM call failed: {e}")
                attempt = OptimizationAttempt(
                    attempt_num=iteration,
                    strategy="LLM call failed",
                    changes=[],
                    nodes_rewritten=[],
                    result="invalid",
                    speedup=0.0,
                    error=ValidationError(
                        category=ErrorCategory.EXECUTION_ERROR,
                        message=str(e),
                        hint="LLM API error"
                    )
                )
                history.add_attempt(attempt, original_sql)
                continue

            # Apply response using DagV2 assembler
            try:
                optimized_sql = pipeline.apply_response(response_text)

                # Parse rewrite_sets from response for logging
                import re
                json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
                if json_match:
                    rewrite_data = json.loads(json_match.group(1))
                    rewrite_sets = rewrite_data.get("rewrite_sets", [])
                    if rewrite_sets:
                        first_set = rewrite_sets[0]
                        strategy = first_set.get("transform", "unknown")
                        nodes_rewritten = list(first_set.get("nodes", {}).keys())
                        changes = first_set.get("invariants_kept", [])
                    else:
                        strategy = "no rewrite sets"
                        nodes_rewritten = []
                        changes = []
                else:
                    strategy = "unknown"
                    nodes_rewritten = []
                    changes = []

            except Exception as e:
                attempt = OptimizationAttempt(
                    attempt_num=iteration,
                    strategy="parse failed",
                    changes=[],
                    nodes_rewritten=[],
                    result="invalid",
                    speedup=0.0,
                    error=ValidationError(
                        category=ErrorCategory.SYNTAX_ERROR,
                        message=f"Failed to apply rewrites: {e}",
                        hint="Check LLM output format"
                    )
                )
                history.add_attempt(attempt, original_sql)
                continue

            # Check for no change
            if optimized_sql.strip() == original_sql.strip():
                attempt = OptimizationAttempt(
                    attempt_num=iteration,
                    strategy=strategy,
                    changes=changes,
                    nodes_rewritten=nodes_rewritten,
                    result="no_change",
                    speedup=1.0,
                    error=ValidationError(
                        category=ErrorCategory.NO_CHANGE,
                        message="Rewrites produced identical SQL",
                        hint="Try a different optimization technique"
                    )
                )
                history.add_attempt(attempt, optimized_sql)
                continue

            # Validate and time
            is_valid, speedup, orig_rows, opt_rows, error = self._validate_and_time(
                original_sql, optimized_sql
            )

            if is_valid:
                successful_attempts += 1
                result_str = "success"
                logger.info(f"  Valid optimization: {speedup:.2f}x speedup")
            elif error and error.category == ErrorCategory.REGRESSION:
                result_str = "regression"
                logger.info(f"  Regression: {speedup:.2f}x")
            else:
                result_str = "invalid"
                logger.info(f"  Invalid: {error.message if error else 'unknown'}")

            # Record attempt
            attempt = OptimizationAttempt(
                attempt_num=iteration,
                strategy=strategy,
                changes=changes,
                nodes_rewritten=nodes_rewritten,
                result=result_str,
                speedup=speedup if is_valid else 0.0,
                error=error
            )
            history.add_attempt(
                attempt,
                optimized_sql if is_valid else original_sql,
                {"nodes": nodes_rewritten} if is_valid else None
            )

            # Check stopping conditions
            if is_valid and speedup >= self.target_speedup:
                logger.info(f"Target speedup {self.target_speedup}x achieved!")
                break

            if history.consecutive_failures >= self.max_consecutive_failures:
                logger.info(f"Stopping: {self.max_consecutive_failures} consecutive failures")
                break

            # Rotate to different example on failure or sub-target success
            if not is_valid or (is_valid and speedup < self.target_speedup):
                self._rotate_example()

        # Extra run: if best < target, replan from best SQL and try once more
        if history.best_speedup < self.target_speedup and history.best_sql and history.best_sql.strip() != original_sql.strip():
            extra_iteration = self.max_iterations + 1
            logger.info(
                "Extra run: best %.2fx < target %.2fx. Replanning from best SQL (attempt %d).",
                history.best_speedup,
                self.target_speedup,
                extra_iteration,
            )

            # Build a fresh DAG pipeline from best SQL
            from .dag_v2 import DagV2Pipeline
            best_plan, best_plan_json = self._get_plan_analysis(history.best_sql)
            best_pipeline = DagV2Pipeline(history.best_sql, plan_json=best_plan_json)
            best_base_prompt = best_pipeline.get_prompt()

            # Re-init example selector based on best SQL
            self._init_example_selector(history.best_sql)
            current_examples = self._example_selector.current_examples if hasattr(self, '_example_selector') else []
            example_label = ", ".join(ex.id for ex in current_examples) if current_examples else "none"
            logger.info(f"Extra run using examples: {example_label}")

            # Build prompt with history (summaries of what worked/didn't)
            history_section = history.to_prompt()
            full_prompt, _example_used = self._build_prompt_with_example(
                best_base_prompt, history_section, best_plan
            )
            self._write_log_file(
                f"attempt_{extra_iteration:02d}_prompt.txt",
                full_prompt
            )

            # Call LLM
            try:
                lm = dspy.settings.lm
                response = lm(full_prompt)

                # Extract response text
                if isinstance(response, list) and len(response) > 0:
                    response_text = str(response[0])
                elif hasattr(response, 'text'):
                    response_text = response.text
                else:
                    response_text = str(response)

                self._write_log_file(
                    f"attempt_{extra_iteration:02d}_response.txt",
                    response_text
                )

            except Exception as e:
                attempt = OptimizationAttempt(
                    attempt_num=extra_iteration,
                    strategy="LLM call failed",
                    changes=[],
                    nodes_rewritten=[],
                    result="invalid",
                    speedup=0.0,
                    error=ValidationError(
                        category=ErrorCategory.EXECUTION_ERROR,
                        message=str(e),
                        hint="LLM API error"
                    )
                )
                history.add_attempt(attempt, history.best_sql)
                # Skip further processing
                elapsed = time.time() - start_time
                return RewriteResult(
                    original_sql=original_sql,
                    optimized_sql=history.best_sql or original_sql,
                    valid=history.best_speedup > 1.0,
                    speedup=history.best_speedup,
                    total_attempts=len(history.attempts),
                    successful_attempts=successful_attempts,
                    history=history,
                    elapsed_time=elapsed,
                    rewrites=history.best_rewrites,
                    error=None if history.best_speedup > 1.0 else "No valid optimization found"
                )

            # Apply response using DagV2 assembler
            try:
                optimized_sql = best_pipeline.apply_response(response_text)

                # Parse rewrite_sets from response for logging
                import re
                json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
                if json_match:
                    rewrite_data = json.loads(json_match.group(1))
                    rewrite_sets = rewrite_data.get("rewrite_sets", [])
                    if rewrite_sets:
                        first_set = rewrite_sets[0]
                        strategy = first_set.get("transform", "unknown")
                        nodes_rewritten = list(first_set.get("nodes", {}).keys())
                        changes = first_set.get("invariants_kept", [])
                    else:
                        strategy = "no rewrite sets"
                        nodes_rewritten = []
                        changes = []
                else:
                    strategy = "unknown"
                    nodes_rewritten = []
                    changes = []

            except Exception as e:
                attempt = OptimizationAttempt(
                    attempt_num=extra_iteration,
                    strategy="parse failed",
                    changes=[],
                    nodes_rewritten=[],
                    result="invalid",
                    speedup=0.0,
                    error=ValidationError(
                        category=ErrorCategory.SYNTAX_ERROR,
                        message=f"Failed to apply rewrites: {e}",
                        hint="Check LLM output format"
                    )
                )
                history.add_attempt(attempt, history.best_sql)
                elapsed = time.time() - start_time
                return RewriteResult(
                    original_sql=original_sql,
                    optimized_sql=history.best_sql or original_sql,
                    valid=history.best_speedup > 1.0,
                    speedup=history.best_speedup,
                    total_attempts=len(history.attempts),
                    successful_attempts=successful_attempts,
                    history=history,
                    elapsed_time=elapsed,
                    rewrites=history.best_rewrites,
                    error=None if history.best_speedup > 1.0 else "No valid optimization found"
                )

            # Check for no change
            if optimized_sql.strip() == history.best_sql.strip():
                attempt = OptimizationAttempt(
                    attempt_num=extra_iteration,
                    strategy=strategy,
                    changes=changes,
                    nodes_rewritten=nodes_rewritten,
                    result="no_change",
                    speedup=1.0,
                    error=ValidationError(
                        category=ErrorCategory.NO_CHANGE,
                        message="Rewrites produced identical SQL",
                        hint="Try a different optimization technique"
                    )
                )
                history.add_attempt(attempt, optimized_sql)
            else:
                # Validate and time against ORIGINAL SQL to preserve correctness/speedup baseline
                is_valid, speedup, orig_rows, opt_rows, error = self._validate_and_time(
                    original_sql, optimized_sql
                )

                if is_valid:
                    successful_attempts += 1
                    result_str = "success"
                    logger.info(f"  Extra run valid optimization: {speedup:.2f}x speedup")
                elif error and error.category == ErrorCategory.REGRESSION:
                    result_str = "regression"
                    logger.info(f"  Extra run regression: {speedup:.2f}x")
                else:
                    result_str = "invalid"
                    logger.info(f"  Extra run invalid: {error.message if error else 'unknown'}")

                attempt = OptimizationAttempt(
                    attempt_num=extra_iteration,
                    strategy=strategy,
                    changes=changes,
                    nodes_rewritten=nodes_rewritten,
                    result=result_str,
                    speedup=speedup if is_valid else 0.0,
                    error=error
                )
                history.add_attempt(
                    attempt,
                    optimized_sql if is_valid else history.best_sql,
                    {"nodes": nodes_rewritten} if is_valid else None
                )

        elapsed = time.time() - start_time

        return RewriteResult(
            original_sql=original_sql,
            optimized_sql=history.best_sql or original_sql,
            valid=history.best_speedup > 1.0,
            speedup=history.best_speedup,
            total_attempts=len(history.attempts),
            successful_attempts=successful_attempts,
            history=history,
            elapsed_time=elapsed,
            rewrites=history.best_rewrites,
            error=None if history.best_speedup > 1.0 else "No valid optimization found"
        )


# ============================================================
# Convenience Functions
# ============================================================

def optimize_with_history(
    sql: str,
    db_path: str,
    provider: str = "deepseek",
    max_iterations: int = 5,
    target_speedup: float = 2.0
) -> RewriteResult:
    """Convenience function for adaptive SQL rewriting.

    Args:
        sql: SQL query to optimize
        db_path: Path to database for validation
        provider: LLM provider name
        max_iterations: Maximum attempts
        target_speedup: Stop early if achieved

    Returns:
        RewriteResult with optimized SQL and history
    """
    rewriter = AdaptiveRewriter(
        db_path=db_path,
        provider=provider,
        max_iterations=max_iterations,
        target_speedup=target_speedup
    )
    return rewriter.optimize(sql)


# Alias for backwards compatibility
optimize_query = optimize_with_history
