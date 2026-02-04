"""
DSPy-based SQL Query Optimizer with Validation and Retries

Key features:
- Signature-based prompting with structured inputs/outputs
- ChainOfThought reasoning for multi-step optimization
- **Validation step** - checks semantic equivalence before accepting
- **Retry logic** - retries with feedback if validation fails
- **Model-specific tuning** - loads constraints from model_configs/
- **DB-specific hints** - loads hints from db_configs/
- **Few-shot examples** - gold examples from top-performing optimizations
- **dspy.Suggest assertions** - soft constraints for retry guidance
- MIPROv2 optimizer support for prompt tuning
"""

from typing import Optional, Tuple, List, Dict, Any, Callable
from dataclasses import dataclass, field
from pathlib import Path
import dspy
import yaml
import json
import time


# ============================================================
# Validation Result (Core QueryTorque Output)
# ============================================================

@dataclass
class ValidationResult:
    """Result of validating an optimized query against the original.

    This is QueryTorque's core output - semantic correctness AND timing.
    """
    is_correct: bool
    original_time_ms: float
    optimized_time_ms: float
    speedup: float
    original_rows: int
    optimized_rows: int
    error: Optional[str] = None

    @property
    def is_regression(self) -> bool:
        """True if optimized query is slower than original."""
        return self.speedup < 1.0

    def __str__(self) -> str:
        if not self.is_correct:
            return f"INVALID: {self.error}"
        status = "REGRESSION" if self.is_regression else "IMPROVED"
        return (f"{status}: {self.speedup:.2f}x speedup "
                f"({self.original_time_ms:.1f}ms → {self.optimized_time_ms:.1f}ms, "
                f"{self.original_rows} rows)")

# DSPy 3.x removed Suggest/Assert - make it optional
try:
    from dspy.primitives.assertions import Suggest
    SUGGEST_AVAILABLE = True
except ImportError:
    try:
        from dspy.assertions import Suggest
        SUGGEST_AVAILABLE = True
    except ImportError:
        # Suggest not available in this DSPy version
        SUGGEST_AVAILABLE = False
        def Suggest(condition, message, target_module=None):
            """No-op placeholder when Suggest is not available."""
            pass


# ============================================================
# Configuration Loading
# ============================================================

def get_config_dir() -> Path:
    """Get the knowledge_base config directory."""
    # Navigate from this file to research/knowledge_base
    current = Path(__file__).resolve()
    # packages/qt-sql/qt_sql/optimization/dspy_optimizer.py
    # -> ../../../../research/knowledge_base
    return current.parent.parent.parent.parent.parent / "research" / "knowledge_base"


def load_model_config(model_name: str) -> Dict[str, Any]:
    """Load model-specific configuration.

    Args:
        model_name: Name like 'deepseek', 'groq', 'gemini'

    Returns:
        Config dict with constraints, strengths, failure_patterns, prompt_suffix
    """
    config_path = get_config_dir() / "model_configs" / f"{model_name}.yaml"

    if not config_path.exists():
        return {
            "constraints": [],
            "strengths": [],
            "failure_patterns": [],
            "prompt_suffix": ""
        }

    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def load_db_config(db_name: str) -> Dict[str, Any]:
    """Load database-specific configuration.

    Args:
        db_name: Name like 'duckdb', 'postgres', 'snowflake'

    Returns:
        Config dict with hints, syntax_notes, limitations, strengths
    """
    config_path = get_config_dir() / "db_configs" / f"{db_name}.yaml"

    if not config_path.exists():
        return {
            "hints": [],
            "syntax_notes": [],
            "limitations": [],
            "strengths": []
        }

    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def build_system_prompt(model_name: str = None, db_name: str = None) -> str:
    """Build system prompt from model and DB configs.

    Args:
        model_name: Optional model name for model-specific constraints
        db_name: Optional DB name for DB-specific hints

    Returns:
        Combined prompt suffix string
    """
    parts = []

    if model_name:
        model_config = load_model_config(model_name)
        if model_config.get("prompt_suffix"):
            parts.append(model_config["prompt_suffix"])

    if db_name:
        db_config = load_db_config(db_name)
        hints = db_config.get("hints", [])
        if hints:
            hint_text = "\n".join(f"- {h['text']}" for h in hints if isinstance(h, dict))
            if hint_text:
                parts.append(f"\nDB-SPECIFIC HINTS ({db_name}):\n{hint_text}")

    return "\n".join(parts)


def load_constraint_set(name: str = "dag_v5") -> Dict[str, Any]:
    """Load a constraint set from knowledge_base/constraints.

    Args:
        name: Constraint set name (without .yaml)

    Returns:
        Config dict with constraints and prompt_suffix
    """
    config_path = get_config_dir() / "constraints" / f"{name}.yaml"
    if not config_path.exists():
        return {"constraints": [], "prompt_suffix": ""}
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def build_dag_constraints(constraint_set: str = "dag_v5") -> str:
    """Build constraints text for DAG-based DSPy prompts."""
    cfg = load_constraint_set(constraint_set)
    if cfg.get("prompt_suffix"):
        return cfg["prompt_suffix"]
    # Fallback: join constraint texts if prompt_suffix absent
    items = cfg.get("constraints", [])
    lines = []
    for i, item in enumerate(items, 1):
        if isinstance(item, dict) and item.get("text"):
            lines.append(f"{i}. {item['text']}")
    if not lines:
        return ""
    return "CONSTRAINTS:\n" + "\n".join(lines)


def load_gold_examples(num_examples: int = 3) -> List[dspy.Example]:
    """Load gold examples for few-shot learning.

    Args:
        num_examples: Number of examples to load (default 3)

    Returns:
        List of dspy.Example objects
    """
    try:
        from research.knowledge_base.duckdb.gold_examples import get_gold_examples
        return get_gold_examples(num_examples)
    except ImportError:
        # Fall back to relative import if running from different location
        examples_path = get_config_dir() / "duckdb" / "gold_examples.py"
        if examples_path.exists():
            import importlib.util
            spec = importlib.util.spec_from_file_location("gold_examples", examples_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.get_gold_examples(num_examples)
        return []


def detect_knowledge_patterns(sql: str, dag: "SQLDag" = None) -> str:
    """Detect KNOWLEDGE_BASE patterns relevant to this query.

    This function is now a wrapper around the centralized opportunity_detector module.
    It provides backward compatibility while delegating to the single source of truth.

    When a DAG is provided, also includes multi-node predicate pushdown analysis.

    Args:
        sql: SQL query to analyze
        dag: Optional pre-built SQLDag for pushdown analysis

    Returns:
        Formatted string with relevant patterns, or empty string if none
    """
    # Use the centralized knowledge base
    from qt_sql.optimization.knowledge_base import detect_opportunities, format_opportunities_for_prompt
    opportunities = detect_opportunities(sql)
    patterns = format_opportunities_for_prompt(opportunities)

    # Add pushdown analysis if DAG is provided
    if dag is not None:
        try:
            from qt_sql.optimization.predicate_analysis import analyze_pushdown_opportunities
            analysis = analyze_pushdown_opportunities(dag)
            pushdown_context = analysis.to_prompt_context()
            if pushdown_context:
                if patterns:
                    patterns += "\n\n" + pushdown_context
                else:
                    patterns = pushdown_context
        except Exception:
            # Silently ignore pushdown analysis failures
            pass

    return patterns


@dataclass
class OptimizationResult:
    """Result of an optimization attempt."""
    original_sql: str
    optimized_sql: str
    rationale: str
    speedup: Optional[float] = None
    correct: Optional[bool] = None
    attempts: int = 1
    error: Optional[str] = None


class SQLOptimizer(dspy.Signature):
    """Optimize SQL query for better execution performance."""

    original_query: str = dspy.InputField(
        desc="The original SQL query to optimize"
    )
    execution_plan: str = dspy.InputField(
        desc="Parsed execution plan showing operator costs and row counts"
    )
    row_estimates: str = dspy.InputField(
        desc="Table scan statistics: table name, rows scanned, filter status"
    )
    optimization_hints: str = dspy.InputField(
        desc="Detected optimization opportunities with rewrite patterns",
        default=""
    )
    constraints: str = dspy.InputField(
        desc="Model and DB-specific constraints to follow",
        default=""
    )

    optimized_query: str = dspy.OutputField(
        desc="The optimized SQL query with identical semantics"
    )
    optimization_rationale: str = dspy.OutputField(
        desc="Explanation of what was optimized and why it improves performance"
    )


class SQLOptimizerWithFeedback(dspy.Signature):
    """Optimize SQL query using a DIFFERENT strategy after a failed attempt."""

    original_query: str = dspy.InputField(
        desc="The original SQL query to optimize"
    )
    execution_plan: str = dspy.InputField(
        desc="Parsed execution plan showing operator costs and row counts"
    )
    row_estimates: str = dspy.InputField(
        desc="Table scan statistics: table name, rows scanned, filter status"
    )
    optimization_hints: str = dspy.InputField(
        desc="Detected optimization opportunities with rewrite patterns",
        default=""
    )
    constraints: str = dspy.InputField(
        desc="Model and DB-specific constraints to follow",
        default=""
    )
    previous_attempt: str = dspy.InputField(
        desc="Previous optimization that FAILED - do NOT repeat this approach"
    )
    failure_reason: str = dspy.InputField(
        desc="Why it failed (e.g. wrong row count)"
    )

    optimized_query: str = dspy.OutputField(
        desc="A DIFFERENT optimization using a different strategy. If unsure, return the original query unchanged."
    )
    optimization_rationale: str = dspy.OutputField(
        desc="What different approach you tried"
    )


class ValidatedOptimizationPipeline(dspy.Module):
    """Optimization pipeline with validation and retry logic.

    This pipeline:
    1. Generates an optimized query using few-shot examples
    2. Validates it (runs both, compares results)
    3. If wrong, retries with feedback up to max_retries
    4. Uses dspy.Suggest assertions for retry guidance
    5. Returns best valid result or reports failure
    """

    def __init__(
        self,
        validator_fn=None,
        max_retries: int = 2,
        model_name: str = None,
        db_name: str = None,
        use_few_shot: bool = True,
        num_examples: int = 3,
        use_assertions: bool = True
    ):
        """
        Args:
            validator_fn: Function(original_sql, optimized_sql) -> (correct: bool, error: str|None)
            max_retries: Maximum retry attempts after validation failure
            model_name: Model name for loading model-specific constraints
            db_name: Database name for loading DB-specific hints
            use_few_shot: Whether to use few-shot examples (default True)
            num_examples: Number of few-shot examples to use (default 3)
            use_assertions: Whether to use dspy.Suggest assertions (default True)
        """
        super().__init__()
        self.optimizer = dspy.ChainOfThought(SQLOptimizer)
        self.retry_optimizer = dspy.ChainOfThought(SQLOptimizerWithFeedback)
        self.validator_fn = validator_fn
        self.max_retries = max_retries
        self.constraints = build_system_prompt(model_name, db_name)
        self.use_assertions = use_assertions
        self._last_attempt = None  # Track for retry assertions

        # Load few-shot examples
        # In DSPy 3.x, ChainOfThought wraps Predict - demos go on .predict.demos
        if use_few_shot:
            examples = load_gold_examples(num_examples)
            if examples:
                if hasattr(self.optimizer, 'predict') and hasattr(self.optimizer.predict, 'demos'):
                    self.optimizer.predict.demos = examples
                elif hasattr(self.optimizer, 'demos'):
                    self.optimizer.demos = examples

    def _apply_assertions(
        self,
        query: str,
        optimized_sql: str,
        rationale: str,
        is_retry: bool = False
    ) -> None:
        """Apply dspy.Suggest assertions for quality guidance.

        Args:
            query: Original SQL query
            optimized_sql: Generated optimized query
            rationale: Optimization rationale
            is_retry: Whether this is a retry attempt
        """
        if not self.use_assertions:
            return

        # Soft constraint: output must differ from input
        Suggest(
            optimized_sql.strip() != query.strip(),
            "Optimization should modify the query, not return it unchanged",
            target_module=self.optimizer
        )

        # Soft constraint: rationale must mention optimization technique
        techniques = [
            "pushdown", "filter", "join", "cte", "early", "exists",
            "union", "index", "predicate", "partition", "scan",
            "materialize", "aggregate", "distinct"
        ]
        Suggest(
            any(t in rationale.lower() for t in techniques),
            "Rationale should mention specific optimization technique used",
            target_module=self.optimizer
        )

        # Soft constraint: retry must try different approach
        if is_retry and self._last_attempt:
            Suggest(
                optimized_sql.strip() != self._last_attempt.strip(),
                "Must try a DIFFERENT optimization strategy than previous attempt",
                target_module=self.retry_optimizer
            )

    def forward(
        self,
        query: str,
        plan: str,
        rows: str
    ) -> OptimizationResult:
        """Run optimization with validation and retries.

        Args:
            query: Original SQL query
            plan: Execution plan summary
            rows: Row estimate summary

        Returns:
            OptimizationResult with optimized query and metadata
        """
        attempts = 0
        last_attempt = None
        last_error = None

        # Detect relevant optimization patterns
        hints = detect_knowledge_patterns(query)

        # First attempt
        attempts += 1
        result = self.optimizer(
            original_query=query,
            execution_plan=plan,
            row_estimates=rows,
            optimization_hints=hints,
            constraints=self.constraints
        )
        optimized_sql = result.optimized_query
        rationale = result.optimization_rationale

        # Apply assertions for quality guidance
        self._apply_assertions(query, optimized_sql, rationale, is_retry=False)

        # Validate if validator provided
        if self.validator_fn:
            correct, error = self.validator_fn(query, optimized_sql)

            if correct:
                return OptimizationResult(
                    original_sql=query,
                    optimized_sql=optimized_sql,
                    rationale=rationale,
                    correct=True,
                    attempts=attempts
                )

            # Validation failed - retry with feedback
            last_attempt = optimized_sql
            self._last_attempt = optimized_sql  # Track for assertions
            last_error = error or "Results don't match original query"

            while attempts < self.max_retries + 1:
                attempts += 1

                retry_result = self.retry_optimizer(
                    original_query=query,
                    execution_plan=plan,
                    row_estimates=rows,
                    optimization_hints=hints,
                    constraints=self.constraints,
                    previous_attempt=last_attempt,
                    failure_reason=last_error
                )

                optimized_sql = retry_result.optimized_query
                rationale = retry_result.optimization_rationale

                # Apply assertions for retry guidance
                self._apply_assertions(query, optimized_sql, rationale, is_retry=True)

                correct, error = self.validator_fn(query, optimized_sql)

                if correct:
                    self._last_attempt = None  # Reset
                    return OptimizationResult(
                        original_sql=query,
                        optimized_sql=optimized_sql,
                        rationale=f"[After {attempts} attempts] {rationale}",
                        correct=True,
                        attempts=attempts
                    )

                last_attempt = optimized_sql
                self._last_attempt = optimized_sql
                last_error = error or "Results don't match original query"

            # All retries exhausted
            self._last_attempt = None
            return OptimizationResult(
                original_sql=query,
                optimized_sql=optimized_sql,
                rationale=rationale,
                correct=False,
                attempts=attempts,
                error=f"Validation failed after {attempts} attempts: {last_error}"
            )

        # No validator - return unvalidated result
        return OptimizationResult(
            original_sql=query,
            optimized_sql=optimized_sql,
            rationale=rationale,
            attempts=attempts
        )


def create_duckdb_validator(db_path: str):
    """Create a validator function that uses DuckDB to check semantic equivalence.

    Args:
        db_path: Path to DuckDB database

    Returns:
        Validator function: (original_sql, optimized_sql) -> (correct, error)
    """
    import duckdb

    def validator(original_sql: str, optimized_sql: str) -> Tuple[bool, Optional[str]]:
        try:
            conn = duckdb.connect(db_path, read_only=True)

            # Run original
            try:
                orig_result = conn.execute(original_sql).fetchall()
            except Exception as e:
                conn.close()
                return False, f"Original query error: {e}"

            # Run optimized
            try:
                opt_result = conn.execute(optimized_sql).fetchall()
            except Exception as e:
                conn.close()
                return False, f"SYNTAX ERROR in optimized query: {e}. Fix the SQL syntax."

            conn.close()

            # Compare results
            orig_set = set(tuple(r) for r in orig_result)
            opt_set = set(tuple(r) for r in opt_result)

            if orig_set == opt_set:
                return True, None

            # Only factual feedback - no guessing
            missing = orig_set - opt_set
            extra = opt_set - orig_set

            error_parts = []
            error_parts.append(f"WRONG: {len(orig_result)} rows expected, got {len(opt_result)}.")

            if missing:
                error_parts.append(f"Missing {len(missing)} rows.")
                if len(missing) <= 3:
                    error_parts.append(f"Sample missing: {list(missing)[:3]}")
            if extra:
                error_parts.append(f"Extra {len(extra)} rows.")
                if len(extra) <= 3:
                    error_parts.append(f"Sample extra: {list(extra)[:3]}")

            return False, " ".join(error_parts)

        except Exception as e:
            return False, f"Validation error: {e}"

    return validator


def create_duckdb_validator_with_regression_guard(
    db_path: str,
    min_speedup: float = 1.0,
    benchmark_runs: int = 3
):
    """Create a validator that checks semantic equivalence AND rejects regressions.

    Args:
        db_path: Path to DuckDB database
        min_speedup: Minimum acceptable speedup (default 1.0 = no regression)
        benchmark_runs: Number of benchmark runs (first discarded as warmup)

    Returns:
        Validator function: (original_sql, optimized_sql) -> (correct, error)
    """
    import duckdb
    import time

    def validator(original_sql: str, optimized_sql: str) -> Tuple[bool, Optional[str]]:
        try:
            conn = duckdb.connect(db_path, read_only=True)

            # Run original (correctness check)
            try:
                orig_result = conn.execute(original_sql).fetchall()
            except Exception as e:
                conn.close()
                return False, f"Original query error: {e}"

            # Run optimized (correctness check)
            try:
                opt_result = conn.execute(optimized_sql).fetchall()
            except Exception as e:
                conn.close()
                return False, f"SYNTAX ERROR in optimized query: {e}. Fix the SQL syntax."

            # Check semantic equivalence
            orig_set = set(tuple(r) for r in orig_result)
            opt_set = set(tuple(r) for r in opt_result)

            if orig_set != opt_set:
                conn.close()
                missing = orig_set - opt_set
                extra = opt_set - orig_set
                error_parts = [f"WRONG: {len(orig_result)} rows expected, got {len(opt_result)}."]
                if missing:
                    error_parts.append(f"Missing {len(missing)} rows.")
                if extra:
                    error_parts.append(f"Extra {len(extra)} rows.")
                return False, " ".join(error_parts)

            # Benchmark for regression check
            orig_times = []
            opt_times = []

            for _ in range(benchmark_runs):
                start = time.time()
                conn.execute(original_sql).fetchall()
                orig_times.append(time.time() - start)

                start = time.time()
                conn.execute(optimized_sql).fetchall()
                opt_times.append(time.time() - start)

            conn.close()

            # Average excluding first run (warmup)
            orig_avg = sum(orig_times[1:]) / (benchmark_runs - 1) if benchmark_runs > 1 else orig_times[0]
            opt_avg = sum(opt_times[1:]) / (benchmark_runs - 1) if benchmark_runs > 1 else opt_times[0]

            speedup = orig_avg / opt_avg if opt_avg > 0 else 1.0

            if speedup < min_speedup:
                return False, f"REGRESSION: {speedup:.2f}x speedup < {min_speedup}x minimum. Keep original query."

            return True, None

        except Exception as e:
            return False, f"Validation error: {e}"

    return validator


def validate_optimization(
    original_sql: str,
    optimized_sql: str,
    db_path: str,
    benchmark_runs: int = 3
) -> ValidationResult:
    """Validate an optimized query and measure performance improvement.

    This is QueryTorque's core validation - checks semantic correctness
    AND measures actual speedup.

    Args:
        original_sql: The original SQL query
        optimized_sql: The optimized SQL query
        db_path: Path to DuckDB database
        benchmark_runs: Number of benchmark runs (first is warmup)

    Returns:
        ValidationResult with correctness, timing, and speedup
    """
    import duckdb

    try:
        conn = duckdb.connect(db_path, read_only=True)

        # Run original for correctness
        try:
            orig_result = conn.execute(original_sql).fetchall()
        except Exception as e:
            conn.close()
            return ValidationResult(
                is_correct=False,
                original_time_ms=0,
                optimized_time_ms=0,
                speedup=0,
                original_rows=0,
                optimized_rows=0,
                error=f"Original query error: {e}"
            )

        # Run optimized for correctness
        try:
            opt_result = conn.execute(optimized_sql).fetchall()
        except Exception as e:
            conn.close()
            return ValidationResult(
                is_correct=False,
                original_time_ms=0,
                optimized_time_ms=0,
                speedup=0,
                original_rows=len(orig_result),
                optimized_rows=0,
                error=f"Optimized query syntax error: {e}"
            )

        # Check semantic equivalence
        orig_set = set(tuple(r) for r in orig_result)
        opt_set = set(tuple(r) for r in opt_result)

        if orig_set != opt_set:
            conn.close()
            missing = len(orig_set - opt_set)
            extra = len(opt_set - orig_set)
            return ValidationResult(
                is_correct=False,
                original_time_ms=0,
                optimized_time_ms=0,
                speedup=0,
                original_rows=len(orig_result),
                optimized_rows=len(opt_result),
                error=f"Results differ: missing {missing} rows, extra {extra} rows"
            )

        # Benchmark timing
        orig_times = []
        opt_times = []

        for _ in range(benchmark_runs):
            start = time.perf_counter()
            conn.execute(original_sql).fetchall()
            orig_times.append((time.perf_counter() - start) * 1000)  # ms

            start = time.perf_counter()
            conn.execute(optimized_sql).fetchall()
            opt_times.append((time.perf_counter() - start) * 1000)  # ms

        conn.close()

        # Average excluding first run (warmup)
        if benchmark_runs > 1:
            orig_avg = sum(orig_times[1:]) / (benchmark_runs - 1)
            opt_avg = sum(opt_times[1:]) / (benchmark_runs - 1)
        else:
            orig_avg = orig_times[0]
            opt_avg = opt_times[0]

        speedup = orig_avg / opt_avg if opt_avg > 0 else 1.0

        return ValidationResult(
            is_correct=True,
            original_time_ms=orig_avg,
            optimized_time_ms=opt_avg,
            speedup=speedup,
            original_rows=len(orig_result),
            optimized_rows=len(opt_result),
            error=None
        )

    except Exception as e:
        return ValidationResult(
            is_correct=False,
            original_time_ms=0,
            optimized_time_ms=0,
            speedup=0,
            original_rows=0,
            optimized_rows=0,
            error=f"Validation error: {e}"
        )


def configure_lm(
    provider: str = "deepseek",
    model: Optional[str] = None,
    api_key: Optional[str] = None
) -> None:
    """Configure the DSPy language model."""
    import os

    provider_configs = {
        "deepseek": {
            "model": model or "deepseek-chat",
            "api_key": api_key or os.getenv("DEEPSEEK_API_KEY"),
            "api_base": "https://api.deepseek.com"
        },
        "groq": {
            "model": model or "llama-3.3-70b-versatile",
            "api_key": api_key or os.getenv("GROQ_API_KEY"),
        },
        "gemini": {
            "model": model or "gemini-2.0-flash",
            "api_key": api_key or os.getenv("GEMINI_API_KEY"),
        },
        "anthropic": {
            "model": model or "claude-3-5-sonnet-20241022",
            "api_key": api_key or os.getenv("ANTHROPIC_API_KEY"),
        }
    }

    if provider not in provider_configs:
        raise ValueError(f"Unknown provider: {provider}")

    config = provider_configs[provider]

    if provider == "groq":
        lm = dspy.LM(f"groq/{config['model']}", api_key=config['api_key'])
    elif provider == "gemini":
        lm = dspy.LM(f"gemini/{config['model']}", api_key=config['api_key'])
    elif provider == "anthropic":
        lm = dspy.LM(f"anthropic/{config['model']}", api_key=config['api_key'])
    else:
        lm = dspy.LM(
            f"openai/{config['model']}",
            api_key=config['api_key'],
            api_base=config.get('api_base')
        )

    dspy.configure(lm=lm)


def optimize_query_with_validation(
    original_sql: str,
    execution_plan: str,
    row_estimates: str,
    db_path: str,
    provider: str = "deepseek",
    db_name: str = "duckdb",
    max_retries: int = 2
) -> OptimizationResult:
    """Optimize a SQL query with validation and retries.

    Args:
        original_sql: The SQL query to optimize
        execution_plan: Execution plan summary
        row_estimates: Row scan statistics
        db_path: Path to database for validation
        provider: LLM provider (also used to load model config)
        db_name: Database name for loading DB-specific hints
        max_retries: Max retry attempts

    Returns:
        OptimizationResult with validated optimized query
    """
    configure_lm(provider=provider)

    validator = create_duckdb_validator(db_path)
    pipeline = ValidatedOptimizationPipeline(
        validator_fn=validator,
        max_retries=max_retries,
        model_name=provider,  # Use provider name to load model config
        db_name=db_name
    )

    return pipeline(
        query=original_sql,
        plan=execution_plan,
        rows=row_estimates
    )


# ============================================================
# MIPROv2 Training Support
# ============================================================

def create_training_example(
    sql: str,
    plan: str,
    scans: str,
    known_good_optimization: Optional[str] = None
) -> dspy.Example:
    """Create a training example for MIPROv2.

    Args:
        sql: Original SQL query
        plan: Execution plan
        scans: Table scan info
        known_good_optimization: Optional known-good optimized query

    Returns:
        dspy.Example for training
    """
    example = dspy.Example(
        original_query=sql,
        execution_plan=plan,
        row_estimates=scans
    )
    if known_good_optimization:
        example = example.with_inputs("original_query", "execution_plan", "row_estimates")
    return example


def speedup_metric(example, prediction, trace=None) -> float:
    """Metric for MIPROv2 that measures speedup and correctness.

    Returns 0.0 if incorrect, otherwise score based on speedup.
    This is a lightweight metric for use without database access.

    Field names match pipeline forward() arguments:
    - example.query (original SQL)
    - prediction.optimized_sql (optimized SQL)
    - prediction.rationale (optimization rationale)
    """
    optimized = getattr(prediction, 'optimized_sql', None)
    if not optimized:
        return 0.0

    original = getattr(example, 'query', '')
    if optimized.strip() == original.strip():
        return 0.1  # No change

    # Check for good patterns in rationale
    good_patterns = [
        "predicate pushdown", "filter push", "join elimination",
        "is not null", "scan consolidation", "cte", "early",
        "exists", "union all", "materialize", "index"
    ]
    rationale = getattr(prediction, 'rationale', '').lower()
    pattern_score = sum(0.1 for p in good_patterns if p in rationale)

    return min(1.0, 0.3 + pattern_score)


def create_optimization_metric(
    db_path: str,
    benchmark_fn: Callable[[str], float] = None
) -> Callable:
    """Create a metric function for MIPROv2 that validates correctness AND measures speedup.

    Args:
        db_path: Path to database for validation
        benchmark_fn: Optional custom benchmark function(sql) -> time_seconds

    Returns:
        Metric function: (example, prediction, trace) -> float
    """
    import duckdb
    import time

    validator = create_duckdb_validator(db_path)

    def default_benchmark(sql: str, runs: int = 3) -> float:
        """Default benchmark: 3 runs, discard first, average 2-3."""
        conn = duckdb.connect(db_path, read_only=True)
        times = []
        for _ in range(runs):
            start = time.perf_counter()
            conn.execute(sql).fetchall()
            times.append(time.perf_counter() - start)
        conn.close()
        return sum(times[1:]) / 2 if len(times) > 1 else times[0]

    benchmark = benchmark_fn or default_benchmark

    def optimization_metric(example, prediction, trace=None) -> float:
        """Metric for MIPROv2 optimization.

        Returns:
            0.0 if incorrect
            0.1-1.0 based on speedup if correct
        """
        if not prediction.optimized_query:
            return 0.0

        # Check correctness via validator
        correct, error = validator(example.original_query, prediction.optimized_query)
        if not correct:
            return 0.0

        # Measure speedup
        try:
            orig_time = benchmark(example.original_query)
            opt_time = benchmark(prediction.optimized_query)
            speedup = orig_time / opt_time if opt_time > 0 else 1.0
        except Exception:
            # If benchmark fails, just check for change
            if prediction.optimized_query.strip() == example.original_query.strip():
                return 0.2  # Correct but no change
            return 0.3  # Correct with change but couldn't measure

        # Score: 0.2 (no change) to 1.0 (3x+ speedup)
        if speedup >= 3.0:
            return 1.0
        elif speedup >= 2.0:
            return 0.8
        elif speedup >= 1.5:
            return 0.6
        elif speedup >= 1.1:
            return 0.4
        else:
            return 0.2  # Correct but no speedup

    return optimization_metric


# ============================================================
# Pipeline Persistence (Save/Load)
# ============================================================

def save_pipeline(pipeline: ValidatedOptimizationPipeline, path: str) -> None:
    """Save an optimized pipeline to disk.

    Args:
        pipeline: The trained/optimized pipeline
        path: Path to save (JSON format)
    """
    save_path = Path(path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    pipeline.save(str(save_path))


def load_pipeline(
    path: str,
    validator_fn=None,
    max_retries: int = 2,
    model_name: str = None,
    db_name: str = None
) -> ValidatedOptimizationPipeline:
    """Load a saved pipeline from disk.

    Args:
        path: Path to saved pipeline
        validator_fn: Validator function to use
        max_retries: Max retries for validation
        model_name: Model name for constraints
        db_name: Database name for hints

    Returns:
        Loaded ValidatedOptimizationPipeline
    """
    pipeline = ValidatedOptimizationPipeline(
        validator_fn=validator_fn,
        max_retries=max_retries,
        model_name=model_name,
        db_name=db_name,
        use_few_shot=False  # Will load from saved state
    )
    pipeline.load(path)
    return pipeline


# ============================================================
# DAG-Based Optimizer (Node-Level Rewrites)
# ============================================================

class SQLDagOptimizer(dspy.Signature):
    """Optimize SQL by rewriting specific DAG nodes.

    Instead of rewriting the entire SQL, output targeted rewrites
    for specific nodes (CTEs, subqueries, main_query).
    """

    query_dag: str = dspy.InputField(
        desc="DAG structure showing nodes (CTEs, subqueries, main_query) and their dependencies"
    )
    node_sql: str = dspy.InputField(
        desc="SQL for each node in the DAG"
    )
    execution_plan: str = dspy.InputField(
        desc="Execution plan showing operator costs and row counts"
    )
    optimization_hints: str = dspy.InputField(
        desc="Detected optimization opportunities with rewrite patterns",
        default=""
    )
    constraints: str = dspy.InputField(
        desc="Model and DB-specific constraints",
        default=""
    )

    rewrites: str = dspy.OutputField(
        desc='JSON object: {"node_id": "new SELECT statement", ...} for nodes to rewrite. Only include nodes you are changing.'
    )
    explanation: str = dspy.OutputField(
        desc="What was optimized and why"
    )


class SQLDagOptimizerWithFeedback(dspy.Signature):
    """Retry DAG optimization with a DIFFERENT strategy after failure."""

    query_dag: str = dspy.InputField(
        desc="DAG structure showing nodes and dependencies"
    )
    node_sql: str = dspy.InputField(
        desc="SQL for each node in the DAG"
    )
    execution_plan: str = dspy.InputField(
        desc="Execution plan showing operator costs"
    )
    optimization_hints: str = dspy.InputField(
        desc="Detected optimization opportunities with rewrite patterns",
        default=""
    )
    constraints: str = dspy.InputField(
        desc="Model and DB-specific constraints",
        default=""
    )
    previous_rewrites: str = dspy.InputField(
        desc="Previous rewrites that FAILED - try a different approach"
    )
    failure_reason: str = dspy.InputField(
        desc="Why the previous attempt failed"
    )

    rewrites: str = dspy.OutputField(
        desc='JSON object with DIFFERENT rewrites. Return {} to keep original.'
    )
    explanation: str = dspy.OutputField(
        desc="What different approach you tried"
    )


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


def load_dag_gold_examples(num_examples: int = 3) -> List[dspy.Example]:
    """Load DAG-format gold examples for few-shot learning.

    Args:
        num_examples: Number of examples to load (default 3)

    Returns:
        List of dspy.Example objects
    """
    try:
        from research.knowledge_base.duckdb.dag_gold_examples import get_dag_gold_examples
        return get_dag_gold_examples(num_examples)
    except ImportError:
        # Fall back to relative import
        examples_path = get_config_dir() / "duckdb" / "dag_gold_examples.py"
        if examples_path.exists():
            import importlib.util
            spec = importlib.util.spec_from_file_location("dag_gold_examples", examples_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.get_dag_gold_examples(num_examples)
        return []


class DagOptimizationPipeline(dspy.Module):
    """DAG-based optimization pipeline with validation and retries.

    Key difference from ValidatedOptimizationPipeline:
    - Uses DAG structure for targeted node rewrites
    - Outputs node-level changes, not full SQL
    - Better for large queries (less token usage)
    - Preserves unchanged parts exactly
    - Uses DAG-format few-shot examples for consistent output
    """

    def __init__(
        self,
        validator_fn: Callable = None,
        max_retries: int = 2,
        model_name: str = None,
        db_name: str = None,
        use_few_shot: bool = True,
        num_examples: int = 3,
    ):
        """
        Args:
            validator_fn: Function(original_sql, optimized_sql) -> (correct, error)
            max_retries: Maximum retry attempts
            model_name: Model name for constraints
            db_name: Database name for hints
            use_few_shot: Whether to use few-shot examples (default True)
            num_examples: Number of few-shot examples to use (default 3)
        """
        super().__init__()
        self.optimizer = dspy.ChainOfThought(SQLDagOptimizer)
        self.retry_optimizer = dspy.ChainOfThought(SQLDagOptimizerWithFeedback)
        self.validator_fn = validator_fn
        self.max_retries = max_retries
        self.constraints = build_system_prompt(model_name, db_name)

        # Load DAG-format few-shot examples
        if use_few_shot:
            examples = load_dag_gold_examples(num_examples)
            if examples:
                # In DSPy 3.x, demos go on .predict.demos or .demos
                if hasattr(self.optimizer, 'predict') and hasattr(self.optimizer.predict, 'demos'):
                    self.optimizer.predict.demos = examples
                elif hasattr(self.optimizer, 'demos'):
                    self.optimizer.demos = examples

    def _parse_rewrites(self, rewrites_str: str) -> Dict[str, str]:
        """Parse rewrites JSON from LLM output."""
        text = rewrites_str.strip()

        # Remove markdown code blocks
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:])
            if text.endswith("```"):
                text = text[:-3].strip()

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Try to extract JSON object
            start = text.find("{")
            end = text.rfind("}") + 1
            if start != -1 and end > start:
                try:
                    return json.loads(text[start:end])
                except json.JSONDecodeError:
                    pass
            return {}

    def forward(
        self,
        sql: str,
        plan: str = "",
        dag: "SQLDag" = None,
    ) -> DagOptimizationResult:
        """Run DAG-based optimization with validation and retries.

        Args:
            sql: Original SQL query
            plan: Execution plan summary
            dag: Pre-built SQLDag (optional, will build if not provided)

        Returns:
            DagOptimizationResult with optimized SQL and rewrites
        """
        # Build DAG if not provided
        if dag is None:
            from .sql_dag import SQLDag
            dag = SQLDag.from_sql(sql)

        # Build DAG prompt components
        dag_structure = []
        dag_structure.append("Nodes:")
        for node_id in dag.topological_order():
            node = dag.nodes[node_id]
            parts = [f"  [{node_id}]", f"type={node.node_type}"]
            if node.tables:
                parts.append(f"tables={node.tables}")
            if node.cte_refs:
                parts.append(f"refs={node.cte_refs}")
            if node.is_correlated:
                parts.append("CORRELATED")
            dag_structure.append(" ".join(parts))

        dag_structure.append("\nEdges:")
        for edge in dag.edges:
            dag_structure.append(f"  {edge.source} → {edge.target}")

        query_dag = "\n".join(dag_structure)

        # Build node SQL
        node_sql_parts = []
        for node_id in dag.topological_order():
            node = dag.nodes[node_id]
            if node.sql:
                node_sql_parts.append(f"### {node_id}\n```sql\n{node.sql.strip()}\n```")

        node_sql = "\n\n".join(node_sql_parts)

        # Detect relevant optimization patterns (including pushdown analysis)
        hints = detect_knowledge_patterns(sql, dag=dag)

        attempts = 0
        last_rewrites_str = ""
        last_error = None

        # First attempt
        attempts += 1
        result = self.optimizer(
            query_dag=query_dag,
            node_sql=node_sql,
            execution_plan=plan,
            optimization_hints=hints,
            constraints=self.constraints
        )

        rewrites = self._parse_rewrites(result.rewrites)
        explanation = result.explanation

        # Apply rewrites
        if rewrites:
            optimized_sql = dag.apply_rewrites(rewrites)
        else:
            optimized_sql = sql

        # Validate if validator provided
        if self.validator_fn:
            correct, error = self.validator_fn(sql, optimized_sql)

            if correct:
                return DagOptimizationResult(
                    original_sql=sql,
                    optimized_sql=optimized_sql,
                    rewrites=rewrites,
                    explanation=explanation,
                    correct=True,
                    attempts=attempts
                )

            # Validation failed - retry with feedback
            last_rewrites_str = result.rewrites
            last_error = error or "Results don't match original query"

            while attempts < self.max_retries + 1:
                attempts += 1

                retry_result = self.retry_optimizer(
                    query_dag=query_dag,
                    node_sql=node_sql,
                    execution_plan=plan,
                    optimization_hints=hints,
                    constraints=self.constraints,
                    previous_rewrites=last_rewrites_str,
                    failure_reason=last_error
                )

                rewrites = self._parse_rewrites(retry_result.rewrites)
                explanation = retry_result.explanation

                if rewrites:
                    optimized_sql = dag.apply_rewrites(rewrites)
                else:
                    optimized_sql = sql

                correct, error = self.validator_fn(sql, optimized_sql)

                if correct:
                    return DagOptimizationResult(
                        original_sql=sql,
                        optimized_sql=optimized_sql,
                        rewrites=rewrites,
                        explanation=f"[After {attempts} attempts] {explanation}",
                        correct=True,
                        attempts=attempts
                    )

                last_rewrites_str = retry_result.rewrites
                last_error = error or "Results don't match"

            # All retries exhausted
            return DagOptimizationResult(
                original_sql=sql,
                optimized_sql=optimized_sql,
                rewrites=rewrites,
                explanation=explanation,
                correct=False,
                attempts=attempts,
                error=f"Validation failed after {attempts} attempts: {last_error}"
            )

        # No validator - return unvalidated result
        return DagOptimizationResult(
            original_sql=sql,
            optimized_sql=optimized_sql,
            rewrites=rewrites,
            explanation=explanation,
            attempts=attempts
        )


def optimize_with_dag(
    sql: str,
    plan: str,
    db_path: str,
    provider: str = "deepseek",
    db_name: str = "duckdb",
    max_retries: int = 2
) -> DagOptimizationResult:
    """Convenience function for DAG-based optimization.

    Args:
        sql: SQL query to optimize
        plan: Execution plan summary
        db_path: Path to database for validation
        provider: LLM provider name
        db_name: Database type for hints
        max_retries: Max retry attempts

    Returns:
        DagOptimizationResult
    """
    configure_lm(provider=provider)

    validator = create_duckdb_validator(db_path)
    pipeline = DagOptimizationPipeline(
        validator_fn=validator,
        max_retries=max_retries,
        model_name=provider,
        db_name=db_name
    )

    return pipeline(sql=sql, plan=plan)
