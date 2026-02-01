"""
DSPy-based SQL Query Optimizer

This module provides a DSPy-powered optimization pipeline that uses
LLMs to optimize SQL queries based on execution plan analysis.

Key features:
- Signature-based prompting with structured inputs/outputs
- ChainOfThought reasoning for multi-step optimization
- MIPROv2 optimizer support for prompt tuning
- LM portability (DeepSeek, Groq, Gemini, etc.)

Usage:
    from qt_sql.optimization.dspy_optimizer import optimize_query

    result = optimize_query(
        original_sql="SELECT ...",
        execution_plan="...",
        row_estimates="..."
    )
    print(result.optimized_query)
    print(result.optimization_rationale)
"""

from typing import Optional
import dspy


class SQLOptimizer(dspy.Signature):
    """Optimize SQL query for better execution performance.

    Given a SQL query, its execution plan, and row estimates,
    produce an optimized version that maintains identical semantics
    while reducing execution time.
    """

    original_query: str = dspy.InputField(
        desc="The original SQL query to optimize"
    )
    execution_plan: str = dspy.InputField(
        desc="Parsed execution plan showing operator costs and row counts"
    )
    row_estimates: str = dspy.InputField(
        desc="Table scan statistics: table name, rows scanned, filter status"
    )

    optimized_query: str = dspy.OutputField(
        desc="The optimized SQL query with identical semantics"
    )
    optimization_rationale: str = dspy.OutputField(
        desc="Explanation of what was optimized and why it improves performance"
    )


class SQLOptimizationPipeline(dspy.Module):
    """Multi-step SQL optimization pipeline using ChainOfThought reasoning.

    This pipeline:
    1. Analyzes the execution plan to identify bottlenecks
    2. Considers applicable optimization patterns
    3. Applies transformations while preserving semantics
    4. Explains the optimization rationale
    """

    def __init__(self):
        super().__init__()
        self.optimizer = dspy.ChainOfThought(SQLOptimizer)

    def forward(
        self,
        query: str,
        plan: str,
        rows: str
    ) -> dspy.Prediction:
        """Run the optimization pipeline.

        Args:
            query: Original SQL query
            plan: Execution plan summary (operators, costs)
            rows: Row estimate summary (tables, scans, filters)

        Returns:
            Prediction with optimized_query and optimization_rationale
        """
        return self.optimizer(
            original_query=query,
            execution_plan=plan,
            row_estimates=rows
        )


class OptimizationValidator(dspy.Signature):
    """Validate that an optimized query is semantically equivalent."""

    original_query: str = dspy.InputField(
        desc="The original SQL query"
    )
    optimized_query: str = dspy.InputField(
        desc="The optimized SQL query to validate"
    )

    is_equivalent: bool = dspy.OutputField(
        desc="True if queries return identical results, False otherwise"
    )
    differences: str = dspy.OutputField(
        desc="Description of semantic differences, if any"
    )


def configure_lm(
    provider: str = "deepseek",
    model: Optional[str] = None,
    api_key: Optional[str] = None
) -> None:
    """Configure the DSPy language model.

    Args:
        provider: LLM provider ("deepseek", "groq", "gemini", "anthropic")
        model: Model name (defaults to provider's best model)
        api_key: API key (reads from environment if not provided)
    """
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
        raise ValueError(f"Unknown provider: {provider}. Supported: {list(provider_configs.keys())}")

    config = provider_configs[provider]

    if provider == "groq":
        lm = dspy.LM(f"groq/{config['model']}", api_key=config['api_key'])
    elif provider == "gemini":
        lm = dspy.LM(f"gemini/{config['model']}", api_key=config['api_key'])
    elif provider == "anthropic":
        lm = dspy.LM(f"anthropic/{config['model']}", api_key=config['api_key'])
    else:  # deepseek (OpenAI-compatible)
        lm = dspy.LM(
            f"openai/{config['model']}",
            api_key=config['api_key'],
            api_base=config.get('api_base')
        )

    dspy.configure(lm=lm)


def optimize_query(
    original_sql: str,
    execution_plan: str,
    row_estimates: str,
    provider: str = "deepseek",
    model: Optional[str] = None
) -> dspy.Prediction:
    """Optimize a SQL query using DSPy.

    Args:
        original_sql: The SQL query to optimize
        execution_plan: Execution plan summary
        row_estimates: Row scan statistics
        provider: LLM provider to use
        model: Optional model override

    Returns:
        Prediction with optimized_query and optimization_rationale

    Example:
        >>> result = optimize_query(
        ...     original_sql="SELECT * FROM large_table WHERE ...",
        ...     execution_plan="SEQ_SCAN large_table: 67% cost, 5.5M rows",
        ...     row_estimates="large_table: 345M rows (no filter)"
        ... )
        >>> print(result.optimized_query)
        >>> print(result.optimization_rationale)
    """
    configure_lm(provider=provider, model=model)

    pipeline = SQLOptimizationPipeline()
    return pipeline(
        query=original_sql,
        plan=execution_plan,
        rows=row_estimates
    )


# Training metric for MIPROv2 optimization
def speedup_metric(example, prediction, trace=None) -> float:
    """Metric for evaluating optimization quality.

    This metric can be used with MIPROv2 to optimize the prompt.
    It measures execution time improvement and semantic correctness.

    Args:
        example: Expected example with original timing
        prediction: Model prediction with optimized query
        trace: Optional trace for debugging

    Returns:
        Float score (higher is better, max 1.0)
    """
    # This is a placeholder - in production, you would:
    # 1. Execute both queries on test database
    # 2. Compare results for semantic equivalence
    # 3. Measure timing improvement

    # For now, check if optimization was produced
    if not prediction.optimized_query:
        return 0.0

    if prediction.optimized_query.strip() == example.original_query.strip():
        return 0.1  # No change, minimal score

    # Check for common optimization patterns in rationale
    good_patterns = [
        "predicate pushdown",
        "filter push",
        "join elimination",
        "is not null",
        "scan consolidation",
        "case when"
    ]

    rationale_lower = prediction.optimization_rationale.lower()
    pattern_matches = sum(1 for p in good_patterns if p in rationale_lower)

    return min(1.0, 0.3 + (pattern_matches * 0.15))
