"""DSPy-based DAX optimizer with validation against Power BI Desktop."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import dspy

from qt_dax.validation import DAXEquivalenceValidator


@dataclass
class DAXOptimizationResult:
    """Result of a DSPy DAX optimization attempt."""

    optimized_dax: str = ""
    rationale: str = ""
    attempts: int = 0
    correct: bool = False
    speedup_ratio: float = 1.0
    status: str = "error"
    error: str = ""
    warnings: list[str] = None

    def __post_init__(self) -> None:
        if self.warnings is None:
            self.warnings = []


class DAXOptimizer(dspy.Signature):
    """Optimize a single DAX measure."""

    measure_name: str = dspy.InputField(
        desc="Name of the DAX measure"
    )
    original_dax: str = dspy.InputField(
        desc="Original DAX expression for the measure"
    )
    issues: str = dspy.InputField(
        desc="Detected issues and guidance for the measure"
    )
    constraints: str = dspy.InputField(
        desc="Hard constraints that must be preserved"
    )

    optimized_dax: str = dspy.OutputField(
        desc="Optimized DAX expression only (no measure name, no EVALUATE)"
    )
    rationale: str = dspy.OutputField(
        desc="Brief rationale for changes"
    )


class DAXOptimizerWithFeedback(dspy.Signature):
    """Retry optimization using validation feedback."""

    measure_name: str = dspy.InputField(
        desc="Name of the DAX measure"
    )
    original_dax: str = dspy.InputField(
        desc="Original DAX expression for the measure"
    )
    issues: str = dspy.InputField(
        desc="Detected issues and guidance for the measure"
    )
    constraints: str = dspy.InputField(
        desc="Hard constraints that must be preserved"
    )
    previous_attempt: str = dspy.InputField(
        desc="Previous optimized DAX attempt"
    )
    failure_reason: str = dspy.InputField(
        desc="Why the previous attempt failed validation"
    )

    optimized_dax: str = dspy.OutputField(
        desc="Corrected optimized DAX expression"
    )
    rationale: str = dspy.OutputField(
        desc="Brief rationale for changes"
    )


def configure_lm(
    provider: str = "deepseek",
    model: Optional[str] = None,
    api_key: Optional[str] = None,
) -> None:
    """Configure the DSPy language model."""
    import os

    provider_configs = {
        "deepseek": {
            "model": model or "deepseek-chat",
            "api_key": api_key or os.getenv("DEEPSEEK_API_KEY"),
            "api_base": "https://api.deepseek.com",
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
        },
    }

    if provider not in provider_configs:
        raise ValueError(f"Unknown provider: {provider}")

    config = provider_configs[provider]

    if provider == "groq":
        lm = dspy.LM(f"groq/{config['model']}", api_key=config["api_key"])
    elif provider == "gemini":
        lm = dspy.LM(f"gemini/{config['model']}", api_key=config["api_key"])
    elif provider == "anthropic":
        lm = dspy.LM(f"anthropic/{config['model']}", api_key=config["api_key"])
    else:
        lm = dspy.LM(
            f"openai/{config['model']}",
            api_key=config["api_key"],
            api_base=config.get("api_base"),
        )

    dspy.configure(lm=lm)


def optimize_measure_with_validation(
    *,
    measure_name: str,
    original_dax: str,
    issues_text: str,
    validator: DAXEquivalenceValidator,
    provider: str = "deepseek",
    model: Optional[str] = None,
    max_retries: int = 2,
) -> DAXOptimizationResult:
    """Optimize a DAX measure with validation and retries."""
    configure_lm(provider=provider, model=model)

    constraints = (
        "Preserve semantics exactly. Return only the DAX expression (no measure name). "
        "Do not use EVALUATE or DEFINE. Keep it compatible with Power BI." 
        "Prefer readable variables and avoid unnecessary iterators."
    )

    optimizer = dspy.ChainOfThought(DAXOptimizer)
    retry_optimizer = dspy.ChainOfThought(DAXOptimizerWithFeedback)

    attempts = 0
    last_error = ""
    result = DAXOptimizationResult()

    while attempts <= max_retries:
        attempts += 1

        if attempts == 1:
            prediction = optimizer(
                measure_name=measure_name,
                original_dax=original_dax,
                issues=issues_text,
                constraints=constraints,
            )
        else:
            prediction = retry_optimizer(
                measure_name=measure_name,
                original_dax=original_dax,
                issues=issues_text,
                constraints=constraints,
                previous_attempt=result.optimized_dax or original_dax,
                failure_reason=last_error or "Validation failed",
            )

        optimized = (prediction.optimized_dax or "").strip()
        rationale = (prediction.rationale or "").strip()

        result.optimized_dax = optimized
        result.rationale = rationale
        result.attempts = attempts

        if not optimized:
            last_error = "Empty optimized DAX"
            result.error = last_error
            result.status = "error"
            continue

        validation = validator.validate(original_dax, optimized)
        result.status = validation.status
        result.speedup_ratio = validation.speedup_ratio
        result.warnings = list(validation.warnings)

        if validation.status == "pass":
            result.correct = True
            return result

        last_error = "; ".join(validation.errors) or "Validation failed"
        result.error = last_error

    result.correct = False
    return result
