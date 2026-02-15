"""Candidate generation (parallel workers).

This module generates optimization candidates using LLM inference.
Supports parallel generation with multiple workers.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, List, Optional

from .sql_rewriter import SQLRewriter, extract_transforms_from_response

logger = logging.getLogger(__name__)


@dataclass
class Candidate:
    """An optimization candidate from a worker."""
    worker_id: int
    prompt: str
    response: str
    optimized_sql: str
    examples_used: list[str]
    transforms: list[str]
    style: Optional[str] = None
    error: Optional[str] = None
    set_local_commands: list[str] = None
    interface_warnings: list[str] = None

    def __post_init__(self):
        if self.set_local_commands is None:
            self.set_local_commands = []
        if self.interface_warnings is None:
            self.interface_warnings = []


class CandidateGenerator:
    """Generate N optimization candidates in parallel.

    Uses qt_shared.llm for LLM inference when available,
    or accepts a custom analyze_fn callback.
    """

    def __init__(
        self,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        analyze_fn: Optional[Callable[[str], str]] = None,
    ):
        """Initialize generator.

        Args:
            provider: LLM provider (anthropic, openai, deepseek, etc.)
            model: Model name to use
            analyze_fn: Optional custom LLM function (overrides provider/model)
        """
        self.provider = provider
        self.model = model
        self._analyze_fn = analyze_fn
        self._llm_client = None

    def _get_llm_client(self):
        """Get or create LLM client."""
        if self._llm_client is None and self._analyze_fn is None:
            try:
                from qt_shared.llm import create_llm_client
                self._llm_client = create_llm_client(
                    provider=self.provider,
                    model=self.model,
                )
            except ImportError:
                logger.warning("qt_shared.llm not available")
                self._llm_client = None

        return self._llm_client

    def _analyze(self, prompt: str) -> str:
        """Send prompt to LLM and get response.

        Args:
            prompt: The optimization prompt

        Returns:
            LLM response text
        """
        # Use custom analyze_fn if provided
        if self._analyze_fn is not None:
            return self._analyze_fn(prompt)

        # Use LLM client
        client = self._get_llm_client()
        if client is None:
            raise RuntimeError(
                "No LLM client available. Either provide analyze_fn, "
                "or install qt_shared and configure LLM provider."
            )

        return client.analyze(prompt)

    def _analyze_with_max_tokens(
        self, prompt: str, max_tokens: int = 4096
    ) -> str:
        """Analyst call with explicit max_tokens.

        The analyst briefing needs ~2000-3200 output tokens. DeepSeek defaults
        to 16384 (sufficient), but Anthropic/OpenAI default to 4096.
        Falls back to _analyze() if the client doesn't support max_tokens.

        Args:
            prompt: The analysis prompt
            max_tokens: Maximum output tokens

        Returns:
            LLM response text
        """
        # Custom analyze_fn doesn't support max_tokens — use it directly
        if self._analyze_fn is not None:
            return self._analyze_fn(prompt)

        client = self._get_llm_client()
        if client is None:
            raise RuntimeError(
                "No LLM client available. Either provide analyze_fn, "
                "or install qt_shared and configure LLM provider."
            )

        # Try calling with max_tokens if the client supports it
        try:
            if hasattr(client, "analyze"):
                import inspect
                sig = inspect.signature(client.analyze)
                if "max_tokens" in sig.parameters:
                    return client.analyze(prompt, max_tokens=max_tokens)
        except Exception:
            pass

        # Fallback: use default analyze
        return client.analyze(prompt)

    def generate_one(
        self,
        sql: str,
        prompt: str,
        examples_used: list[str],
        worker_id: int,
        dialect: str = "duckdb",
        script_ir=None,
    ) -> Candidate:
        """Generate a single optimization candidate.

        Args:
            sql: Original SQL query
            prompt: Optimization prompt
            examples_used: List of example IDs used in prompt
            worker_id: Worker identifier
            dialect: SQL dialect
            script_ir: Optional ScriptIR for patch-mode application

        Returns:
            Candidate with optimized SQL or error
        """
        try:
            # Get LLM response
            response = self._analyze(prompt)

            # Apply rewrite
            rewriter = SQLRewriter(sql, dialect=dialect, script_ir=script_ir)
            result = rewriter.apply_response(response)

            # Use explicit transform from JSON rewrite_set when available,
            # fall back to AST-diff inference for raw SQL responses
            if result.rewrite_set and result.rewrite_set.transform:
                transforms = [result.rewrite_set.transform]
            elif result.transform and result.transform != "semantic_rewrite":
                transforms = [result.transform]
            else:
                transforms = extract_transforms_from_response(
                    response,
                    original_sql=sql,
                    optimized_sql=result.optimized_sql
                )

            return Candidate(
                worker_id=worker_id,
                prompt=prompt,
                response=response,
                optimized_sql=result.optimized_sql,
                examples_used=examples_used,
                transforms=transforms,
                error=result.error if not result.success else None,
                set_local_commands=result.set_local_commands,
                interface_warnings=result.warnings,
            )

        except Exception as e:
            logger.warning(f"Worker {worker_id} failed: {e}")
            return Candidate(
                worker_id=worker_id,
                prompt=prompt,
                response="",
                optimized_sql=sql,  # Return original on error
                examples_used=examples_used,
                transforms=[],
                error=str(e),
            )

    def generate(
        self,
        sql: str,
        prompt: str,
        examples_used: list[str],
        n: int = 10,
        dialect: str = "duckdb",
    ) -> list[Candidate]:
        """Generate N candidates in parallel.

        Args:
            sql: Original SQL query
            prompt: Optimization prompt
            examples_used: List of example IDs used in prompt
            n: Number of candidates to generate
            dialect: SQL dialect

        Returns:
            List of Candidates sorted by worker_id
        """
        results: list[Candidate] = []

        with ThreadPoolExecutor(max_workers=n) as pool:
            tasks = [
                pool.submit(
                    self.generate_one,
                    sql=sql,
                    prompt=prompt,
                    examples_used=examples_used,
                    worker_id=i + 1,
                    dialect=dialect,
                )
                for i in range(n)
            ]

            for task in as_completed(tasks):
                try:
                    candidate = task.result()
                    results.append(candidate)
                except Exception as e:
                    logger.error(f"Task execution failed: {e}")

        # Sort by worker_id for consistent ordering
        return sorted(results, key=lambda c: c.worker_id)

    def generate_sequential(
        self,
        sql: str,
        prompt: str,
        examples_used: list[str],
        n: int = 10,
        dialect: str = "duckdb",
    ) -> list[Candidate]:
        """Generate N candidates sequentially (for debugging).

        Args:
            sql: Original SQL query
            prompt: Optimization prompt
            examples_used: List of example IDs used in prompt
            n: Number of candidates to generate
            dialect: SQL dialect

        Returns:
            List of Candidates sorted by worker_id
        """
        results = []
        for i in range(n):
            candidate = self.generate_one(
                sql=sql,
                prompt=prompt,
                examples_used=examples_used,
                worker_id=i + 1,
                dialect=dialect,
            )
            results.append(candidate)
        return results
