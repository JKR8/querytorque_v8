"""Metered LLM client wrapper — records token usage per org for billing."""

import logging
import uuid
from typing import Dict, Optional

from .protocol import LLMClient

logger = logging.getLogger(__name__)


class MeteredLLMClient:
    """Wraps any LLMClient and records per-call token usage.

    After each analyze() call, reads inner.last_usage and writes an
    LLMUsage record to the database (async-safe via sync fallback).

    Usage:
        inner = create_llm_client(...)
        metered = MeteredLLMClient(inner, org_id=org.id, provider="deepseek", model="deepseek-reasoner")
        response = metered.analyze(prompt)
        # LLMUsage row written automatically
    """

    def __init__(
        self,
        inner: LLMClient,
        org_id: uuid.UUID,
        provider: str,
        model: str,
        job_id: Optional[uuid.UUID] = None,
        call_type: str = "optimize",
    ):
        self.inner = inner
        self.org_id = org_id
        self.provider = provider
        self.model = model
        self.job_id = job_id
        self.call_type = call_type
        self.last_usage: Dict[str, int] = {}
        self._total_prompt_tokens = 0
        self._total_completion_tokens = 0
        self._total_cost_usd = 0.0

    def analyze(self, prompt: str) -> str:
        """Send prompt to inner LLM and record usage."""
        response = self.inner.analyze(prompt)
        self.last_usage = getattr(self.inner, "last_usage", {})
        self._record_usage()
        return response

    def _record_usage(self) -> None:
        """Write LLMUsage record to database."""
        usage = self.last_usage
        if not usage:
            return

        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        total_tokens = usage.get("total_tokens", prompt_tokens + completion_tokens)
        cache_hit = usage.get("prompt_cache_hit_tokens", 0) or usage.get("cached_tokens", 0)

        self._total_prompt_tokens += prompt_tokens
        self._total_completion_tokens += completion_tokens

        try:
            from qt_shared.database.connection import get_session_context
            from qt_shared.database.models import LLMUsage
            import asyncio

            async def _write():
                async with get_session_context() as session:
                    record = LLMUsage(
                        org_id=self.org_id,
                        job_id=self.job_id,
                        provider=self.provider,
                        model=self.model,
                        prompt_tokens=prompt_tokens,
                        completion_tokens=completion_tokens,
                        total_tokens=total_tokens,
                        cache_hit_tokens=cache_hit,
                        call_type=self.call_type,
                    )
                    session.add(record)
                    await session.commit()

            try:
                loop = asyncio.get_running_loop()
                loop.create_task(_write())
            except RuntimeError:
                asyncio.run(_write())
        except Exception as e:
            logger.warning("Failed to record LLM usage: %s", e)

    @property
    def total_prompt_tokens(self) -> int:
        return self._total_prompt_tokens

    @property
    def total_completion_tokens(self) -> int:
        return self._total_completion_tokens
