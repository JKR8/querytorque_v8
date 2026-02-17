"""Ollama LLM client (local, self-hosted)."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Dict, Optional

from .rate_limit import llm_call_guard

logger = logging.getLogger(__name__)


class OllamaClient:
    """LLM client for a local Ollama server.

    Uses the non-streaming `/api/generate` endpoint so callers receive one
    complete response string, matching the `LLMClient` protocol.
    """

    def __init__(
        self,
        model: str = "qwen2.5-coder:7b-instruct",
        base_url: str = "http://127.0.0.1:11434",
        temperature: float = 0.0,
        num_ctx: int = 8192,
        timeout_s: int = 120,
    ):
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.temperature = temperature
        self.num_ctx = num_ctx
        self.timeout_s = timeout_s
        self.last_usage: Dict[str, int] = {}
        logger.info(
            "Initialized OllamaClient: model=%s, base_url=%s",
            self.model,
            self.base_url,
        )

    def analyze(self, prompt: str, max_tokens: Optional[int] = None) -> str:
        """Send a prompt to Ollama and return model text output."""
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": self.temperature,
                "num_ctx": self.num_ctx,
            },
        }
        if max_tokens is not None:
            payload["options"]["num_predict"] = int(max_tokens)

        req = urllib.request.Request(
            url=f"{self.base_url}/api/generate",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with llm_call_guard():
                with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            raise RuntimeError(
                "Failed to reach Ollama. Ensure `ollama serve` is running "
                f"at {self.base_url} and model `{self.model}` is available."
            ) from e

        text = body.get("response", "") or ""
        self.last_usage = {
            "prompt_tokens": int(body.get("prompt_eval_count") or 0),
            "completion_tokens": int(body.get("eval_count") or 0),
            "total_tokens": int((body.get("prompt_eval_count") or 0) + (body.get("eval_count") or 0)),
        }
        return text
