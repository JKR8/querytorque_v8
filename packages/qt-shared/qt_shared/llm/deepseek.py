"""DeepSeek LLM client."""

import logging
import time

from .rate_limit import llm_call_guard

logger = logging.getLogger(__name__)


class DeepSeekClient:
    """LLM client for DeepSeek API.

    DeepSeek API is OpenAI-compatible. Uses deepseek-reasoner for rule selection.
    """

    DEEPSEEK_BASE_URL = "https://api.deepseek.com"

    def __init__(self, api_key: str, model: str = "deepseek-reasoner"):
        """Initialize DeepSeek client.

        Args:
            api_key: DeepSeek API key
            model: Model name. Options include:
                - deepseek-reasoner (R1 reasoning model)
                - deepseek-chat (fast chat model)
        """
        self.api_key = api_key
        self.model = model
        logger.info("Initialized DeepSeekClient with model=%s", model)

    # Store last reasoning for callers that want to inspect/save it
    last_reasoning: str = ""
    last_duration: float = 0.0
    last_usage: dict = {}

    def analyze(self, prompt: str) -> str:
        """Send prompt to DeepSeek and return response."""
        try:
            from openai import OpenAI
        except ImportError:
            logger.error("openai package not installed")
            raise ImportError("openai package required: pip install openai")

        logger.debug("Sending request to DeepSeek API (prompt=%d chars)", len(prompt))
        start_time = time.time()
        with llm_call_guard():
            client = OpenAI(api_key=self.api_key, base_url=self.DEEPSEEK_BASE_URL)

            # DeepSeek chat supports max 8192 output tokens
            # DeepSeek reasoner supports up to 16384
            max_tokens = 16384 if "reasoner" in self.model else 8192

            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt},
                ],
                max_tokens=max_tokens,
            )

        duration = time.time() - start_time
        DeepSeekClient.last_duration = duration
        response_text = response.choices[0].message.content or ""

        # Capture token usage and cache metrics
        self.last_usage = {}
        DeepSeekClient.last_usage = {}
        if hasattr(response, 'usage') and response.usage:
            u = response.usage
            self.last_usage = {
                "prompt_tokens": getattr(u, 'prompt_tokens', 0),
                "completion_tokens": getattr(u, 'completion_tokens', 0),
                "total_tokens": getattr(u, 'total_tokens', 0),
                "prompt_cache_hit_tokens": getattr(u, 'prompt_cache_hit_tokens', 0),
                "prompt_cache_miss_tokens": getattr(u, 'prompt_cache_miss_tokens', 0),
            }
            explicit_cost = getattr(u, "cost", None)
            if isinstance(explicit_cost, (int, float)):
                self.last_usage["cost_usd"] = float(explicit_cost)
            DeepSeekClient.last_usage = self.last_usage

        # R1 reasoner: final answer should be in content, reasoning chain in reasoning_content.
        # Known issue: sometimes content is empty and the JSON answer is in reasoning_content.
        reasoning = getattr(response.choices[0].message, 'reasoning_content', None)
        DeepSeekClient.last_reasoning = reasoning or ""

        if reasoning:
            logger.info(
                "DeepSeek reasoning: %d chars (%.1fs thinking)",
                len(reasoning), duration
            )

        if not response_text.strip() and reasoning:
            logger.warning("DeepSeek content empty, extracting from reasoning_content")
            response_text = self._extract_from_reasoning(reasoning)

        logger.info(
            "DeepSeek API response: model=%s, duration=%.2fs, response=%d chars, "
            "cache_hit=%d, cache_miss=%d",
            self.model, duration, len(response_text),
            self.last_usage.get("prompt_cache_hit_tokens", 0),
            self.last_usage.get("prompt_cache_miss_tokens", 0),
        )

        return response_text

    @staticmethod
    def _extract_from_reasoning(reasoning: str) -> str:
        """Extract JSON answer from reasoning_content when content is empty.

        The reasoner sometimes puts the final JSON in reasoning_content
        instead of content. We look for the last complete JSON block.
        """
        import re

        # Look for ```json ... ``` blocks (last one is usually the final answer)
        json_blocks = re.findall(r'```json\s*(.*?)\s*```', reasoning, re.DOTALL)
        if json_blocks:
            return json_blocks[-1]

        # Look for raw JSON with rewrite_sets
        matches = re.findall(r'\{[^{}]*"rewrite_sets".*?\}(?:\s*\})?', reasoning, re.DOTALL)
        if matches:
            return matches[-1]

        # Last resort: find the last { ... } block
        brace_depth = 0
        last_json_start = -1
        last_json_end = -1
        for i, ch in enumerate(reasoning):
            if ch == '{':
                if brace_depth == 0:
                    last_json_start = i
                brace_depth += 1
            elif ch == '}':
                brace_depth -= 1
                if brace_depth == 0 and last_json_start >= 0:
                    last_json_end = i + 1

        if last_json_start >= 0 and last_json_end > last_json_start:
            candidate = reasoning[last_json_start:last_json_end]
            if '"rewrite_sets"' in candidate:
                return candidate

        # Return full reasoning as fallback
        return reasoning
