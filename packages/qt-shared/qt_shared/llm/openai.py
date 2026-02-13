"""OpenAI LLM client."""

import logging
import time

logger = logging.getLogger(__name__)


class OpenAIClient:
    """LLM client for OpenAI API.

    Supports GPT-4o and other OpenAI models.
    Also supports OpenAI-compatible APIs (OpenRouter, etc.) via base_url.
    """

    def __init__(self, api_key: str, model: str = "gpt-4o", base_url: str = None):
        """Initialize OpenAI client.

        Args:
            api_key: OpenAI API key
            model: Model name. Options include:
                - gpt-4o (recommended)
                - gpt-4o-mini
                - gpt-4-turbo
            base_url: Optional base URL for OpenAI-compatible APIs (e.g., OpenRouter)
        """
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.last_usage: dict = {}
        logger.info("Initialized OpenAIClient with model=%s, base_url=%s", model, base_url)

    def analyze(self, prompt: str) -> str:
        """Send prompt to OpenAI and return response."""
        try:
            from openai import OpenAI
        except ImportError:
            logger.error("openai package not installed")
            raise ImportError("openai package required: pip install openai")

        logger.debug("Sending request to OpenAI API (prompt=%d chars)", len(prompt))
        start_time = time.time()

        client = OpenAI(api_key=self.api_key, base_url=self.base_url)

        # Kimi K2.5 via OpenRouter supports up to 128k context, 8k output
        # GPT-4o supports 128k context, 16k output
        max_tokens = 8192

        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "user", "content": prompt},
            ],
            max_tokens=max_tokens,
            temperature=0.0,
        )

        duration = time.time() - start_time
        response_text = response.choices[0].message.content

        # Capture token usage and cache metrics
        self.last_usage = {}
        if hasattr(response, 'usage') and response.usage:
            u = response.usage
            self.last_usage = {
                "prompt_tokens": getattr(u, 'prompt_tokens', 0),
                "completion_tokens": getattr(u, 'completion_tokens', 0),
                "total_tokens": getattr(u, 'total_tokens', 0),
                # DeepSeek via OpenRouter
                "prompt_cache_hit_tokens": getattr(u, 'prompt_cache_hit_tokens', 0),
                "prompt_cache_miss_tokens": getattr(u, 'prompt_cache_miss_tokens', 0),
                # OpenAI native
                "cached_tokens": getattr(
                    getattr(u, 'prompt_tokens_details', None), 'cached_tokens', 0
                ),
            }

        logger.info(
            "OpenAI API response: model=%s, duration=%.2fs, response=%d chars, "
            "cache_hit=%d, cache_miss=%d",
            self.model, duration, len(response_text),
            self.last_usage.get("prompt_cache_hit_tokens", 0),
            self.last_usage.get("prompt_cache_miss_tokens", 0),
        )

        return response_text
