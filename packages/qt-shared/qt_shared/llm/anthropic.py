"""Anthropic Claude LLM client."""

import logging
import time

logger = logging.getLogger(__name__)

# System prompt for all optimization LLM calls
OPTIMIZATION_SYSTEM_PROMPT = (
    "You are a senior SQL performance engineer. "
    "Return ONLY valid JSON — no markdown fences, no commentary."
)


class AnthropicClient:
    """LLM client for Anthropic Claude API."""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        """Initialize Anthropic client.

        Args:
            api_key: Anthropic API key
            model: Model name (default: claude-sonnet-4-20250514)
        """
        self.api_key = api_key
        self.model = model
        logger.info("Initialized AnthropicClient with model=%s", model)

    def analyze(self, prompt: str) -> str:
        """Send prompt to Claude and return response."""
        try:
            import anthropic
        except ImportError:
            logger.error("anthropic package not installed")
            raise ImportError("anthropic package required: pip install anthropic")

        logger.debug("Sending request to Anthropic API (prompt=%d chars)", len(prompt))
        start_time = time.time()

        client = anthropic.Anthropic(api_key=self.api_key)

        message = client.messages.create(
            model=self.model,
            max_tokens=4096,
            system=OPTIMIZATION_SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        duration = time.time() - start_time
        response_text = message.content[0].text
        logger.info(
            "Anthropic API response: model=%s, duration=%.2fs, response=%d chars",
            self.model, duration, len(response_text)
        )

        return response_text
