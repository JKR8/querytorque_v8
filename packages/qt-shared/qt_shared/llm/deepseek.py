"""DeepSeek LLM client."""

import logging
import time

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

    def analyze(self, prompt: str) -> str:
        """Send prompt to DeepSeek and return response."""
        try:
            from openai import OpenAI
        except ImportError:
            logger.error("openai package not installed")
            raise ImportError("openai package required: pip install openai")

        logger.debug("Sending request to DeepSeek API (prompt=%d chars)", len(prompt))
        start_time = time.time()

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
        response_text = response.choices[0].message.content or ""

        # Log reasoning content if available (R1 models)
        reasoning = getattr(response.choices[0].message, 'reasoning_content', None)
        if reasoning:
            logger.debug("DeepSeek reasoning: %s...", reasoning[:200])

        logger.info(
            "DeepSeek API response: model=%s, duration=%.2fs, response=%d chars",
            self.model, duration, len(response_text)
        )

        return response_text
