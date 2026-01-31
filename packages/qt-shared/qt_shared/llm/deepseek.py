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

        # DeepSeek reasoner doesn't support response_format or system messages well
        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "user", "content": prompt},
            ],
            max_tokens=4096,
        )

        duration = time.time() - start_time
        response_text = response.choices[0].message.content
        logger.info(
            "DeepSeek API response: model=%s, duration=%.2fs, response=%d chars",
            self.model, duration, len(response_text)
        )

        return response_text
