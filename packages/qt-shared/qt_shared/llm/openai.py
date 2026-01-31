"""OpenAI LLM client."""

import logging
import time

logger = logging.getLogger(__name__)

# System prompt for optimization calls
OPTIMIZATION_SYSTEM_PROMPT = (
    "You are a senior SQL performance engineer. "
    "Return ONLY valid JSON — no markdown fences, no commentary."
)


class OpenAIClient:
    """LLM client for OpenAI API.

    Supports GPT-4o and other OpenAI models.
    """

    def __init__(self, api_key: str, model: str = "gpt-4o"):
        """Initialize OpenAI client.

        Args:
            api_key: OpenAI API key
            model: Model name. Options include:
                - gpt-4o (recommended)
                - gpt-4o-mini
                - gpt-4-turbo
        """
        self.api_key = api_key
        self.model = model
        logger.info("Initialized OpenAIClient with model=%s", model)

    def analyze(self, prompt: str) -> str:
        """Send prompt to OpenAI and return response."""
        try:
            from openai import OpenAI
        except ImportError:
            logger.error("openai package not installed")
            raise ImportError("openai package required: pip install openai")

        logger.debug("Sending request to OpenAI API (prompt=%d chars)", len(prompt))
        start_time = time.time()

        client = OpenAI(api_key=self.api_key)

        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": OPTIMIZATION_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            max_tokens=4096,
            temperature=0.0,
            response_format={"type": "json_object"},
        )

        duration = time.time() - start_time
        response_text = response.choices[0].message.content
        logger.info(
            "OpenAI API response: model=%s, duration=%.2fs, response=%d chars",
            self.model, duration, len(response_text)
        )

        return response_text
