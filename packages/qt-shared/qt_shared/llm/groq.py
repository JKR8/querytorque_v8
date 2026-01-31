"""Groq LLM client."""

import logging
import time

logger = logging.getLogger(__name__)

# System prompt for optimization calls
OPTIMIZATION_SYSTEM_PROMPT = (
    "You are a senior SQL performance engineer. "
    "Return ONLY valid JSON — no markdown fences, no commentary."
)


class GroqClient:
    """LLM client for Groq API.

    Groq provides fast inference for open models like Llama, Mixtral, etc.
    Cost-effective option for high-throughput workloads.
    """

    def __init__(
        self,
        api_key: str,
        model: str = "llama-3.3-70b-versatile",
        temperature: float = 0.7,
        max_tokens: int = 8192,
        reasoning_effort: str = "high",
    ):
        """Initialize Groq client.

        Args:
            api_key: Groq API key
            model: Model name. Options include:
                - llama-3.3-70b-versatile
                - llama-3.1-70b-versatile
            temperature: Sampling temperature (0.0-1.0)
            max_tokens: Maximum response tokens
            reasoning_effort: Reasoning level for supported models
        """
        self.api_key = api_key
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.reasoning_effort = reasoning_effort
        logger.info(
            "Initialized GroqClient: model=%s, temp=%.1f, max_tokens=%d",
            model, temperature, max_tokens
        )

    def analyze(self, prompt: str) -> str:
        """Send prompt to Groq and return response."""
        try:
            from groq import Groq
        except ImportError:
            logger.error("groq package not installed")
            raise ImportError("groq package required: pip install groq")

        logger.debug("Sending request to Groq API (prompt=%d chars)", len(prompt))
        start_time = time.time()

        client = Groq(api_key=self.api_key)

        request_params = {
            "messages": [
                {"role": "system", "content": OPTIMIZATION_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            "model": self.model,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
        }

        # Add reasoning_effort for models that support it
        if self.reasoning_effort and "deepseek" in self.model.lower():
            request_params["reasoning_format"] = "parsed"
            request_params["reasoning_effort"] = self.reasoning_effort

        # Enable JSON mode on supported models
        if "reasoning_format" not in request_params:
            request_params["response_format"] = {"type": "json_object"}

        chat_completion = client.chat.completions.create(**request_params)

        duration = time.time() - start_time
        response_text = chat_completion.choices[0].message.content
        logger.info(
            "Groq API response: model=%s, duration=%.2fs, response=%d chars",
            self.model, duration, len(response_text)
        )

        return response_text
