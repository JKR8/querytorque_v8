"""Protocol definition for LLM clients.

All LLM client implementations should implement this protocol.
"""

from typing import Dict, Protocol


class LLMClient(Protocol):
    """Protocol for LLM client implementations.

    Any LLM client (DeepSeek, Gemini, Groq, etc.) should implement this protocol.

    Attributes:
        last_usage: Dict with token usage and cache metrics from the most recent
            API call. Keys vary by provider but may include:
            - prompt_tokens, completion_tokens, total_tokens
            - prompt_cache_hit_tokens, prompt_cache_miss_tokens (DeepSeek/OpenRouter)
            - cached_tokens (OpenAI native)
    """

    last_usage: Dict[str, int]

    def analyze(self, prompt: str) -> str:
        """Send prompt to LLM and return response.

        Args:
            prompt: The prompt text to send to the LLM

        Returns:
            The LLM's response as a string
        """
        ...
