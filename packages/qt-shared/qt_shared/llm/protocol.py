"""Protocol definition for LLM clients.

All LLM client implementations should implement this protocol.
"""

from typing import Protocol


class LLMClient(Protocol):
    """Protocol for LLM client implementations.

    Any LLM client (Anthropic, Gemini, Groq, etc.) should implement this protocol.
    """

    def analyze(self, prompt: str) -> str:
        """Send prompt to LLM and return response.

        Args:
            prompt: The prompt text to send to the LLM

        Returns:
            The LLM's response as a string
        """
        ...
