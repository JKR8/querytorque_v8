"""LLM client implementations for QueryTorque.

Provides a unified interface for multiple LLM providers:
- DeepSeek
- Gemini (API and CLI)
- Groq
- OpenAI
"""

from .protocol import LLMClient
from .deepseek import DeepSeekClient
from .gemini import GeminiAPIClient, GeminiCLIClient
from .groq import GroqClient
from .openai import OpenAIClient
from .factory import create_llm_client

# Alias for backward compatibility
GeminiClient = GeminiAPIClient

__all__ = [
    # Protocol
    "LLMClient",
    # Clients
    "DeepSeekClient",
    "GeminiAPIClient",
    "GeminiCLIClient",
    "GeminiClient",
    "GroqClient",
    "OpenAIClient",
    # Factory
    "create_llm_client",
]
