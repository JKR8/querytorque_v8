"""Factory function for creating LLM clients."""

from typing import Optional

from ..config import get_settings
from .protocol import LLMClient
from .deepseek import DeepSeekClient
from .gemini import GeminiAPIClient, GeminiCLIClient
from .groq import GroqClient
from .openai import OpenAIClient


def create_llm_client(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    enable_reasoning: bool = None,
) -> Optional[LLMClient]:
    """Create an LLM client based on configuration.

    If no arguments are provided, uses settings from environment.

    Args:
        provider: LLM provider name
            (deepseek, gemini-api, gemini-cli, groq, openai, openrouter)
        model: Model name (required; no implicit provider defaults)
        api_key: API key (optional, uses environment if not specified)
        enable_reasoning: Explicit reasoning mode control (forwarded to OpenAIClient).
            None = auto-detect, True = force on, False = force off.

    Returns:
        An LLM client instance, or None if no provider is configured.
    """
    settings = get_settings()

    # Use provided values or fall back to settings
    if provider is None:
        provider = settings.llm_provider
    if model is None:
        model = settings.llm_model

    if not provider:
        return None
    if not (model or "").strip():
        raise ValueError(
            "LLM model is required. Set QT_LLM_MODEL or pass model explicitly."
        )

    # Get API key from settings if not provided
    if api_key is None:
        if provider == "deepseek":
            api_key = settings.deepseek_api_key
        elif provider in ("gemini-api", "gemini"):
            api_key = settings.gemini_api_key
        elif provider == "groq":
            api_key = settings.groq_api_key
        elif provider == "openai":
            api_key = settings.openai_api_key
        elif provider == "openrouter":
            api_key = settings.openrouter_api_key

    # Create client based on provider
    if provider == "deepseek":
        if not api_key:
            raise ValueError("DeepSeek API key required")
        return DeepSeekClient(
            api_key=api_key,
            model=model,
        )
    elif provider in ("gemini-api", "gemini"):
        if not api_key:
            raise ValueError("Gemini API key required")
        return GeminiAPIClient(
            api_key=api_key,
            model=model,
        )
    elif provider == "gemini-cli":
        return GeminiCLIClient(model=model)
    elif provider == "groq":
        if not api_key:
            raise ValueError("Groq API key required")
        return GroqClient(
            api_key=api_key,
            model=model,
        )
    elif provider == "openai":
        if not api_key:
            raise ValueError("OpenAI API key required")
        return OpenAIClient(
            api_key=api_key,
            model=model,
            enable_reasoning=enable_reasoning,
        )
    elif provider == "openrouter":
        if not api_key:
            raise ValueError("OpenRouter API key required")
        return OpenAIClient(
            api_key=api_key,
            model=model,
            base_url="https://openrouter.ai/api/v1",
            enable_reasoning=enable_reasoning,
        )
    else:
        raise ValueError(f"Unknown LLM provider: {provider}")
