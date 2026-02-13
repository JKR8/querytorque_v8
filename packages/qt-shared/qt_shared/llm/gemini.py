"""Google Gemini LLM clients (API and CLI)."""

import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

# System prompt for optimization calls
OPTIMIZATION_SYSTEM_PROMPT = (
    "You are a senior SQL performance engineer. "
    "Return ONLY valid JSON — no markdown fences, no commentary."
)


class GeminiAPIClient:
    """LLM client for Google Gemini API.

    Supports various Gemini models including:
    - gemini-3-flash-preview (recommended for free tier)
    - gemini-2.5-flash
    - gemini-2.5-pro
    """

    def __init__(self, api_key: str, model: str = "gemini-3-flash-preview"):
        """Initialize Gemini API client.

        Args:
            api_key: Google AI API key
            model: Model name
        """
        self.api_key = api_key
        self.model = model
        self.last_usage: dict = {}
        logger.info("Initialized GeminiAPIClient with model=%s", model)

    def analyze(self, prompt: str) -> str:
        """Send prompt to Gemini API and return response."""
        try:
            import google.generativeai as genai
        except ImportError:
            logger.error("google-generativeai package not installed")
            raise ImportError("google-generativeai package required: pip install google-generativeai")

        logger.debug("Sending request to Gemini API (prompt=%d chars)", len(prompt))
        start_time = time.time()

        genai.configure(api_key=self.api_key)

        generation_config = genai.types.GenerationConfig(
            max_output_tokens=8192,
            temperature=0.1,
        )

        model = genai.GenerativeModel(
            self.model,
            generation_config=generation_config,
            system_instruction=OPTIMIZATION_SYSTEM_PROMPT,
        )

        response = model.generate_content(prompt)

        duration = time.time() - start_time
        response_text = response.text
        logger.info(
            "Gemini API response: model=%s, duration=%.2fs, response=%d chars",
            self.model, duration, len(response_text)
        )

        return response_text


class GeminiCLIClient:
    """LLM client that uses the Gemini CLI (OAuth-based).

    This client calls the locally installed `gemini` CLI tool via subprocess.
    Useful when you have Gemini CLI configured with OAuth.

    Requires: npm install -g @google/gemini-cli
    """

    def __init__(self, model: Optional[str] = None, timeout_seconds: int = 120):
        """Initialize Gemini CLI client.

        Args:
            model: Optional model name (e.g., 'gemini-2.5-pro').
                   If None, uses CLI default.
            timeout_seconds: Timeout for CLI calls
        """
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.last_usage: dict = {}
        logger.info("Initialized GeminiCLIClient with model=%s", model or "default")

    def analyze(self, prompt: str) -> str:
        """Send prompt to Gemini CLI and return response."""
        import subprocess

        logger.debug("Sending request to Gemini CLI (prompt=%d chars)", len(prompt))
        start_time = time.time()

        cmd = ["gemini"]
        if self.model:
            cmd.extend(["-m", self.model])

        try:
            full_input = "SYSTEM: " + OPTIMIZATION_SYSTEM_PROMPT + "\n\n" + prompt

            result = subprocess.run(
                cmd,
                input=full_input,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )

            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"
                logger.error("Gemini CLI error: %s", error_msg[:500])
                raise RuntimeError(f"Gemini CLI failed: {error_msg[:200]}")

            # Filter out credential loading messages
            response_lines = []
            for line in result.stdout.split('\n'):
                if not line.startswith("Loaded cached credentials"):
                    response_lines.append(line)
            response_text = '\n'.join(response_lines).strip()

            duration = time.time() - start_time
            logger.info(
                "Gemini CLI response: model=%s, duration=%.2fs, response=%d chars",
                self.model or "default", duration, len(response_text)
            )

            return response_text

        except subprocess.TimeoutExpired:
            logger.error("Gemini CLI timed out after %ds", self.timeout_seconds)
            raise TimeoutError(f"Gemini CLI timed out after {self.timeout_seconds}s")
        except FileNotFoundError:
            logger.error("Gemini CLI not found")
            raise RuntimeError("Gemini CLI not found. Install with: npm install -g @google/gemini-cli")


# Alias for backward compatibility
GeminiClient = GeminiAPIClient
