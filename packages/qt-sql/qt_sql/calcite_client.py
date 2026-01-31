"""HTTP client for qt-calcite service.

Provides graceful degradation when qt-calcite is unavailable.
"""

import logging
from typing import Optional
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

DEFAULT_CALCITE_URL = "http://localhost:8001"


@dataclass
class CalciteResult:
    """Result from Calcite optimization."""
    success: bool
    original_sql: str
    optimized_sql: Optional[str] = None
    query_changed: bool = False
    rules_applied: list[str] = None
    original_time_ms: Optional[int] = None
    optimized_time_ms: Optional[int] = None
    improvement_percent: Optional[float] = None
    validation_passed: Optional[bool] = None
    original_cost: Optional[float] = None
    optimized_cost: Optional[float] = None
    llm_reasoning: Optional[str] = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.rules_applied is None:
            self.rules_applied = []


class CalciteClient:
    """HTTP client for qt-calcite service.

    Features:
    - Graceful degradation when service is unavailable
    - Configurable timeouts
    - Health checks
    """

    def __init__(
        self,
        base_url: str = DEFAULT_CALCITE_URL,
        timeout: float = 300.0,
        enabled: bool = True,
    ):
        """Initialize Calcite client.

        Args:
            base_url: Base URL of the qt-calcite service
            timeout: Request timeout in seconds
            enabled: Whether to enable Calcite optimization
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.enabled = enabled
        self._available: Optional[bool] = None

    async def is_available(self) -> bool:
        """Check if the Calcite service is available.

        Caches the result after first check.
        """
        if not self.enabled:
            return False

        if self._available is not None:
            return self._available

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.base_url}/health")
                data = response.json()
                self._available = data.get("status") == "ok" and data.get("jar_found", False)
                logger.info("Calcite service available: %s", self._available)
                return self._available
        except Exception as e:
            logger.warning("Calcite service not available: %s", e)
            self._available = False
            return False

    def reset_availability(self):
        """Reset availability check (forces re-check on next call)."""
        self._available = None

    async def optimize(
        self,
        sql: str,
        connection_string: str = ":memory:",
        mode: str = "hep",
        compare: bool = True,
        dry_run: bool = False,
        deepseek_api_key: Optional[str] = None,
    ) -> CalciteResult:
        """Optimize a SQL query using Calcite.

        Args:
            sql: SQL query to optimize
            connection_string: Database connection string
            mode: Optimization mode ("hep" or "volcano")
            compare: Whether to compare performance
            dry_run: Whether to run in dry-run mode
            deepseek_api_key: Optional DeepSeek API key override

        Returns:
            CalciteResult with optimization details
        """
        if not await self.is_available():
            return CalciteResult(
                success=False,
                original_sql=sql,
                error="Calcite service not available"
            )

        payload = {
            "sql": sql,
            "connection_string": connection_string,
            "mode": mode,
            "compare": compare,
            "dry_run": dry_run,
            "timeout_seconds": int(self.timeout),
        }

        if deepseek_api_key:
            payload["deepseek_api_key"] = deepseek_api_key

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/optimize",
                    json=payload,
                )

                if response.status_code != 200:
                    return CalciteResult(
                        success=False,
                        original_sql=sql,
                        error=f"HTTP {response.status_code}: {response.text}"
                    )

                data = response.json()

                return CalciteResult(
                    success=data.get("success", False),
                    original_sql=data.get("original_sql", sql),
                    optimized_sql=data.get("optimized_sql"),
                    query_changed=data.get("query_changed", False),
                    rules_applied=data.get("rules_applied", []),
                    original_time_ms=data.get("original_time_ms"),
                    optimized_time_ms=data.get("optimized_time_ms"),
                    improvement_percent=data.get("improvement_percent"),
                    validation_passed=data.get("validation_passed"),
                    original_cost=data.get("original_cost"),
                    optimized_cost=data.get("optimized_cost"),
                    llm_reasoning=data.get("llm_reasoning"),
                    error=data.get("error"),
                )

        except httpx.TimeoutException:
            return CalciteResult(
                success=False,
                original_sql=sql,
                error=f"Request timeout ({self.timeout}s)"
            )
        except Exception as e:
            logger.error("Calcite optimization failed: %s", e)
            return CalciteResult(
                success=False,
                original_sql=sql,
                error=str(e)
            )

    async def get_rules(self) -> dict:
        """Get available Calcite optimization rules.

        Returns:
            Dict of rule categories and rules, or empty dict if unavailable
        """
        if not await self.is_available():
            return {}

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(f"{self.base_url}/rules")
                return response.json()
        except Exception as e:
            logger.warning("Failed to get Calcite rules: %s", e)
            return {}


# Singleton instance with default settings
_default_client: Optional[CalciteClient] = None


def get_calcite_client(
    base_url: Optional[str] = None,
    enabled: bool = True,
) -> CalciteClient:
    """Get or create the default Calcite client.

    Args:
        base_url: Optional custom base URL
        enabled: Whether to enable Calcite (default True)

    Returns:
        CalciteClient instance
    """
    global _default_client

    if base_url:
        return CalciteClient(base_url=base_url, enabled=enabled)

    if _default_client is None:
        import os
        url = os.environ.get("QTCALCITE_URL", DEFAULT_CALCITE_URL)
        _default_client = CalciteClient(base_url=url, enabled=enabled)

    return _default_client
