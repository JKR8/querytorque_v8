"""Database executor factory for creating database executors.

This module provides a unified interface for creating database executors
(DuckDB, PostgreSQL, Snowflake, etc.) with a consistent API.

Usage:
    # From DSN string
    executor = create_executor_from_dsn("postgres://user:pass@localhost:5432/db")

    # From config dict
    executor = create_executor(
        db_type="postgres",
        config={"host": "localhost", "port": 5432, "database": "test"}
    )

    # From environment variables
    executor = create_executor(db_type="postgres")  # Uses QT_POSTGRES_* env vars
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Protocol, Type
from urllib.parse import urlparse, unquote


# =============================================================================
# Protocol: Database Executor Interface
# =============================================================================

class DatabaseExecutor(Protocol):
    """Protocol defining the interface for database executors.

    All database executors must implement these methods to be compatible
    with the QueryTorque optimization and validation pipeline.
    """

    def connect(self) -> None:
        """Establish connection to the database."""
        ...

    def close(self) -> None:
        """Close the database connection."""
        ...

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        ...

    def execute_script(self, sql_script: str) -> None:
        """Execute multi-statement SQL script."""
        ...

    def explain(self, sql: str, analyze: bool = True) -> dict[str, Any]:
        """Get execution plan as dict."""
        ...

    def get_schema_info(self, include_row_counts: bool = True) -> dict[str, Any]:
        """Get schema information (tables, columns)."""
        ...

    def get_table_stats(self, table_name: str) -> dict[str, Any]:
        """Get table statistics (row counts, indexes)."""
        ...

    def __enter__(self) -> "DatabaseExecutor":
        ...

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        ...


# =============================================================================
# Database Configuration Classes
# =============================================================================

@dataclass
class DatabaseConfig(ABC):
    """Base class for database configuration.

    Subclasses define the specific connection parameters for each database type.
    """

    @classmethod
    @abstractmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create config from environment variables."""
        ...

    @classmethod
    @abstractmethod
    def from_dsn(cls, dsn: str) -> "DatabaseConfig":
        """Create config from a DSN connection string."""
        ...

    @abstractmethod
    def get_executor(self) -> DatabaseExecutor:
        """Create and return an executor for this configuration."""
        ...

    @property
    @abstractmethod
    def db_type(self) -> str:
        """Return the database type identifier."""
        ...


@dataclass
class DuckDBConfig(DatabaseConfig):
    """DuckDB connection configuration."""

    database: str = ":memory:"
    read_only: bool = False

    @property
    def db_type(self) -> str:
        return "duckdb"

    @classmethod
    def from_env(cls) -> "DuckDBConfig":
        """Create config from environment variables."""
        return cls(
            database=os.getenv("QT_DUCKDB_DATABASE", ":memory:"),
            read_only=os.getenv("QT_DUCKDB_READ_ONLY", "false").lower() == "true",
        )

    @classmethod
    def from_dsn(cls, dsn: str) -> "DuckDBConfig":
        """Create config from path or DSN.

        Supports:
        - Simple path: /path/to/db.duckdb
        - Memory: :memory:
        - DSN: duckdb:///path/to/db.duckdb
        """
        if dsn.startswith("duckdb://"):
            path = dsn.replace("duckdb://", "")
            return cls(database=path or ":memory:")
        return cls(database=dsn)

    def get_executor(self) -> DatabaseExecutor:
        from .duckdb_executor import DuckDBExecutor
        return DuckDBExecutor(
            database=self.database,
            read_only=self.read_only and self.database != ":memory:",
        )


@dataclass
class PostgresConfig(DatabaseConfig):
    """PostgreSQL connection configuration."""

    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = ""
    schema: str = "public"

    @property
    def db_type(self) -> str:
        return "postgres"

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        """Create config from environment variables (QT_POSTGRES_*)."""
        return cls(
            host=os.getenv("QT_POSTGRES_HOST", "localhost"),
            port=int(os.getenv("QT_POSTGRES_PORT", "5432")),
            database=os.getenv("QT_POSTGRES_DATABASE", "postgres"),
            user=os.getenv("QT_POSTGRES_USER", "postgres"),
            password=os.getenv("QT_POSTGRES_PASSWORD", ""),
            schema=os.getenv("QT_POSTGRES_SCHEMA", "public"),
        )

    @classmethod
    def from_dsn(cls, dsn: str) -> "PostgresConfig":
        """Parse a PostgreSQL connection string.

        Supports formats:
        - postgresql://user:pass@host:port/database
        - postgres://user:pass@host:port/database
        - host=localhost port=5432 dbname=test user=postgres (libpq style)
        - host:port:database:user:password (colon-separated)
        """
        if dsn.startswith("postgresql://") or dsn.startswith("postgres://"):
            parsed = urlparse(dsn)
            return cls(
                host=parsed.hostname or "localhost",
                port=parsed.port or 5432,
                database=parsed.path.lstrip("/") if parsed.path else "postgres",
                user=unquote(parsed.username or "postgres"),
                password=unquote(parsed.password or ""),
            )
        elif "=" in dsn:
            # libpq key=value format
            params = {}
            for part in dsn.split():
                if "=" in part:
                    key, value = part.split("=", 1)
                    params[key.strip()] = value.strip()
            return cls(
                host=params.get("host", "localhost"),
                port=int(params.get("port", "5432")),
                database=params.get("dbname", params.get("database", "postgres")),
                user=params.get("user", "postgres"),
                password=params.get("password", ""),
            )
        else:
            # Colon-separated: host:port:database:user:password
            parts = dsn.split(":")
            return cls(
                host=parts[0] if len(parts) > 0 else "localhost",
                port=int(parts[1]) if len(parts) > 1 else 5432,
                database=parts[2] if len(parts) > 2 else "postgres",
                user=parts[3] if len(parts) > 3 else "postgres",
                password=parts[4] if len(parts) > 4 else "",
            )

    def get_executor(self) -> DatabaseExecutor:
        from .postgres_executor import PostgresExecutor
        return PostgresExecutor(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            schema=self.schema,
        )


@dataclass
class SnowflakeConfig(DatabaseConfig):
    """Snowflake connection configuration (placeholder for future implementation)."""

    account: str = ""
    user: str = ""
    password: str = ""
    warehouse: str = ""
    database: str = ""
    schema: str = "public"
    role: str = ""

    @property
    def db_type(self) -> str:
        return "snowflake"

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """Create config from environment variables (QT_SNOWFLAKE_*)."""
        return cls(
            account=os.getenv("QT_SNOWFLAKE_ACCOUNT", ""),
            user=os.getenv("QT_SNOWFLAKE_USER", ""),
            password=os.getenv("QT_SNOWFLAKE_PASSWORD", ""),
            warehouse=os.getenv("QT_SNOWFLAKE_WAREHOUSE", ""),
            database=os.getenv("QT_SNOWFLAKE_DATABASE", ""),
            schema=os.getenv("QT_SNOWFLAKE_SCHEMA", "public"),
            role=os.getenv("QT_SNOWFLAKE_ROLE", ""),
        )

    @classmethod
    def from_dsn(cls, dsn: str) -> "SnowflakeConfig":
        """Parse a Snowflake connection string.

        Supports:
        - snowflake://user:pass@account/database/schema?warehouse=wh&role=role
        """
        if dsn.startswith("snowflake://"):
            parsed = urlparse(dsn)
            # Parse query parameters
            params = {}
            if parsed.query:
                for part in parsed.query.split("&"):
                    if "=" in part:
                        k, v = part.split("=", 1)
                        params[k] = v

            path_parts = parsed.path.lstrip("/").split("/")
            return cls(
                account=parsed.hostname or "",
                user=unquote(parsed.username or ""),
                password=unquote(parsed.password or ""),
                database=path_parts[0] if len(path_parts) > 0 else "",
                schema=path_parts[1] if len(path_parts) > 1 else "public",
                warehouse=params.get("warehouse", ""),
                role=params.get("role", ""),
            )
        raise ValueError(f"Invalid Snowflake DSN: {dsn}")

    def get_executor(self) -> DatabaseExecutor:
        from .snowflake_executor import SnowflakeExecutor
        return SnowflakeExecutor(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role,
        )


@dataclass
class DatabricksConfig(DatabaseConfig):
    """Databricks SQL warehouse connection configuration."""

    server_hostname: str = ""
    http_path: str = ""
    access_token: str = ""
    catalog: str = "hive_metastore"
    schema: str = "default"

    @property
    def db_type(self) -> str:
        return "databricks"

    @classmethod
    def from_env(cls) -> "DatabricksConfig":
        """Create config from environment variables (QT_DATABRICKS_*)."""
        return cls(
            server_hostname=os.getenv("QT_DATABRICKS_SERVER_HOSTNAME", ""),
            http_path=os.getenv("QT_DATABRICKS_HTTP_PATH", ""),
            access_token=os.getenv("QT_DATABRICKS_ACCESS_TOKEN", ""),
            catalog=os.getenv("QT_DATABRICKS_CATALOG", "hive_metastore"),
            schema=os.getenv("QT_DATABRICKS_SCHEMA", "default"),
        )

    @classmethod
    def from_dsn(cls, dsn: str) -> "DatabricksConfig":
        """Parse a Databricks connection string.

        Supports:
        - databricks://token:TOKEN@HOST/HTTP_PATH?catalog=X&schema=Y
        """
        if dsn.startswith("databricks://"):
            parsed = urlparse(dsn)
            # Query parameters
            params = {}
            if parsed.query:
                for part in parsed.query.split("&"):
                    if "=" in part:
                        k, v = part.split("=", 1)
                        params[k] = unquote(v)

            return cls(
                server_hostname=parsed.hostname or "",
                http_path=unquote(parsed.path.lstrip("/")) if parsed.path else "",
                access_token=unquote(parsed.password or ""),
                catalog=params.get("catalog", "hive_metastore"),
                schema=params.get("schema", "default"),
            )
        raise ValueError(f"Invalid Databricks DSN: {dsn}")

    def get_executor(self) -> DatabaseExecutor:
        from .databricks_executor import DatabricksExecutor
        return DatabricksExecutor(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
            catalog=self.catalog,
            schema=self.schema,
        )


# =============================================================================
# Registry: Database Type to Config Class Mapping
# =============================================================================

_CONFIG_REGISTRY: Dict[str, Type[DatabaseConfig]] = {
    "duckdb": DuckDBConfig,
    "postgres": PostgresConfig,
    "postgresql": PostgresConfig,
    "pg": PostgresConfig,
    "snowflake": SnowflakeConfig,
    "sf": SnowflakeConfig,
    "databricks": DatabricksConfig,
    "dbx": DatabricksConfig,
}


def register_config(db_type: str, config_class: Type[DatabaseConfig]) -> None:
    """Register a new database config type.

    Use this to add support for additional databases.

    Args:
        db_type: Database type identifier (e.g., "mysql", "bigquery")
        config_class: DatabaseConfig subclass for this database type
    """
    _CONFIG_REGISTRY[db_type.lower()] = config_class


def get_config_class(db_type: str) -> Type[DatabaseConfig]:
    """Get the config class for a database type.

    Args:
        db_type: Database type identifier

    Returns:
        DatabaseConfig subclass

    Raises:
        ValueError: If database type is not registered
    """
    config_class = _CONFIG_REGISTRY.get(db_type.lower())
    if config_class is None:
        supported = ", ".join(sorted(set(_CONFIG_REGISTRY.keys())))
        raise ValueError(f"Unknown database type: {db_type}. Supported: {supported}")
    return config_class


# =============================================================================
# Factory Functions
# =============================================================================

def create_executor(
    db_type: str = "duckdb",
    config: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> DatabaseExecutor:
    """Create a database executor.

    Args:
        db_type: Database type ("duckdb", "postgres", "snowflake", etc.)
        config: Configuration dict (keys depend on db_type)
        **kwargs: Additional config parameters (merged with config dict)

    Returns:
        Database executor instance

    Examples:
        # DuckDB
        executor = create_executor("duckdb", database="/path/to/db.duckdb")

        # PostgreSQL
        executor = create_executor("postgres", config={
            "host": "localhost",
            "port": 5432,
            "database": "mydb",
            "user": "user",
            "password": "pass"
        })

        # From environment
        executor = create_executor("postgres")  # Uses QT_POSTGRES_* env vars
    """
    config_class = get_config_class(db_type)

    # Merge config dict with kwargs
    merged_config = {**(config or {}), **kwargs}

    if merged_config:
        # Create config from provided parameters
        db_config = config_class(**merged_config)
    else:
        # Use environment variables
        db_config = config_class.from_env()

    return db_config.get_executor()


def create_executor_from_dsn(dsn: str) -> DatabaseExecutor:
    """Create an executor from a DSN connection string.

    The database type is inferred from the DSN scheme.

    Args:
        dsn: Database connection string

    Returns:
        Database executor instance

    Examples:
        # PostgreSQL
        executor = create_executor_from_dsn("postgres://user:pass@localhost:5432/db")

        # DuckDB
        executor = create_executor_from_dsn("duckdb:///path/to/db.duckdb")
        executor = create_executor_from_dsn("/path/to/db.duckdb")

        # Snowflake
        executor = create_executor_from_dsn("snowflake://user:pass@account/db/schema")
    """
    # Infer database type from DSN
    dsn_lower = dsn.lower()

    if dsn_lower.startswith("postgres://") or dsn_lower.startswith("postgresql://"):
        return PostgresConfig.from_dsn(dsn).get_executor()
    elif dsn_lower.startswith("snowflake://"):
        return SnowflakeConfig.from_dsn(dsn).get_executor()
    elif dsn_lower.startswith("databricks://"):
        return DatabricksConfig.from_dsn(dsn).get_executor()
    elif dsn_lower.startswith("duckdb://"):
        return DuckDBConfig.from_dsn(dsn).get_executor()
    elif dsn_lower == ":memory:" or dsn_lower.endswith(".duckdb") or dsn_lower.endswith(".db"):
        # Assume DuckDB for file paths with explicit DuckDB extensions
        return DuckDBConfig(database=dsn).get_executor()
    else:
        raise ValueError(
            f"Unrecognized database DSN scheme: {dsn!r}. "
            f"Supported prefixes: postgres://, postgresql://, snowflake://, databricks://, duckdb://, "
            f":memory:, *.duckdb, *.db. "
            f"Will NOT silently fall back to another engine."
        )


def create_executor_from_cli_args(
    db_type: str = "duckdb",
    # DuckDB options
    duckdb_path: Optional[str] = None,
    read_only: bool = True,
    # PostgreSQL options
    pg_dsn: Optional[str] = None,
    pg_host: Optional[str] = None,
    pg_port: Optional[int] = None,
    pg_database: Optional[str] = None,
    pg_user: Optional[str] = None,
    pg_password: Optional[str] = None,
    # Snowflake options
    sf_account: Optional[str] = None,
    sf_user: Optional[str] = None,
    sf_password: Optional[str] = None,
    sf_warehouse: Optional[str] = None,
    sf_database: Optional[str] = None,
    # Databricks options
    dbx_server_hostname: Optional[str] = None,
    dbx_http_path: Optional[str] = None,
    dbx_access_token: Optional[str] = None,
    dbx_catalog: Optional[str] = None,
    dbx_schema: Optional[str] = None,
) -> DatabaseExecutor:
    """Create executor from CLI-style arguments.

    This is a convenience function for CLI integration that accepts
    all possible database parameters and uses the appropriate ones
    based on db_type.

    Args:
        db_type: Database type
        duckdb_path: DuckDB database path
        read_only: DuckDB read-only mode
        pg_dsn: PostgreSQL DSN (takes precedence over individual params)
        pg_host, pg_port, pg_database, pg_user, pg_password: PG params
        sf_*: Snowflake params
        dbx_*: Databricks params

    Returns:
        Database executor instance
    """
    db_type_lower = db_type.lower()

    if db_type_lower in ("postgres", "postgresql", "pg"):
        if pg_dsn:
            return PostgresConfig.from_dsn(pg_dsn).get_executor()
        else:
            config = PostgresConfig(
                host=pg_host or os.getenv("QT_POSTGRES_HOST", "localhost"),
                port=pg_port or int(os.getenv("QT_POSTGRES_PORT", "5432")),
                database=pg_database or os.getenv("QT_POSTGRES_DATABASE", "postgres"),
                user=pg_user or os.getenv("QT_POSTGRES_USER", "postgres"),
                password=pg_password or os.getenv("QT_POSTGRES_PASSWORD", ""),
            )
            return config.get_executor()

    elif db_type_lower in ("snowflake", "sf"):
        config = SnowflakeConfig(
            account=sf_account or os.getenv("QT_SNOWFLAKE_ACCOUNT", ""),
            user=sf_user or os.getenv("QT_SNOWFLAKE_USER", ""),
            password=sf_password or os.getenv("QT_SNOWFLAKE_PASSWORD", ""),
            warehouse=sf_warehouse or os.getenv("QT_SNOWFLAKE_WAREHOUSE", ""),
            database=sf_database or os.getenv("QT_SNOWFLAKE_DATABASE", ""),
        )
        return config.get_executor()

    elif db_type_lower in ("databricks", "dbx"):
        config = DatabricksConfig(
            server_hostname=dbx_server_hostname or os.getenv("QT_DATABRICKS_SERVER_HOSTNAME", ""),
            http_path=dbx_http_path or os.getenv("QT_DATABRICKS_HTTP_PATH", ""),
            access_token=dbx_access_token or os.getenv("QT_DATABRICKS_ACCESS_TOKEN", ""),
            catalog=dbx_catalog or os.getenv("QT_DATABRICKS_CATALOG", "hive_metastore"),
            schema=dbx_schema or os.getenv("QT_DATABRICKS_SCHEMA", "default"),
        )
        return config.get_executor()

    else:
        # Default to DuckDB
        config = DuckDBConfig(
            database=duckdb_path or os.getenv("QT_DUCKDB_DATABASE", ":memory:"),
            read_only=read_only,
        )
        return config.get_executor()


# Alias for backwards compatibility
create_executor_from_args = create_executor_from_cli_args
