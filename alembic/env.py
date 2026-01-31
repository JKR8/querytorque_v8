"""Alembic environment configuration for QueryTorque.

This module configures Alembic to work with the qt_shared database models,
supporting both synchronous (offline) and asynchronous (online) migrations.
"""

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# Import all models from qt_shared to ensure they're registered with Base
from qt_shared.database import Base
from qt_shared.database.models import (
    Organization,
    User,
    Workspace,
    AnalysisJob,
    Subscription,
    APIUsage,
)

# Alembic Config object for access to .ini file values
config = context.config

# Configure Python logging from alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata for autogenerate support
target_metadata = Base.metadata


def get_database_url() -> str:
    """Get database URL from environment or config.

    Priority:
    1. QT_DATABASE_URL environment variable
    2. Build from QT_DB_* components
    3. Fall back to alembic.ini sqlalchemy.url
    """
    # Check for explicit database URL
    db_url = os.environ.get("QT_DATABASE_URL")
    if db_url:
        # Ensure we use the sync driver for Alembic
        if "+asyncpg" in db_url:
            db_url = db_url.replace("+asyncpg", "")
        return db_url

    # Build URL from components
    host = os.environ.get("QT_DB_HOST", "localhost")
    port = os.environ.get("QT_DB_PORT", "5432")
    name = os.environ.get("QT_DB_NAME", "querytorque")
    user = os.environ.get("QT_DB_USER", "querytorque")
    password = os.environ.get("QT_DB_PASSWORD", "querytorque_dev")

    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


def get_async_database_url() -> str:
    """Get async database URL for online migrations."""
    url = get_database_url()
    # Convert to async driver
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This generates SQL scripts without requiring a database connection.
    Useful for reviewing migrations before applying them.

    Usage:
        alembic upgrade head --sql > migration.sql
    """
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Execute migrations with an active connection."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
        # Include object names in autogenerate
        include_name=lambda name, type_, parent_names: True,
        # Render SQLAlchemy column types
        render_as_batch=False,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Run migrations asynchronously for online mode."""
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_async_database_url()

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    Creates an async engine and runs migrations within a transaction.
    """
    asyncio.run(run_async_migrations())


# Determine which mode to run based on context
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
