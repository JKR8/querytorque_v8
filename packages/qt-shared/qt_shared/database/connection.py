"""Database connection and session management for QueryTorque."""

import os
from typing import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker,
    AsyncEngine,
)
from sqlalchemy.pool import NullPool

from .models import Base


# Global engine instance
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_database_url() -> str:
    """Get database URL from environment.

    SECURITY: Database password must be set via QT_DB_PASSWORD environment variable.
    No default password is provided for security reasons.

    Raises:
        ValueError: If database is configured but password is not set.
    """
    # Check for explicit database URL
    db_url = os.environ.get("QT_DATABASE_URL")
    if db_url:
        # Convert postgres:// to postgresql+asyncpg:// for async
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return db_url

    # Build URL from components
    host = os.environ.get("QT_DB_HOST", "localhost")
    port = os.environ.get("QT_DB_PORT", "5432")
    name = os.environ.get("QT_DB_NAME", "querytorque")
    user = os.environ.get("QT_DB_USER", "querytorque")
    password = os.environ.get("QT_DB_PASSWORD", "")

    # Require password if database is explicitly configured
    if not password and host != "localhost":
        raise ValueError(
            "Database password required. Set QT_DB_PASSWORD environment variable."
        )

    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{name}"


def get_engine() -> AsyncEngine:
    """Get or create the database engine."""
    global _engine
    if _engine is None:
        database_url = get_database_url()

        # Use NullPool for serverless environments, connection pooling otherwise
        pool_class = NullPool if os.environ.get("QT_SERVERLESS") else None

        _engine = create_async_engine(
            database_url,
            echo=os.environ.get("QT_DB_ECHO", "").lower() == "true",
            poolclass=pool_class,
            pool_size=5 if pool_class is None else None,
            max_overflow=10 if pool_class is None else None,
            pool_timeout=30 if pool_class is None else None,
            pool_recycle=1800 if pool_class is None else None,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Get or create the session factory."""
    global _session_factory
    if _session_factory is None:
        engine = get_engine()
        _session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _session_factory


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database sessions."""
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def get_session_context() -> AsyncGenerator[AsyncSession, None]:
    """Context manager for database sessions (non-FastAPI use)."""
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db() -> None:
    """Initialize database tables (development only)."""
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_db() -> None:
    """Close database connections."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
