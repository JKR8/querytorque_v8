"""SQLAlchemy models for QueryTorque shared database.

These models are used by the QueryTorque SQL optimization platform.
"""

import uuid
from datetime import datetime
from typing import Optional, List

from sqlalchemy import (
    String,
    Text,
    Integer,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    JSON,
    TypeDecorator,
    CHAR,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class GUID(TypeDecorator):
    """Platform-independent GUID type.

    Uses PostgreSQL's UUID type when available, otherwise uses CHAR(36).
    """
    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID())
        else:
            return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == "postgresql":
            return value
        else:
            return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        elif isinstance(value, uuid.UUID):
            return value
        else:
            return uuid.UUID(value)


class FlexibleJSON(TypeDecorator):
    """JSON type that uses JSONB on PostgreSQL, JSON elsewhere."""
    impl = JSON
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB)
        else:
            return dialect.type_descriptor(JSON)


# Alias for consistency
UUID = GUID


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class Organization(Base):
    """Organization (tenant) model."""

    __tablename__ = "organizations"

    id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        primary_key=True,
        default=uuid.uuid4,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    tier: Mapped[str] = mapped_column(String(50), default="free")
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    settings: Mapped[Optional[dict]] = mapped_column(FlexibleJSON, nullable=True, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relationships
    users: Mapped[List["User"]] = relationship("User", back_populates="organization")
    workspaces: Mapped[List["Workspace"]] = relationship("Workspace", back_populates="organization")
    subscription: Mapped[Optional["Subscription"]] = relationship(
        "Subscription", back_populates="organization", uselist=False
    )
    api_usage: Mapped[List["APIUsage"]] = relationship("APIUsage", back_populates="organization")

    def __repr__(self) -> str:
        return f"<Organization(id={self.id}, name='{self.name}', tier='{self.tier}')>"


class User(Base):
    """User model."""

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        primary_key=True,
        default=uuid.uuid4,
    )
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    org_id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(String(50), default="member")
    auth0_id: Mapped[Optional[str]] = mapped_column(String(255), unique=True, nullable=True)
    api_key_hash: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    api_key_prefix: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_login: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relationships
    organization: Mapped["Organization"] = relationship("Organization", back_populates="users")
    analysis_jobs: Mapped[List["AnalysisJob"]] = relationship("AnalysisJob", back_populates="user")
    api_usage: Mapped[List["APIUsage"]] = relationship("APIUsage", back_populates="user")

    __table_args__ = (
        Index("idx_users_org", "org_id"),
        Index("idx_users_email", "email"),
        Index("idx_users_api_key_prefix", "api_key_prefix"),
    )

    def __repr__(self) -> str:
        return f"<User(id={self.id}, email='{self.email}', role='{self.role}')>"


class Workspace(Base):
    """Workspace model for organizing analysis jobs."""

    __tablename__ = "workspaces"

    id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        primary_key=True,
        default=uuid.uuid4,
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    settings: Mapped[Optional[dict]] = mapped_column(FlexibleJSON, nullable=True, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relationships
    organization: Mapped["Organization"] = relationship("Organization", back_populates="workspaces")
    analysis_jobs: Mapped[List["AnalysisJob"]] = relationship("AnalysisJob", back_populates="workspace")

    __table_args__ = (
        Index("idx_workspaces_org", "org_id"),
    )

    def __repr__(self) -> str:
        return f"<Workspace(id={self.id}, name='{self.name}')>"


class AnalysisJob(Base):
    """Analysis job model for SQL optimization."""

    __tablename__ = "analysis_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        primary_key=True,
        default=uuid.uuid4,
    )
    workspace_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        GUID(),
        ForeignKey("workspaces.id", ondelete="SET NULL"),
        nullable=True,
    )
    user_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        GUID(),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    file_type: Mapped[str] = mapped_column(String(20), nullable=False, default="sql")
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    file_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    torque_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    quality_gate: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    progress: Mapped[int] = mapped_column(Integer, default=0)
    result_json: Mapped[Optional[dict]] = mapped_column(FlexibleJSON, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    processing_time_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationships
    workspace: Mapped[Optional["Workspace"]] = relationship("Workspace", back_populates="analysis_jobs")
    user: Mapped[Optional["User"]] = relationship("User", back_populates="analysis_jobs")

    __table_args__ = (
        Index("idx_jobs_workspace", "workspace_id"),
        Index("idx_jobs_user", "user_id"),
        Index("idx_jobs_status", "status"),
        Index("idx_jobs_created", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<AnalysisJob(id={self.id}, file='{self.file_name}', status='{self.status}')>"


class Subscription(Base):
    """Subscription model for billing."""

    __tablename__ = "subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        primary_key=True,
        default=uuid.uuid4,
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    stripe_price_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    tier: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="active")
    current_period_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    current_period_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_at_period_end: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relationships
    organization: Mapped["Organization"] = relationship("Organization", back_populates="subscription")

    __table_args__ = (
        Index("idx_subscriptions_org", "org_id"),
        Index("idx_subscriptions_stripe", "stripe_subscription_id"),
    )

    def __repr__(self) -> str:
        return f"<Subscription(id={self.id}, tier='{self.tier}', status='{self.status}')>"


class APIUsage(Base):
    """API usage tracking for rate limiting and billing."""

    __tablename__ = "api_usage"

    id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        primary_key=True,
        default=uuid.uuid4,
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        GUID(),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    endpoint: Mapped[str] = mapped_column(String(255), nullable=False)
    method: Mapped[str] = mapped_column(String(10), nullable=False)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    response_time_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    request_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    response_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    # Relationships
    organization: Mapped["Organization"] = relationship("Organization", back_populates="api_usage")
    user: Mapped[Optional["User"]] = relationship("User", back_populates="api_usage")

    __table_args__ = (
        Index("idx_usage_org_date", "org_id", "created_at"),
        Index("idx_usage_user_date", "user_id", "created_at"),
        Index("idx_usage_endpoint", "endpoint", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<APIUsage(id={self.id}, endpoint='{self.endpoint}')>"
