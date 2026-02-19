"""Initial schema — all QueryTorque SaaS models.

Revision ID: 001
Revises: None
Create Date: 2026-02-19 09:00:00+00:00

Tables:
  organizations, users, workspaces, analysis_jobs, subscriptions,
  api_usage, credentials, llm_usage, fleet_surveys, github_installations
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create all tables."""

    # --- organizations ---
    op.create_table(
        "organizations",
        sa.Column("id", postgresql.UUID(), nullable=False, default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("tier", sa.String(50), server_default="free", nullable=False),
        sa.Column("stripe_customer_id", sa.String(255), nullable=True),
        sa.Column("settings", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    # --- users ---
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(), nullable=False, default=sa.text("gen_random_uuid()")),
        sa.Column("email", sa.String(255), nullable=False),
        sa.Column("org_id", postgresql.UUID(), nullable=False),
        sa.Column("role", sa.String(50), server_default="member", nullable=False),
        sa.Column("auth0_id", sa.String(255), nullable=True),
        sa.Column("api_key_hash", sa.String(255), nullable=True),
        sa.Column("api_key_prefix", sa.String(16), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("last_login", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
        sa.UniqueConstraint("auth0_id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
    )
    op.create_index("idx_users_org", "users", ["org_id"])
    op.create_index("idx_users_email", "users", ["email"])
    op.create_index("idx_users_api_key_prefix", "users", ["api_key_prefix"])

    # --- workspaces ---
    op.create_table(
        "workspaces",
        sa.Column("id", postgresql.UUID(), nullable=False, default=sa.text("gen_random_uuid()")),
        sa.Column("org_id", postgresql.UUID(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("settings", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
    )
    op.create_index("idx_workspaces_org", "workspaces", ["org_id"])

    # --- credentials ---
    op.create_table(
        "credentials",
        sa.Column("id", postgresql.UUID(), nullable=False, default=sa.text("gen_random_uuid()")),
        sa.Column("org_id", postgresql.UUID(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("engine", sa.String(50), nullable=False),
        sa.Column("encrypted_dsn", sa.LargeBinary(), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("last_tested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
    )
    op.create_index("idx_credentials_org", "credentials", ["org_id"])

    # --- subscriptions ---
    op.create_table(
        "subscriptions",
        sa.Column("id", postgresql.UUID(), nullable=False, default=sa.text("gen_random_uuid()")),
        sa.Column("org_id", postgresql.UUID(), nullable=False),
        sa.Column("stripe_subscription_id", sa.String(255), nullable=True),
        sa.Column("stripe_price_id", sa.String(255), nullable=True),
        sa.Column("tier", sa.String(50), nullable=False),
        sa.Column("status", sa.String(50), server_default="active", nullable=False),
        sa.Column("current_period_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("current_period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_at_period_end", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
    )
    op.create_index("idx_subscriptions_org", "subscriptions", ["org_id"])
    op.create_index("idx_subscriptions_stripe", "subscriptions", ["stripe_subscription_id"])

    # --- analysis_jobs ---
    op.create_table(
        "analysis_jobs",
        sa.Column("id", postgresql.UUID(), nullable=False, default=sa.text("gen_random_uuid()")),
        sa.Column("workspace_id", postgresql.UUID(), nullable=True),
        sa.Column("user_id", postgresql.UUID(), nullable=True),
        sa.Column("org_id", postgresql.UUID(), nullable=True),
        sa.Column("file_type", sa.String(20), server_default="sql", nullable=False),
        sa.Column("file_name", sa.String(255), nullable=False),
        sa.Column("file_hash", sa.String(64), nullable=True),
        sa.Column("torque_score", sa.Integer(), nullable=True),
        sa.Column("quality_gate", sa.String(20), nullable=True),
        sa.Column("status", sa.String(20), server_default="pending", nullable=False),
        sa.Column("progress", sa.Integer(), server_default="0", nullable=False),
        sa.Column("result_json", postgresql.JSONB(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("processing_time_ms", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        # SaaS extensions
        sa.Column("celery_task_id", sa.String(255), nullable=True),
        sa.Column("job_type", sa.String(50), server_default="optimize", nullable=False),
        sa.Column("input_sql", sa.Text(), nullable=True),
        sa.Column("credential_id", postgresql.UUID(), nullable=True),
        sa.Column("llm_tokens_prompt", sa.Integer(), server_default="0", nullable=False),
        sa.Column("llm_tokens_completion", sa.Integer(), server_default="0", nullable=False),
        sa.Column("llm_cost_usd", sa.Float(), nullable=True),
        sa.Column("best_speedup", sa.Float(), nullable=True),
        sa.Column("best_sql", sa.Text(), nullable=True),
        sa.Column("outcome", sa.String(20), nullable=True),
        sa.Column("callback_url", sa.String(2048), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["workspace_id"], ["workspaces.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["credential_id"], ["credentials.id"], ondelete="SET NULL"),
    )
    op.create_index("idx_jobs_workspace", "analysis_jobs", ["workspace_id"])
    op.create_index("idx_jobs_user", "analysis_jobs", ["user_id"])
    op.create_index("idx_jobs_org", "analysis_jobs", ["org_id"])
    op.create_index("idx_jobs_status", "analysis_jobs", ["status"])
    op.create_index("idx_jobs_created", "analysis_jobs", ["created_at"])
    op.create_index("idx_jobs_celery", "analysis_jobs", ["celery_task_id"])

    # --- api_usage ---
    op.create_table(
        "api_usage",
        sa.Column("id", postgresql.UUID(), nullable=False, default=sa.text("gen_random_uuid()")),
        sa.Column("org_id", postgresql.UUID(), nullable=False),
        sa.Column("user_id", postgresql.UUID(), nullable=True),
        sa.Column("endpoint", sa.String(255), nullable=False),
        sa.Column("method", sa.String(10), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("response_time_ms", sa.Integer(), nullable=True),
        sa.Column("request_size", sa.Integer(), nullable=True),
        sa.Column("response_size", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="SET NULL"),
    )
    op.create_index("idx_usage_org_date", "api_usage", ["org_id", "created_at"])
    op.create_index("idx_usage_user_date", "api_usage", ["user_id", "created_at"])
    op.create_index("idx_usage_endpoint", "api_usage", ["endpoint", "created_at"])

    # --- llm_usage ---
    op.create_table(
        "llm_usage",
        sa.Column("id", postgresql.UUID(), nullable=False, default=sa.text("gen_random_uuid()")),
        sa.Column("org_id", postgresql.UUID(), nullable=False),
        sa.Column("job_id", postgresql.UUID(), nullable=True),
        sa.Column("provider", sa.String(50), nullable=False),
        sa.Column("model", sa.String(100), nullable=False),
        sa.Column("prompt_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("completion_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("total_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("cost_usd", sa.Float(), nullable=True),
        sa.Column("cache_hit_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("call_type", sa.String(50), server_default="optimize", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_id"], ["analysis_jobs.id"], ondelete="SET NULL"),
    )
    op.create_index("idx_llm_usage_org_date", "llm_usage", ["org_id", "created_at"])
    op.create_index("idx_llm_usage_job", "llm_usage", ["job_id"])

    # --- fleet_surveys ---
    op.create_table(
        "fleet_surveys",
        sa.Column("id", postgresql.UUID(), nullable=False, default=sa.text("gen_random_uuid()")),
        sa.Column("org_id", postgresql.UUID(), nullable=False),
        sa.Column("credential_id", postgresql.UUID(), nullable=True),
        sa.Column("status", sa.String(20), server_default="pending", nullable=False),
        sa.Column("triage_json", postgresql.JSONB(), nullable=True),
        sa.Column("results_json", postgresql.JSONB(), nullable=True),
        sa.Column("celery_task_id", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["credential_id"], ["credentials.id"], ondelete="SET NULL"),
    )
    op.create_index("idx_fleet_surveys_org", "fleet_surveys", ["org_id"])

    # --- github_installations ---
    op.create_table(
        "github_installations",
        sa.Column("id", postgresql.UUID(), nullable=False, default=sa.text("gen_random_uuid()")),
        sa.Column("org_id", postgresql.UUID(), nullable=False),
        sa.Column("installation_id", sa.Integer(), nullable=False),
        sa.Column("encrypted_access_token", sa.LargeBinary(), nullable=True),
        sa.Column("repos", postgresql.JSONB(), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("installation_id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
    )
    op.create_index("idx_github_installations_org", "github_installations", ["org_id"])


def downgrade() -> None:
    """Drop all tables in reverse dependency order."""
    op.drop_table("github_installations")
    op.drop_table("fleet_surveys")
    op.drop_table("llm_usage")
    op.drop_table("api_usage")
    op.drop_table("analysis_jobs")
    op.drop_table("subscriptions")
    op.drop_table("credentials")
    op.drop_table("workspaces")
    op.drop_table("users")
    op.drop_table("organizations")
