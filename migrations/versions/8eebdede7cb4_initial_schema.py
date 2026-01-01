"""initial_schema

Revision ID: 8eebdede7cb4
Revises:
Create Date: 2026-01-01 17:27:32.581213

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "8eebdede7cb4"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create PQ tables."""
    # Create TaskStatus enum
    task_status = postgresql.ENUM(
        "enabled", "disabled", "paused", name="taskstatus", create_type=False
    )
    task_status.create(op.get_bind(), checkfirst=True)

    # Create pq_periodic_tasks table
    op.create_table(
        "pq_periodic_tasks",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column("name", sa.String(255), unique=True, nullable=False, index=True),
        sa.Column("func_path", sa.String(500), nullable=False),
        sa.Column("args", postgresql.JSONB(), nullable=False, server_default="[]"),
        sa.Column("kwargs", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("cron_expression", sa.String(100), nullable=True),
        sa.Column("interval_seconds", sa.Integer(), nullable=True),
        sa.Column(
            "queue_name", sa.String(100), nullable=False, server_default="default"
        ),
        sa.Column("timeout", sa.Integer(), nullable=True),
        sa.Column("result_ttl", sa.Integer(), nullable=False, server_default="500"),
        sa.Column("ttl", sa.Integer(), nullable=True),
        sa.Column("failure_ttl", sa.Integer(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "status",
            sa.Enum("enabled", "disabled", "paused", name="taskstatus"),
            nullable=False,
            server_default="enabled",
        ),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True, index=True),
        sa.Column("total_run_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_pq_periodic_next_run_status",
        "pq_periodic_tasks",
        ["next_run_at", "status"],
    )

    # Create pq_scheduled_tasks table
    op.create_table(
        "pq_scheduled_tasks",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column("func_path", sa.String(500), nullable=False),
        sa.Column("args", postgresql.JSONB(), nullable=False, server_default="[]"),
        sa.Column("kwargs", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column(
            "queue_name", sa.String(100), nullable=False, server_default="default"
        ),
        sa.Column("timeout", sa.Integer(), nullable=True),
        sa.Column("result_ttl", sa.Integer(), nullable=False, server_default="500"),
        sa.Column("ttl", sa.Integer(), nullable=True),
        sa.Column("failure_ttl", sa.Integer(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "is_executed", sa.Boolean(), nullable=False, server_default="false", index=True
        ),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rq_job_id", sa.String(100), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_pq_scheduled_pending",
        "pq_scheduled_tasks",
        ["run_at", "is_executed"],
    )

    # Create pq_scheduler_locks table
    op.create_table(
        "pq_scheduler_locks",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("scheduler_id", sa.String(100), unique=True, nullable=False),
        sa.Column("hostname", sa.String(255), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column("queues", postgresql.JSONB(), nullable=False),
        sa.Column(
            "last_heartbeat",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "acquired_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_pq_scheduler_heartbeat",
        "pq_scheduler_locks",
        ["last_heartbeat"],
    )


def downgrade() -> None:
    """Drop PQ tables."""
    op.drop_table("pq_scheduler_locks")
    op.drop_table("pq_scheduled_tasks")
    op.drop_table("pq_periodic_tasks")

    # Drop enum type
    task_status = postgresql.ENUM("enabled", "disabled", "paused", name="taskstatus")
    task_status.drop(op.get_bind(), checkfirst=True)
