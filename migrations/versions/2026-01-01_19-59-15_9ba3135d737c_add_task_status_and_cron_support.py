"""add task status and cron support

Revision ID: 9ba3135d737c
Revises: bcc7618d02da
Create Date: 2026-01-01 19:59:15.244469

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "9ba3135d737c"
down_revision: str = "bcc7618d02da"
branch_labels: None = None
depends_on: None = None

# PostgreSQL ENUM type for task status
task_status_enum = postgresql.ENUM(
    "pending", "running", "completed", "failed", name="task_status"
)


def upgrade() -> None:
    """Add status tracking to tasks and cron support to periodic."""
    # Create the enum type
    task_status_enum.create(op.get_bind(), checkfirst=True)

    # Add status columns to pq_tasks
    op.add_column(
        "pq_tasks",
        sa.Column("status", task_status_enum, nullable=False, server_default="pending"),
    )
    op.add_column(
        "pq_tasks",
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "pq_tasks",
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "pq_tasks",
        sa.Column("error", sa.Text(), nullable=True),
    )
    op.add_column(
        "pq_tasks",
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
    )

    # Replace index with one that includes status
    op.drop_index("ix_pq_tasks_priority_run_at", table_name="pq_tasks")
    op.create_index(
        "ix_pq_tasks_status_priority_run_at",
        "pq_tasks",
        ["status", "priority", "run_at"],
    )

    # Add cron column to pq_periodic
    op.add_column(
        "pq_periodic",
        sa.Column("cron", sa.String(100), nullable=True),
    )

    # Make run_every nullable (either run_every or cron is required)
    op.alter_column("pq_periodic", "run_every", nullable=True)


def downgrade() -> None:
    """Remove status tracking and cron support."""
    # Restore run_every as non-nullable (will fail if cron-only tasks exist)
    op.alter_column("pq_periodic", "run_every", nullable=False)

    # Remove cron column
    op.drop_column("pq_periodic", "cron")

    # Restore original index
    op.drop_index("ix_pq_tasks_status_priority_run_at", table_name="pq_tasks")
    op.create_index(
        "ix_pq_tasks_priority_run_at",
        "pq_tasks",
        ["priority", "run_at"],
    )

    # Remove status columns
    op.drop_column("pq_tasks", "attempts")
    op.drop_column("pq_tasks", "error")
    op.drop_column("pq_tasks", "completed_at")
    op.drop_column("pq_tasks", "started_at")
    op.drop_column("pq_tasks", "status")

    # Drop the enum type
    task_status_enum.drop(op.get_bind(), checkfirst=True)
