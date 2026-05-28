"""add max_runtime_seconds to pq_tasks and pq_periodic

Revision ID: aee3e8e7e647
Revises: c3d4e5f6a7b8
Create Date: 2026-05-27 17:01:36 Z

Per-task override for the worker's wall-clock ``max_runtime`` cap, on
both one-off tasks (``pq_tasks``) and periodic schedules
(``pq_periodic``). Nullable on both so every existing row (and every
existing enqueue / schedule call site that doesn't pass the new
parameter) keeps the previous behaviour exactly — a NULL value tells
the worker to use its own default.

Schema impact on populated tables is intentionally minimal: PostgreSQL
11+ treats ``ADD COLUMN ... DEFAULT NULL`` as a catalog-only operation
(no table rewrite, brief ACCESS EXCLUSIVE lock on the system catalog
row, typically < 100 ms even for tables with millions of rows). No
backfill — NULL is the intended default.

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "aee3e8e7e647"
down_revision: Union[str, Sequence[str], None] = "c3d4e5f6a7b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add the nullable ``max_runtime_seconds`` column to both task tables."""
    op.add_column(
        "pq_tasks",
        sa.Column("max_runtime_seconds", sa.Float(), nullable=True),
    )
    op.add_column(
        "pq_periodic",
        sa.Column("max_runtime_seconds", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    """Remove the ``max_runtime_seconds`` column from both task tables."""
    op.drop_column("pq_periodic", "max_runtime_seconds")
    op.drop_column("pq_tasks", "max_runtime_seconds")
