"""add requeue to pq_tasks

Revision ID: d4e5f6a7b8c9
Revises: aee3e8e7e647
Create Date: 2026-10-07 12:00:00 Z

Holds the next version of a one-off task when ``upsert()`` hits a row
that is RUNNING. The current run keeps its row unchanged; whoever ends
the run (worker or stale reaper) re-queues the row from this column
instead of closing it. This makes sure one ``client_id`` never runs
twice at the same time (ricwo/pq#27).

Nullable without a default, so on PostgreSQL 11+ ``ADD COLUMN`` is a
catalog-only change (no table rewrite). Existing rows get NULL, which
means "no parked version" — the previous behaviour.

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, Sequence[str], None] = "aee3e8e7e647"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add the nullable ``requeue`` column to ``pq_tasks``."""
    op.add_column(
        "pq_tasks",
        sa.Column("requeue", postgresql.JSONB(), nullable=True),
    )


def downgrade() -> None:
    """Remove the ``requeue`` column from ``pq_tasks``."""
    op.drop_column("pq_tasks", "requeue")
