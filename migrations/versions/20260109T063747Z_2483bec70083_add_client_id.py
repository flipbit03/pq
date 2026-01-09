"""add client_id

Revision ID: 2483bec70083
Revises: 476683af098d
Create Date: 2026-01-09 06:37:47 Z

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "2483bec70083"
down_revision: Union[str, Sequence[str], None] = "476683af098d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add client_id column to pq_tasks and pq_periodic tables."""
    # Add client_id column to pq_tasks
    op.add_column(
        "pq_tasks",
        sa.Column("client_id", sa.String(length=255), nullable=True),
    )
    op.create_index(
        "ix_pq_tasks_client_id",
        "pq_tasks",
        ["client_id"],
        unique=True,
    )

    # Add client_id column to pq_periodic
    op.add_column(
        "pq_periodic",
        sa.Column("client_id", sa.String(length=255), nullable=True),
    )
    op.create_index(
        "ix_pq_periodic_client_id",
        "pq_periodic",
        ["client_id"],
        unique=True,
    )


def downgrade() -> None:
    """Remove client_id column from pq_tasks and pq_periodic tables."""
    op.drop_index("ix_pq_periodic_client_id", table_name="pq_periodic")
    op.drop_column("pq_periodic", "client_id")
    op.drop_index("ix_pq_tasks_client_id", table_name="pq_tasks")
    op.drop_column("pq_tasks", "client_id")
