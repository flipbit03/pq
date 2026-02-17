"""add periodic active

Revision ID: c3d4e5f6a7b8
Revises: b7c8d9e0f1a2
Create Date: 2026-02-17 12:00:00 Z

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c3d4e5f6a7b8"
down_revision: Union[str, Sequence[str], None] = "b7c8d9e0f1a2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add active column to pq_periodic."""
    op.add_column(
        "pq_periodic",
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
    )


def downgrade() -> None:
    """Remove active column from pq_periodic."""
    op.drop_column("pq_periodic", "active")
