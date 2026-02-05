"""add max_concurrent

Revision ID: a1b2c3d4e5f6
Revises: 2483bec70083
Create Date: 2026-02-05 12:00:00 Z

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "2483bec70083"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add max_concurrent and locked_until columns to pq_periodic."""
    op.add_column(
        "pq_periodic",
        sa.Column("max_concurrent", sa.SmallInteger(), nullable=True),
    )
    op.add_column(
        "pq_periodic",
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
    )
    # Backfill existing rows to default max_concurrent=1
    op.execute("UPDATE pq_periodic SET max_concurrent = 1 WHERE max_concurrent IS NULL")


def downgrade() -> None:
    """Remove max_concurrent and locked_until columns from pq_periodic."""
    op.drop_column("pq_periodic", "locked_until")
    op.drop_column("pq_periodic", "max_concurrent")
