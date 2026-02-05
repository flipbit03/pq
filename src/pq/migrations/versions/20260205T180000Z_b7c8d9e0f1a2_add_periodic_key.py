"""add periodic key

Revision ID: b7c8d9e0f1a2
Revises: a1b2c3d4e5f6
Create Date: 2026-02-05 18:00:00 Z

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b7c8d9e0f1a2"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add key column to pq_periodic and update unique constraint."""
    op.add_column(
        "pq_periodic",
        sa.Column("key", sa.String(255), nullable=False, server_default=""),
    )
    op.drop_constraint("pq_periodic_name_key", "pq_periodic", type_="unique")
    op.create_unique_constraint(
        "uq_pq_periodic_name_key", "pq_periodic", ["name", "key"]
    )


def downgrade() -> None:
    """Remove key column and restore original unique constraint."""
    op.drop_constraint("uq_pq_periodic_name_key", "pq_periodic", type_="unique")
    op.drop_column("pq_periodic", "key")
    op.create_unique_constraint("pq_periodic_name_key", "pq_periodic", ["name"])
