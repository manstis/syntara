"""Replace execution-target namespace with typed placement JSONB."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "e0f1a2b3c4d5"
down_revision: str | Sequence[str] | None = "d9e0f1a2b3c4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Replace the pre-release namespace column with typed placement JSONB."""
    op.drop_column("execution_targets", "namespace", schema=EP)
    op.add_column("execution_targets", sa.Column("placement", JSONB(), nullable=False), schema=EP)


def downgrade() -> None:
    """Restore the pre-release namespace column and remove placement JSONB."""
    op.drop_column("execution_targets", "placement", schema=EP)
    op.add_column("execution_targets", sa.Column("namespace", sa.String(), nullable=False), schema=EP)
