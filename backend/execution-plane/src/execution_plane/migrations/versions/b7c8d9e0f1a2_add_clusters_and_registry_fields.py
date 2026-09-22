"""Add clusters and Cluster-owned ExecutionTarget fields.

Revision ID: b7c8d9e0f1a2
Revises: 9f3e1a2b4c7d
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "b7c8d9e0f1a2"
down_revision: str | Sequence[str] | None = "9f3e1a2b4c7d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EP = "execution_plane"


def upgrade() -> None:
    """Create clusters and add Cluster ownership to execution targets."""
    op.create_table(
        "clusters",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("endpoint", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("status_message", sa.String(), nullable=True),
        sa.Column("labels", JSONB(), nullable=False, server_default="{}"),
        sa.Column("api_key", sa.String(), nullable=False),
        sa.Column("created_by", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by", sa.UUID(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="clusters_name_key"),
        sa.UniqueConstraint("endpoint", name="clusters_endpoint_key"),
        schema=EP,
    )
    op.add_column(
        "execution_targets",
        sa.Column("cluster_id", sa.UUID(), nullable=False),
        schema=EP,
    )
    op.add_column(
        "execution_targets",
        sa.Column("is_default", sa.Boolean(), nullable=False),
        schema=EP,
    )
    op.add_column("execution_targets", sa.Column("api_key", sa.String(), nullable=False), schema=EP)
    op.add_column("execution_targets", sa.Column("status_message", sa.String(), nullable=True), schema=EP)
    op.add_column("execution_targets", sa.Column("created_by", sa.UUID(), nullable=False), schema=EP)
    op.add_column("execution_targets", sa.Column("updated_by", sa.UUID(), nullable=False), schema=EP)
    op.add_column(
        "execution_targets",
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        schema=EP,
    )
    op.create_foreign_key(
        "execution_targets_cluster_id_fkey",
        "execution_targets",
        "clusters",
        ["cluster_id"],
        ["id"],
        source_schema=EP,
        referent_schema=EP,
    )


def downgrade() -> None:
    """Remove Cluster ownership and the clusters table."""
    op.drop_constraint(
        "execution_targets_cluster_id_fkey",
        "execution_targets",
        schema=EP,
        type_="foreignkey",
    )
    for column_name in (
        "updated_at",
        "updated_by",
        "created_by",
        "status_message",
        "api_key",
        "is_default",
        "cluster_id",
    ):
        op.drop_column("execution_targets", column_name, schema=EP)
    op.drop_table("clusters", schema=EP)
