"""Tests for the green-field Cluster and ExecutionTarget migration."""

from __future__ import annotations

import importlib
from unittest.mock import patch


def test_cluster_migration_is_next_revision_and_defines_cluster_table() -> None:
    """The migration must create the Cluster table and target ownership fields."""
    migration = importlib.import_module(
        "execution_plane.migrations.versions.b7c8d9e0f1a2_add_clusters_and_registry_fields"
    )

    assert migration.revision == "b7c8d9e0f1a2"
    assert migration.down_revision == "9f3e1a2b4c7d"
    assert "clusters" in migration.upgrade.__doc__


def test_cluster_migration_has_no_legacy_target_backfill() -> None:
    """Green-field migration code must not contain legacy-row migration logic."""
    migration = importlib.import_module(
        "execution_plane.migrations.versions.b7c8d9e0f1a2_add_clusters_and_registry_fields"
    )

    source = migration.__loader__.get_source(migration.__name__)  # type: ignore[union-attr]
    assert source is not None
    assert "op.create_table" in source
    assert '"clusters"' in source
    assert '"execution_targets_cluster_id_fkey"' in source
    assert '"uq_execution_targets_default_cluster"' not in source
    assert 'postgresql_where=sa.text("is_default = true")' not in source
    assert "backfill" not in source.lower()
    assert "legacy" not in source.lower()


def test_cluster_migration_upgrade_records_required_schema_operations() -> None:
    """The upgrade operation creates ownership and lifecycle fields."""
    migration = importlib.import_module(
        "execution_plane.migrations.versions.b7c8d9e0f1a2_add_clusters_and_registry_fields"
    )

    with (
        patch.object(migration.op, "create_table") as create_table,
        patch.object(migration.op, "add_column") as add_column,
        patch.object(migration.op, "create_foreign_key") as create_foreign_key,
        patch.object(migration.op, "create_index") as create_index,
    ):
        migration.upgrade()

    cluster_call = next(call for call in create_table.call_args_list if call.args[0] == "clusters")
    cluster_columns = {column.name: column for column in cluster_call.args[1:] if hasattr(column, "name")}
    assert {"id", "endpoint", "status", "enabled", "status_message", "api_key"} <= set(cluster_columns)

    target_columns = {
        call.args[1].name: call.args[1] for call in add_column.call_args_list if call.args[0] == "execution_targets"
    }
    default_column = target_columns["is_default"]
    assert default_column.nullable is False
    assert default_column.server_default is None

    create_foreign_key.assert_called_once_with(
        "execution_targets_cluster_id_fkey",
        "execution_targets",
        "clusters",
        ["cluster_id"],
        ["id"],
        source_schema="execution_plane",
        referent_schema="execution_plane",
    )
    create_index.assert_not_called()
