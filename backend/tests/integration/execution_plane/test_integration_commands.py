"""Integration revisions freeze credential/configuration state in the journal."""

import pytest
from sqlalchemy import func, select

from syntara.execution_plane.commands.integrations import enqueue_integration_revision
from syntara.integrations.models.integration import Integration, IntegrationUpdate


@pytest.mark.asyncio
async def test_queued_revision_survives_credential_change(integration_case):
    entry_id = await integration_case.enqueue_with_token("first-test-token")
    await integration_case.rotate_token("second-test-token")
    message = await integration_case.load_journal_message(entry_id)
    assert message.command.payload["credential"] == "first-test-token"
    assert "first-test-token" not in await integration_case.raw_journal_text()
    assert message.command.payload["default_target"]["placement"]["namespace"] == "test-workers"


@pytest.mark.asyncio
async def test_same_revision_retry_keeps_original_identity_and_payload(integration_case):
    entry_id = await integration_case.enqueue_with_token("first-test-token")
    original = await integration_case.load_journal_message(entry_id)
    await integration_case.rotate_token("second-test-token")
    async with integration_case.sessions() as session, session.begin():
        integration = await session.get(Integration, integration_case.integration_id)
        retried = await enqueue_integration_revision(session, integration, "upsert", runtime=integration_case.runtime)
    assert retried == original


@pytest.mark.asyncio
async def test_integration_and_outbound_rollback_together(integration_case):
    from unittest.mock import AsyncMock, patch

    await integration_case.credentials("first-test-token")
    async with integration_case.sessions() as session:
        with patch.object(session, "commit", AsyncMock(side_effect=RuntimeError("test commit failure"))):
            with pytest.raises(RuntimeError, match="test commit failure"):
                await integration_case.create(session)
        await session.rollback()
    async with integration_case.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Integration)) == 0
        assert await session.scalar(select(func.count()).select_from(integration_case.journal.schema.table)) == 0


@pytest.mark.asyncio
async def test_only_current_revision_completion_changes_ui_state(integration_case):
    first = await integration_case.enqueue_with_token("test-token")
    second = await integration_case.update(IntegrationUpdate(name="Renamed cluster"))
    await integration_case.complete(first)
    assert (await integration_case.integration()).execution_plane_status == "pending"
    await integration_case.complete(second)
    assert (await integration_case.integration()).execution_plane_status == "ready"


@pytest.mark.asyncio
async def test_disable_and_delete_freeze_explicit_commands(integration_case):
    await integration_case.enqueue_with_token("test-token")
    disabled = await integration_case.update(IntegrationUpdate(enabled=False))
    message = await integration_case.load_journal_message(disabled)
    assert message.command.command_type == "cluster.provision"
    assert message.command.payload["enabled"] is False
    deleted = await integration_case.delete()
    tombstone = await integration_case.load_journal_message(deleted)
    assert tombstone.command.command_type == "cluster.delete"
    assert tombstone.command.payload["resource_key"] == str(integration_case.integration_id)
    await integration_case.complete(disabled)
    await integration_case.complete(deleted)
    assert await integration_case.integration() is None


@pytest.mark.asyncio
async def test_project_assignments_are_frozen_per_revision(integration_case):
    first = await integration_case.enqueue_with_token("test-token", project_scoped=True)
    second = await integration_case.assign_project()
    assert (await integration_case.load_journal_message(first)).command.payload["project_ids"] == []
    assert (await integration_case.load_journal_message(second)).command.payload["project_ids"] == [
        str(integration_case.project_id)
    ]


@pytest.mark.asyncio
async def test_superseded_current_revision_exposes_safe_error(integration_case):
    entry_id = await integration_case.enqueue_with_token("test-token")
    await integration_case.complete(entry_id, status="superseded")
    state = await integration_case.integration()
    assert state.execution_plane_status == "error"
    assert state.execution_plane_error == "command_superseded"


@pytest.mark.asyncio
async def test_reconciliation_reads_command_before_recovering_completion(integration_case):
    from unittest.mock import AsyncMock

    from execution_plane_client.protocol import CommandRead, CommandStatus

    entry_id = await integration_case.enqueue_with_token("test-token")
    message = await integration_case.load_journal_message(entry_id)
    integration_case.runtime.transport.get_command = AsyncMock(
        return_value=CommandRead(command_id=message.command_id, status=CommandStatus.RUNNING, latest_child_sequence=0)
    )
    integration_case.runtime.transport.get_children = AsyncMock()
    await integration_case.runtime.reconciliation.reconcile_command(message.scope, message.command_id)
    integration_case.runtime.transport.get_command.assert_awaited_once()
    integration_case.runtime.transport.get_children.assert_not_awaited()
    assert (await integration_case.integration()).execution_plane_status == "pending"


async def test_maximum_integration_name_produces_valid_default_target(integration_case):
    await integration_case.enqueue_with_token("test-token")
    entry_id = await integration_case.update(IntegrationUpdate(name="x" * 255))
    payload = (await integration_case.load_journal_message(entry_id)).command.payload
    assert payload["name"] == "x" * 255
    assert len(payload["default_target"]["name"]) <= 255
    assert payload["default_target"]["name"].endswith("-default")


async def test_disabled_credential_does_not_prevent_draining_integration(integration_case):
    from syntara.credentials.models.credential import Credential

    await integration_case.enqueue_with_token("test-token")
    async with integration_case.sessions() as session, session.begin():
        credential = await session.get(Credential, integration_case.credential_id)
        credential.enabled = False
    entry_id = await integration_case.update(IntegrationUpdate(enabled=False))
    payload = (await integration_case.load_journal_message(entry_id)).command.payload
    assert payload["enabled"] is False


async def test_legacy_preflight_refuses_unobserved_resource_even_after_http_delivery(
    integration_case, test_db_engine, test_db_admin_url
):
    import asyncio
    import importlib

    from alembic import command
    from alembic.config import Config
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import MetaData, Table, delete, insert, select, update
    from sqlalchemy.ext.asyncio import create_async_engine

    migration = importlib.import_module(
        "syntara.core.database.migrations.versions.60718293a4b5_retire_ep_transport_tables"
    )
    await integration_case.enqueue_with_token("test-token")
    admin_url = test_db_admin_url.set(database=test_db_engine.url.database)
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", admin_url.render_as_string(hide_password=False))
    admin_engine = create_async_engine(admin_url)
    # The new guard correctly refuses to discard current resource metadata.
    # Rebuild this fixture at its historical schema using the preserved common
    # journal columns; its immutable command remains the observation owner.
    with pytest.raises(RuntimeError, match="Refusing journal intent downgrade"):
        await asyncio.to_thread(command.downgrade, cfg, "5f60718293a4")
    async with admin_engine.begin() as connection:
        rows = (await connection.execute(select(integration_case.journal.schema.table))).mappings().all()
        await connection.execute(delete(integration_case.journal.schema.table))
    await asyncio.to_thread(command.downgrade, cfg, "5f60718293a4")
    try:
        async with admin_engine.begin() as connection:
            historical_journal = await connection.run_sync(
                lambda conn: Table("exchange_journal", MetaData(), autoload_with=conn)
            )
            await connection.execute(
                insert(historical_journal),
                [{column.name: row[column.name] for column in historical_journal.columns} for row in rows],
            )
        async with admin_engine.connect() as connection, connection.begin() as transaction:

            def preflight(conn) -> None:
                with Operations.context(MigrationContext.configure(conn)):
                    migration._refuse_unresolved_legacy()

            # HTTP delivery does not finish the resource observation. A current
            # command still owns it, so the historical preflight permits upgrade.
            await connection.execute(
                update(integration_case.journal.schema.table).values(delivery_state="acknowledged")
            )
            await connection.run_sync(preflight)
            await connection.execute(delete(integration_case.journal.schema.table))
            with pytest.raises(RuntimeError, match="integration_observations=1"):
                await connection.run_sync(preflight)
            # Restore command ownership before applying the guarded migration.
            await transaction.rollback()
    finally:
        await admin_engine.dispose()
        await asyncio.to_thread(command.upgrade, cfg, "head")
