"""AO migrations retire legacy tables without losing live delivery responsibility."""

import importlib
from uuid import uuid4

import pytest
import pytest_asyncio
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest_asyncio.fixture
async def migration_engine(test_db_engine, test_db_admin_url):
    """Use the migration owner for tables recreated by historical downgrades."""
    engine = create_async_engine(test_db_admin_url.set(database=test_db_engine.url.database))
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_journal_is_owned_only_by_ao_default_schema(test_db_session):
    connection = await test_db_session.connection()
    tables = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
    assert "exchange_journal" in tables
    assert "execution_plane_activity_bindings" not in tables
    assert "execution_plane_outcome_history" not in tables
    schemas = await connection.run_sync(lambda conn: inspect(conn).get_schema_names())
    assert "execution_plane" not in schemas


@pytest.mark.asyncio
async def test_legacy_preflight_refuses_pending_but_preserves_terminal_bindings(
    test_db_engine, test_db_session, test_db_admin_url, migration_engine
):
    import asyncio

    from alembic import command
    from alembic.config import Config

    await test_db_session.commit()
    cfg = Config("alembic.ini")
    cfg.set_main_option(
        "sqlalchemy.url",
        test_db_admin_url.set(database=test_db_engine.url.database).render_as_string(hide_password=False),
    )
    await asyncio.to_thread(command.downgrade, cfg, "5f60718293a4")
    async with migration_engine.begin() as connection:
        await connection.execute(
            text("""
            INSERT INTO execution_plane_activity_bindings
            (id, request_id, client_id, project_id, temporal_workflow_id, temporal_run_id,
             temporal_activity_id, activity_attempt, task_token_ciphertext, request_payload_ciphertext,
             status, created_at, updated_at)
            VALUES (:id, 'legacy-request', 'ao', :project, 'workflow', 'run', 'activity', 1,
                    'sensitive-token', 'private-payload', 'handoff_pending', now(), now())
        """),
            {"id": uuid4(), "project": uuid4()},
        )
    migration = importlib.import_module(
        "syntara.core.database.migrations.versions.60718293a4b5_retire_ep_transport_tables"
    )

    def preflight(conn) -> None:
        with Operations.context(MigrationContext.configure(conn)):
            migration._refuse_unresolved_legacy()

    async with migration_engine.begin() as connection:
        with pytest.raises(RuntimeError, match="bindings=1") as caught:
            await connection.run_sync(preflight)
        assert "sensitive-token" not in str(caught.value)
        assert "private-payload" not in str(caught.value)
        await connection.execute(text("UPDATE execution_plane_activity_bindings SET status = 'completed'"))
        await connection.run_sync(preflight)
        assert await connection.scalar(text("SELECT count(*) FROM execution_plane_activity_bindings")) == 1


@pytest.mark.asyncio
async def test_cleanup_preserves_completed_outcomes_and_refuses_pending_rows(
    test_db_engine, test_db_session, test_db_admin_url, migration_engine
):
    import asyncio

    from alembic import command
    from alembic.config import Config
    from sqlalchemy import text

    await test_db_session.commit()
    cfg = Config("alembic.ini")
    cfg.set_main_option(
        "sqlalchemy.url",
        test_db_admin_url.set(database=test_db_engine.url.database).render_as_string(hide_password=False),
    )
    await asyncio.to_thread(command.downgrade, cfg, "5f60718293a4")
    event_id, work_id, project_id = uuid4(), uuid4(), uuid4()
    async with migration_engine.begin() as connection:
        await connection.execute(
            text("""
            INSERT INTO execution_plane_completion_inbox
            (event_id, client_id, project_id, work_item_id, request_id, state_revision,
             status, result, completed_at, received_at, next_attempt_at)
            VALUES (:event, 'ao', :project, :work, 'historical-request', 1, 'completed',
                    '{"private": "historical-result"}', now(), now(), now())
        """),
            {"event": event_id, "project": project_id, "work": work_id},
        )
    with pytest.raises(RuntimeError, match="completions=1") as caught:
        await asyncio.to_thread(command.upgrade, cfg, "60718293a4b5")
    assert "historical-result" not in str(caught.value)
    async with migration_engine.begin() as connection:
        await connection.execute(text("UPDATE execution_plane_completion_inbox SET processed_at = now()"))
    await asyncio.to_thread(command.upgrade, cfg, "60718293a4b5")
    async with migration_engine.connect() as connection:
        history = (
            (
                await connection.execute(
                    text("SELECT result, request_id FROM execution_plane_outcome_history WHERE event_id = :event"),
                    {"event": event_id},
                )
            )
            .mappings()
            .one()
        )
        assert history["result"] == {"private": "historical-result"}
        assert history["request_id"] == "historical-request"
        tables = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
    assert "execution_plane_completion_inbox" not in tables
    assert "execution_plane_integration_sync" not in tables


@pytest.mark.asyncio
@pytest.mark.parametrize("command_backed", [False, True])
async def test_context_migration_refuses_live_tokens_and_removes_completed_bindings(
    test_db_engine, test_db_session, test_db_admin_url, migration_engine, command_backed
):
    import asyncio

    from alembic import command
    from alembic.config import Config

    await test_db_session.commit()
    cfg = Config("alembic.ini")
    cfg.set_main_option(
        "sqlalchemy.url",
        test_db_admin_url.set(database=test_db_engine.url.database).render_as_string(hide_password=False),
    )
    await asyncio.to_thread(command.downgrade, cfg, "60718293a4b5")
    async with migration_engine.begin() as connection:
        await connection.execute(
            text("""
            INSERT INTO execution_plane_activity_bindings
            (id, command_id, request_id, client_id, project_id, temporal_workflow_id, temporal_run_id,
             temporal_activity_id, activity_attempt, task_token_ciphertext, request_payload_ciphertext,
             status, created_at, updated_at)
            VALUES (:id, :command, 'old-request', 'ao', :project, 'workflow', 'run', 'activity', 1,
                    'sensitive-token', 'private-payload', 'reconciliation_required', now(), now())
        """),
            {"id": uuid4(), "command": uuid4() if command_backed else None, "project": uuid4()},
        )
    with pytest.raises(RuntimeError, match="bindings=1") as caught:
        await asyncio.to_thread(command.upgrade, cfg, "head")
    assert "sensitive-token" not in str(caught.value)
    assert "private-payload" not in str(caught.value)
    async with migration_engine.begin() as connection:
        await connection.execute(text("UPDATE execution_plane_activity_bindings SET status = 'completed'"))
    await asyncio.to_thread(command.upgrade, cfg, "head")
    async with migration_engine.begin() as connection:
        assert "execution_plane_activity_bindings" not in await connection.run_sync(
            lambda conn: inspect(conn).get_table_names()
        )
        columns = await connection.run_sync(
            lambda conn: {c["name"] for c in inspect(conn).get_columns("exchange_journal")}
        )
        assert {"local_context_ciphertext", "local_context_generation", "correlation_key"} <= columns


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("parent_status", "reconciliation_required"),
    [("outcome_unknown", True), ("outcome_unknown", False), ("accepted", True)],
)
async def test_context_migration_preserves_completed_token_while_parent_needs_resolution(
    test_db_engine, test_db_session, test_db_admin_url, migration_engine, parent_status, reconciliation_required
):
    import asyncio

    from alembic import command
    from alembic.config import Config

    await test_db_session.commit()
    cfg = Config("alembic.ini")
    cfg.set_main_option(
        "sqlalchemy.url",
        test_db_admin_url.set(database=test_db_engine.url.database).render_as_string(hide_password=False),
    )
    await asyncio.to_thread(command.downgrade, cfg, "60718293a4b5")
    parent_id, child_id, project_id = uuid4(), uuid4(), uuid4()
    async with migration_engine.begin() as connection:
        await connection.execute(
            text("""
            INSERT INTO execution_plane_activity_bindings
            (id, command_id, request_id, client_id, project_id, temporal_workflow_id, temporal_run_id,
             temporal_activity_id, activity_attempt, task_token_ciphertext, request_payload_ciphertext,
             status, expected_completion_id, expected_completion_sequence, created_at, updated_at)
            VALUES (:id, :parent, 'uncertain-request', 'ao', :project, 'workflow', 'run', 'activity', 1,
                    'sensitive-token', 'private-payload', 'completed', :child, 1, now(), now())
            """),
            {"id": uuid4(), "parent": parent_id, "child": child_id, "project": project_id},
        )
        for is_parent, command_id in [(True, parent_id), (False, child_id)]:
            await connection.execute(
                text("""
                INSERT INTO exchange_journal
                (entry_id, message_id, command_id, parent_command_id, parent_sequence, message_type,
                 command_type, command_revision, sequence, source_id, destination_id, client_id,
                 project_id, all_projects, direction, target_url, callback_url, envelope_hash,
                 envelope_ciphertext, delivery_state, handling_state, command_status,
                 reconciliation_required, created_at, updated_at)
                VALUES (:entry, :message, :command, :parent, :parent_sequence, 'command.submit',
                        :command_type, 1, 0, :source, :destination, 'ao', :project, false, :direction,
                        'https://target.test/commands', 'https://callback.test/commands', :hash,
                        :ciphertext, 'acknowledged', :handling, :status, :reconcile, now(), now())
                """),
                {
                    "entry": uuid4(),
                    "message": uuid4(),
                    "command": command_id,
                    "parent": None if is_parent else parent_id,
                    "parent_sequence": None if is_parent else 1,
                    "command_type": "work.execute" if is_parent else "work.execute.completed",
                    "source": "ao" if is_parent else "ep",
                    "destination": "ep" if is_parent else "ao",
                    "project": project_id,
                    "direction": "outbound" if is_parent else "inbound",
                    "hash": "0" * 64,
                    "ciphertext": "private-command" if is_parent else "private-outcome-unknown-completion",
                    "handling": "idle" if is_parent else "handled",
                    "status": parent_status if is_parent else "succeeded",
                    "reconcile": reconciliation_required if is_parent else False,
                },
            )
    with pytest.raises(RuntimeError, match="bindings=0, effects=0, parents=1") as caught:
        await asyncio.to_thread(command.upgrade, cfg, "head")
    assert "sensitive-token" not in str(caught.value)
    assert "private-payload" not in str(caught.value)
    async with migration_engine.begin() as connection:
        assert (
            await connection.scalar(
                text("SELECT task_token_ciphertext FROM execution_plane_activity_bindings WHERE command_id = :command"),
                {"command": parent_id},
            )
            == "sensitive-token"
        )
        assert await connection.scalar(text("SELECT version_num FROM alembic_version")) == "60718293a4b5"
        await connection.execute(
            text(
                "UPDATE exchange_journal SET command_status = 'succeeded', reconciliation_required = false "
                "WHERE command_id = :command"
            ),
            {"command": parent_id},
        )
    await asyncio.to_thread(command.upgrade, cfg, "head")


@pytest.mark.asyncio
async def test_shared_intent_schema_matches_current_client(migration_engine):
    from alembic.autogenerate import compare_metadata
    from execution_plane_client.journal.schema import build_journal_schema

    def compare(connection):  # noqa: ANN202 - SQLAlchemy metadata diff objects vary.
        context = MigrationContext.configure(
            connection,
            opts={
                "compare_server_default": True,
                "include_object": lambda _obj, name, kind, _reflected, _target: kind != "table"
                or name == "exchange_journal",
            },
        )
        return compare_metadata(context, build_journal_schema().metadata)

    async with migration_engine.connect() as connection:
        assert await connection.run_sync(compare) == []
        checks = await connection.run_sync(lambda conn: inspect(conn).get_check_constraints("exchange_journal"))
        assert {"ck_exchange_resource_head", "ck_exchange_resource_revision"} <= {check["name"] for check in checks}


@pytest.mark.asyncio
async def test_shared_intent_upgrade_preserves_historical_private_context(migration_engine):
    migration = importlib.import_module(
        "syntara.core.database.migrations.versions.13745a87795f_add_shared_command_intent_and_"
    )
    async with migration_engine.connect() as connection:
        transaction = await connection.begin()
        try:

            def downgrade(conn) -> None:
                with Operations.context(MigrationContext.configure(conn)):
                    migration.downgrade()

            await connection.run_sync(downgrade)
            command_id = uuid4()
            await connection.execute(
                text("""
                INSERT INTO exchange_journal
                (entry_id, message_id, command_id, message_type, command_type, command_revision,
                 sequence, source_id, destination_id, client_id, all_projects, direction,
                 target_url, callback_url, envelope_hash, envelope_ciphertext, delivery_state,
                 handling_state, command_status, local_context_ciphertext, correlation_key, created_at, updated_at)
                VALUES (:entry, :message, :command, 'command.submit', 'work.execute', 1, 0,
                        'ao', 'ep', 'ao', true, 'outbound', 'https://ep.test', 'https://ao.test',
                        repeat('a',64), 'immutable-private-envelope', 'acknowledged', 'handled',
                        'running', 'encrypted-temporal-context', 'activity-correlation', now(), now())
            """),
                {"entry": uuid4(), "message": uuid4(), "command": command_id},
            )

            def upgrade(conn) -> None:
                with Operations.context(MigrationContext.configure(conn)):
                    migration.upgrade()

            await connection.run_sync(upgrade)
            row = (
                (
                    await connection.execute(
                        text("SELECT * FROM exchange_journal WHERE command_id = :command"), {"command": command_id}
                    )
                )
                .mappings()
                .one()
            )
            assert row["envelope_ciphertext"] == "immutable-private-envelope"
            assert row["local_context_ciphertext"] == "encrypted-temporal-context"
            assert row["correlation_key"] == "activity-correlation"
            assert row["command_status"] == "running"
            assert row["resource_type"] is None
            assert not row["observation_pending"]
            assert row["observed_revision"] == 0
            await connection.execute(
                text("UPDATE exchange_journal SET resource_type = 'work_item' WHERE command_id = :command"),
                {"command": command_id},
            )
            with pytest.raises(RuntimeError, match="Refusing journal intent downgrade") as caught:
                await connection.run_sync(downgrade)
            assert "encrypted-temporal-context" not in str(caught.value)
            assert "immutable-private-envelope" not in str(caught.value)
        finally:
            await transaction.rollback()


@pytest.mark.asyncio
async def test_history_removal_preserves_live_journal_and_private_context(migration_engine):
    migration = importlib.import_module(
        "syntara.core.database.migrations.versions.a4a88bd2763a_drop_legacy_execution_plane_outcome_"
    )

    def downgrade(connection) -> None:
        with Operations.context(MigrationContext.configure(connection)):
            migration.downgrade()

    def upgrade(connection) -> None:
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()

    async with migration_engine.connect() as connection:
        transaction = await connection.begin()
        try:
            await connection.run_sync(downgrade)
            await connection.execute(
                text("""
                INSERT INTO execution_plane_outcome_history
                (event_id, client_id, project_id, work_item_id, request_id, state_revision,
                 status, result, completed_at, received_at, processed_at)
                VALUES (:event, 'ao', :project, :work, 'old-request', 1, 'completed',
                        '{"output": "archived"}', now(), now(), now())
                """),
                {"event": uuid4(), "project": uuid4(), "work": uuid4()},
            )
            await connection.execute(
                text("""
                INSERT INTO exchange_journal
                (entry_id, message_id, command_id, message_type, command_type, command_revision,
                 sequence, source_id, destination_id, client_id, all_projects, direction,
                 target_url, callback_url, envelope_hash, envelope_ciphertext, delivery_state,
                 handling_state, command_status, local_context_ciphertext, local_context_generation,
                 correlation_key, created_at, updated_at)
                VALUES (:entry, :message, :command, 'command.submit', 'work.execute', 1, 0,
                        'ao', 'ep', 'ao', true, 'outbound', 'https://ep.test', 'https://ao.test',
                        repeat('a',64), 'immutable-command', 'queued', 'idle', 'accepted',
                        'encrypted-temporal-context', 2, 'activity-correlation', now(), now())
                """),
                {"entry": uuid4(), "message": uuid4(), "command": uuid4()},
            )
            query = text("SELECT * FROM exchange_journal ORDER BY entry_id")
            before = (await connection.execute(query)).mappings().all()
            await connection.run_sync(upgrade)
            assert "execution_plane_outcome_history" not in await connection.run_sync(
                lambda conn: inspect(conn).get_table_names()
            )
            assert (await connection.execute(query)).mappings().all() == before

            await connection.run_sync(downgrade)
            assert await connection.scalar(text("SELECT count(*) FROM execution_plane_outcome_history")) == 0
            assert (await connection.execute(query)).mappings().all() == before
        finally:
            await transaction.rollback()
