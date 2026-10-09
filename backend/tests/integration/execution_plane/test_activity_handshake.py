"""Work intent and private Temporal context share one durable journal record."""

from uuid import uuid4, uuid5

import pytest
from execution_plane_client.errors import ProtocolConflict
from sqlalchemy import func, select

from syntara.execution_plane.activity_context import TemporalIdentity
from syntara.execution_plane.commands.activities import enqueue_script, enqueue_workflow_cancellations


@pytest.mark.asyncio
async def test_context_and_command_rollback_together(temporal_case):
    async with temporal_case.sessions() as session:
        await temporal_case.enqueue(session)
        await session.rollback()
    async with temporal_case.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(temporal_case.journal.schema.table)) == 0


@pytest.mark.asyncio
async def test_retry_reuses_first_envelope_and_only_refreshes_token(temporal_case):
    first = await temporal_case.submit(attempt=1)
    temporal_case.payload["image"] = "changed-after-first-enqueue"
    second = await temporal_case.submit(attempt=2)
    assert first == second
    assert (await temporal_case.activity_context()).attempt == 2
    assert (await temporal_case.local_context()).generation == 2
    assert temporal_case.temporal.calls == []


@pytest.mark.asyncio
async def test_cancellation_before_delivery_is_durable_and_rollbackable(temporal_case):
    original = await temporal_case.submit()
    async with temporal_case.sessions() as session:
        count = await enqueue_workflow_cancellations(session, "workflow", runtime=temporal_case.runtime)
        assert count == 1
        await session.rollback()
    async with temporal_case.sessions() as session:
        assert (
            await temporal_case.journal.get(
                session, temporal_case.scope, uuid5(original.command_id, "cancel"), "outbound"
            )
            is None
        )
    async with temporal_case.sessions() as session, session.begin():
        assert await enqueue_workflow_cancellations(session, "workflow", runtime=temporal_case.runtime) == 1
    async with temporal_case.sessions() as session, session.begin():
        assert await enqueue_workflow_cancellations(session, "workflow", runtime=temporal_case.runtime) == 0
        entries = (await session.execute(select(temporal_case.journal.schema.table))).mappings().all()
        assert len(entries) == 2
        cancel = next(entry for entry in entries if entry["command_type"] == "command.cancel")
        message = await temporal_case.journal.load_message(session, cancel["entry_id"])
        assert message.command.payload["target_command_id"] == str(original.command_id)
    assert temporal_case.temporal.calls == []


@pytest.mark.asyncio
async def test_command_identity_cannot_be_rebound_to_another_project(temporal_case):
    await temporal_case.submit()
    async with temporal_case.sessions() as session, session.begin():
        with pytest.raises(ProtocolConflict):
            await enqueue_script(
                session,
                command_id=temporal_case.command_id,
                project_id=uuid4(),
                temporal_identity=TemporalIdentity(
                    namespace="default", workflow_id="workflow", run_id="run", activity_id="script"
                ),
                task_token=b"token",
                activity_attempt=1,
                payload=temporal_case.payload,
                runtime=temporal_case.runtime,
            )


@pytest.mark.asyncio
async def test_temporal_context_is_encrypted_and_absent_from_wire(temporal_case):
    import base64

    from syntara.execution_plane.activity_context import workflow_correlation_key

    message = await temporal_case.submit()
    local = await temporal_case.local_context()
    async with temporal_case.sessions() as session:
        rows = (await session.execute(select(temporal_case.journal.schema.table))).mappings().all()
    assert len(rows) == 1
    assert "token-1" not in str(rows)
    assert base64.b64encode(b"token-1").decode() not in str(rows)
    assert "token" not in message.model_dump_json()
    assert "task_token" not in repr(local)
    assert rows[0]["local_context_ciphertext"]
    assert rows[0]["correlation_key"] == workflow_correlation_key("workflow")
    assert set(local.data) == {"identity", "attempt", "task_token"}


@pytest.mark.asyncio
async def test_cancellation_skips_completed_effect_even_after_newer_duplicate(temporal_case):
    await temporal_case.receive_completion()
    await temporal_case.pump()
    await temporal_case.receive_completion(sequence=2)
    await temporal_case.pump()
    async with temporal_case.sessions() as session, session.begin():
        assert await enqueue_workflow_cancellations(session, "workflow", runtime=temporal_case.runtime) == 0


@pytest.mark.asyncio
async def test_workflow_cancellation_paginates_and_keeps_other_workflows_separate(temporal_case, monkeypatch):
    from syntara.execution_plane.commands.activities import stable_command_id

    for index in range(4):
        identity = temporal_case.identity.model_copy(update={"activity_id": f"script-{index}"})
        if index == 3:
            identity.workflow_id = "other-workflow"
        async with temporal_case.sessions() as session, session.begin():
            await enqueue_script(
                session,
                command_id=stable_command_id(identity),
                project_id=temporal_case.project_id,
                temporal_identity=identity,
                task_token=b"private-token",
                activity_attempt=1,
                payload=temporal_case.payload,
                runtime=temporal_case.runtime,
            )
    list_entries = temporal_case.journal.list_local_context_entries
    cursors = []

    async def small_pages(session, **kwargs: object) -> object:
        cursors.append(kwargs["after"])
        return await list_entries(session, **{**kwargs, "limit": 2})

    monkeypatch.setattr(temporal_case.journal, "list_local_context_entries", small_pages)
    async with temporal_case.sessions() as session, session.begin():
        assert await enqueue_workflow_cancellations(session, "workflow", runtime=temporal_case.runtime) == 3
        assert await enqueue_workflow_cancellations(session, "workflow", runtime=temporal_case.runtime) == 0
        rows = (await session.execute(select(temporal_case.journal.schema.table))).mappings().all()
        assert len([row for row in rows if row["command_type"] == "command.cancel"]) == 3
    assert cursors[0] is None
    assert cursors[1] is not None
