"""Workflow test peers exchange real durable messages on both sides."""

import pytest
from sqlalchemy import func, select

from tests.fixtures.fake_execution_plane import FakeExecutionPlanePeer


@pytest.mark.asyncio
async def test_fake_peer_persists_acceptance_and_delivers_completion(
    temporal_case,
    test_db_admin_url,
    test_db_engine,
):
    peer = await FakeExecutionPlanePeer.create(
        temporal_case.sessions,
        temporal_case.temporal,
        test_db_admin_url.set(database=test_db_engine.url.database),
    )
    temporal_case.runtime = peer.ao
    try:
        original = await temporal_case.submit()
        assert temporal_case.temporal.calls == []
        for _ in range(5):
            await peer.pump()
        assert len(temporal_case.temporal.calls) == 1
        assert temporal_case.temporal.calls[0][2]["output"]["stdout"] == "hello\n"
        async with peer.ep.sessions() as session:
            table = peer.ep.journal.schema.table
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(table)
                    .where(
                        table.c.command_id == original.command_id,
                        table.c.direction == "inbound",
                    )
                )
                == 1
            )
        assert (await temporal_case.effect_entry()).handling_state == "handled"
    finally:
        await peer.close()
