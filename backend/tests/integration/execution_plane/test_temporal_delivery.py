"""Temporal completion responsibility survives handoff races and uncertain RPCs."""

import pytest


@pytest.mark.asyncio
async def test_old_rpc_cannot_finish_refreshed_activity(temporal_case):
    effect = await temporal_case.prepare_completion(attempt=1)
    await temporal_case.retry_activity(attempt=2)
    await temporal_case.finish_success(effect)
    activity = await temporal_case.activity_context()
    assert activity.attempt == 2
    assert (await temporal_case.effect_entry()).handling_state != "handled"
    await temporal_case.pump()
    assert temporal_case.temporal.calls[-1][0] == b"token-2"
    assert (await temporal_case.effect_entry()).handling_state == "handled"


@pytest.mark.asyncio
async def test_early_completion_defers_until_temporal_observes_current_attempt(temporal_case):
    await temporal_case.prepare_completion()
    temporal_case.temporal.ready = False
    await temporal_case.pump()
    assert temporal_case.temporal.calls == []
    temporal_case.temporal.ready = True
    await temporal_case.make_due()
    await temporal_case.pump()
    assert len(temporal_case.temporal.calls) == 1
    assert (await temporal_case.effect_entry()).handling_state == "handled"


@pytest.mark.asyncio
async def test_ambiguous_rpc_is_observed_without_blind_replay(temporal_case):
    await temporal_case.receive_completion()
    temporal_case.temporal.ambiguous = True
    await temporal_case.pump()
    assert (await temporal_case.effect_entry()).handling_state == "reconciling"
    assert len(temporal_case.temporal.calls) == 1
    await temporal_case.make_due()
    await temporal_case.pump()
    assert len(temporal_case.temporal.calls) == 1
    assert (await temporal_case.effect_entry()).handling_state == "reconciling"


@pytest.mark.asyncio
async def test_definitive_dispatch_rejection_fails_waiting_temporal_activity(temporal_case):
    await temporal_case.submit()
    await temporal_case.reject_submission()
    await temporal_case.pump()
    assert temporal_case.temporal.calls[0][1] == "fail"
    assert temporal_case.temporal.calls[0][2].type == "ExecutionPlaneRejected"
    assert (await temporal_case.effect_entry()).handling_state == "handled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        ("cancelled", "ExecutionPlaneWorkCancelled"),
        ("failed", "ScriptExecutionError"),
        ("outcome_unknown", "ExecutionPlaneOutcomeUnknown"),
    ],
)
async def test_failure_outcomes_are_non_retryable(temporal_case, status, error_type):
    await temporal_case.receive_completion(status=status)
    await temporal_case.pump()
    error = temporal_case.temporal.calls[0][2]
    assert error.type == error_type
    assert error.non_retryable


@pytest.mark.asyncio
async def test_reconcile_without_history_keeps_domain_obligation_explicit(temporal_case):
    effect = await temporal_case.prepare_completion()
    result = await temporal_case.handler.reconcile(effect)
    assert result is not None
    assert result.outcome == "unknown"
    async with temporal_case.sessions() as session, session.begin():
        finished = await temporal_case.handler.finish(effect, result, temporal_case.context, session)
    assert finished.status == "outcome_unknown"
    assert finished.error_code == "temporal_rpc_uncertain"
    assert temporal_case.temporal.calls == []


@pytest.mark.asyncio
async def test_matching_temporal_history_resolves_lost_response(temporal_case):
    from temporalio.api.common.v1 import Payloads
    from temporalio.api.history.v1 import (
        ActivityTaskCompletedEventAttributes,
        ActivityTaskScheduledEventAttributes,
        ActivityTaskStartedEventAttributes,
        HistoryEvent,
    )

    await temporal_case.receive_completion()
    temporal_case.temporal.ambiguous = True
    await temporal_case.pump()
    payloads = await temporal_case.temporal.data_converter.encode([{"value": 42}])
    temporal_case.temporal.history = [
        HistoryEvent(
            event_id=1,
            activity_task_scheduled_event_attributes=ActivityTaskScheduledEventAttributes(activity_id="script"),
        ),
        HistoryEvent(
            event_id=2,
            activity_task_started_event_attributes=ActivityTaskStartedEventAttributes(scheduled_event_id=1, attempt=1),
        ),
        HistoryEvent(
            event_id=3,
            activity_task_completed_event_attributes=ActivityTaskCompletedEventAttributes(
                scheduled_event_id=1, started_event_id=2, result=Payloads(payloads=payloads)
            ),
        ),
    ]
    await temporal_case.make_due()
    await temporal_case.pump()
    assert (await temporal_case.effect_entry()).handling_state == "handled"
    assert len(temporal_case.temporal.calls) == 1


@pytest.mark.asyncio
async def test_wire_valid_large_completion_prepares_with_private_token_context(temporal_case, monkeypatch):
    from execution_plane_client.crypto import canonical_bytes
    from execution_plane_client.protocol import MAX_PAYLOAD_BYTES, MessageEnvelope

    original_receive = temporal_case.runtime.ingress.receive

    async def receive_large(message, identity, session) -> object:
        values = message.model_dump(mode="json")
        values["command"]["payload"]["result"] = {"result": {"output": ""}}
        empty = MessageEnvelope.model_validate(values)
        values["command"]["payload"]["result"]["result"]["output"] = "x" * (
            MAX_PAYLOAD_BYTES - len(canonical_bytes(empty)) - 10
        )
        large = MessageEnvelope.model_validate(values)
        assert len(canonical_bytes(large)) < MAX_PAYLOAD_BYTES
        return await original_receive(large, identity, session)

    monkeypatch.setattr(temporal_case.runtime.ingress, "receive", receive_large)
    await temporal_case.receive_completion()
    # Identity and token metadata enlarge the private preparation past the wire limit.
    async with temporal_case.sessions() as session, session.begin():
        local = await temporal_case.journal.get_local_context(session, temporal_case.scope, temporal_case.command_id)
        activity = await temporal_case.activity_context()
        activity.identity.workflow_id = "w" * 250
        activity.identity.run_id = "r" * 250
        await temporal_case.journal.set_local_context(
            session,
            temporal_case.scope,
            temporal_case.command_id,
            data=activity.model_dump(mode="json"),
            correlation_key=local.correlation_key,
            expected_generation=local.generation,
        )
    await temporal_case.pump()
    assert (await temporal_case.effect_entry()).handling_state == "handled"
    assert len(temporal_case.temporal.calls) == 1
    assert len(temporal_case.temporal.calls[0][2]["output"]) > 500 * 1024


@pytest.mark.asyncio
async def test_oversized_result_reports_explicit_nonretryable_activity_failure(temporal_case, monkeypatch):
    from execution_plane_client.protocol import MessageEnvelope

    original_receive = temporal_case.runtime.ingress.receive

    async def receive_failure(message, identity, session) -> object:
        values = message.model_dump(mode="json")
        values["command"]["payload"]["error_code"] = "completion_result_too_large"
        return await original_receive(MessageEnvelope.model_validate(values), identity, session)

    monkeypatch.setattr(temporal_case.runtime.ingress, "receive", receive_failure)
    await temporal_case.receive_completion(status="failed")
    await temporal_case.pump()
    error = temporal_case.temporal.calls[0][2]
    assert error.type == "ExecutionPlaneResultTooLarge"
    assert error.non_retryable


@pytest.mark.asyncio
async def test_new_completion_after_success_does_not_repeat_rpc_and_latest_rearms_on_retry(temporal_case):
    await temporal_case.receive_completion(sequence=1)
    await temporal_case.pump()
    await temporal_case.receive_completion(sequence=2)
    await temporal_case.pump()
    assert len(temporal_case.temporal.calls) == 1
    assert (await temporal_case.effect_entry()).handling_state == "handled"
    await temporal_case.retry_activity(attempt=2)
    await temporal_case.pump()
    assert len(temporal_case.temporal.calls) == 2
    assert temporal_case.temporal.calls[-1][0] == b"token-2"
    assert (await temporal_case.effect_entry()).handling_state == "handled"


@pytest.mark.asyncio
async def test_new_completion_waits_for_uncertain_older_effect(temporal_case):
    first = await temporal_case.receive_completion(sequence=1)
    temporal_case.temporal.ambiguous = True
    await temporal_case.pump()
    await temporal_case.receive_completion(sequence=2)
    await temporal_case.pump()
    assert len(temporal_case.temporal.calls) == 1
    assert (await temporal_case.effect_entry()).handling_state == "deferred"
    async with temporal_case.sessions() as session:
        old = await temporal_case.journal.get(session, temporal_case.scope, first.command_id, "inbound")
        assert old.handling_state == "reconciling"
        assert old.reconciliation_required
    await temporal_case.make_due()
    await temporal_case.pump()
    assert len(temporal_case.temporal.calls) == 1


@pytest.mark.asyncio
async def test_rejected_dispatch_rearms_for_refreshed_token(temporal_case):
    await temporal_case.submit()
    await temporal_case.reject_submission()
    await temporal_case.pump()
    await temporal_case.retry_activity(attempt=2)
    await temporal_case.pump()
    assert [call[0] for call in temporal_case.temporal.calls] == [b"token-1", b"token-2"]
    assert all(call[1] == "fail" for call in temporal_case.temporal.calls)
    assert (await temporal_case.effect_entry()).handling_state == "handled"


@pytest.mark.asyncio
async def test_rpc_response_racing_token_refresh_cannot_finish_new_journal_claim(temporal_case, monkeypatch):
    await temporal_case.receive_completion()
    get_handle = temporal_case.temporal.get_async_activity_handle

    def refreshing_handle(*, task_token) -> object:
        handle = get_handle(task_token=task_token)
        complete = handle.complete

        async def refresh_then_complete(value, **kwargs: object) -> None:
            if task_token == b"token-1":
                await temporal_case.retry_activity(attempt=2)
            await complete(value, **kwargs)

        handle.complete = refresh_then_complete
        return handle

    monkeypatch.setattr(temporal_case.temporal, "get_async_activity_handle", refreshing_handle)
    await temporal_case.pump()
    assert (await temporal_case.effect_entry()).handling_state == "pending"
    await temporal_case.pump()
    assert (await temporal_case.effect_entry()).handling_state == "handled"
    assert [call[0] for call in temporal_case.temporal.calls] == [b"token-1", b"token-2"]
