"""Shared ingress commits before responding under AO certificate middleware."""

import pytest


@pytest.mark.asyncio
async def test_receipt_requires_both_cert_and_assertion(ao_http_case):
    response = await ao_http_case.post_valid_message(cert_authenticated=False)
    assert response.status_code == 403
    assert await ao_http_case.journal_count() == 0
    response = await ao_http_case.post_valid_message(signed=False)
    assert response.status_code == 401
    assert await ao_http_case.journal_count() == 0


@pytest.mark.asyncio
async def test_wrong_certificate_and_scope_are_rejected(ao_http_case):
    response = await ao_http_case.post_valid_message(certificate_name="untrusted.service")
    assert response.status_code == 403
    response = await ao_http_case.post_valid_message(wrong_scope=True)
    assert response.status_code in {401, 403}
    assert await ao_http_case.journal_count() == 0


@pytest.mark.asyncio
async def test_commit_before_202_duplicate_receipt_and_one_temporal_delivery(ao_http_case):
    first = await ao_http_case.post_valid_message()
    assert first.status_code == 202
    assert await ao_http_case.journal_count() == 1
    duplicate = await ao_http_case.post_valid_message()
    assert duplicate.status_code == 202
    assert duplicate.json()["duplicate"]
    await ao_http_case.case.pump()
    await ao_http_case.case.pump()
    assert len(ao_http_case.case.temporal.calls) == 1


@pytest.mark.asyncio
async def test_commit_failure_cannot_acknowledge_receipt(ao_http_case):
    response = await ao_http_case.post_valid_message(commit_failure=True)
    assert response.status_code == 500
    assert await ao_http_case.journal_count() == 0
