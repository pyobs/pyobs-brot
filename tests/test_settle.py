"""Unit tests for the shared wait_until_settled() helper (staleness detection + resend)."""

import pytest
from pyobs.utils import exceptions as exc

from pyobs_brot._settle import wait_until_settled


class _FakeTransport:
    def __init__(self, connected: bool = True, age: float | None = 0.0) -> None:
        self.connected = connected
        self._age = age

    def telemetry_age(self) -> float | None:
        return self._age


@pytest.mark.asyncio
async def test_wait_until_settled_returns_when_condition_true() -> None:
    transport = _FakeTransport(connected=True, age=0.0)
    calls = 0

    def condition() -> bool:
        nonlocal calls
        calls += 1
        return calls > 3

    await wait_until_settled(condition, transport, poll_interval=0.001, stale_after=5.0)  # type: ignore[arg-type]
    assert calls > 3


@pytest.mark.asyncio
async def test_wait_until_settled_raises_when_disconnected() -> None:
    transport = _FakeTransport(connected=False, age=0.0)
    with pytest.raises(exc.MoveError):
        await wait_until_settled(lambda: False, transport, poll_interval=0.001)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_wait_until_settled_raises_when_telemetry_stale() -> None:
    transport = _FakeTransport(connected=True, age=10.0)
    with pytest.raises(exc.MoveError):
        await wait_until_settled(lambda: False, transport, poll_interval=0.001, stale_after=5.0)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_wait_until_settled_raises_when_no_telemetry_ever_received() -> None:
    transport = _FakeTransport(connected=True, age=None)
    with pytest.raises(exc.MoveError):
        await wait_until_settled(lambda: False, transport, poll_interval=0.001)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_wait_until_settled_resends_at_interval_not_before() -> None:
    transport = _FakeTransport(connected=True, age=0.0)
    resend_calls = 0
    condition_calls = 0

    async def resend() -> None:
        nonlocal resend_calls
        resend_calls += 1

    def condition() -> bool:
        nonlocal condition_calls
        condition_calls += 1
        return condition_calls > 60

    await wait_until_settled(
        condition,
        transport,  # type: ignore[arg-type]
        resend=resend,
        resend_interval=0.05,
        stale_after=5.0,
        poll_interval=0.005,
    )
    assert resend_calls >= 3


@pytest.mark.asyncio
async def test_wait_until_settled_no_resend_if_settles_quickly() -> None:
    transport = _FakeTransport(connected=True, age=0.0)
    resend_calls = 0
    calls = 0

    async def resend() -> None:
        nonlocal resend_calls
        resend_calls += 1

    def condition() -> bool:
        nonlocal calls
        calls += 1
        return calls > 2

    await wait_until_settled(
        condition,
        transport,  # type: ignore[arg-type]
        resend=resend,
        resend_interval=10.0,
        poll_interval=0.001,
    )
    assert resend_calls == 0
