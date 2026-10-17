"""The PLC owns startup completion; the driver waits through both pending states."""

import asyncio
import time
from typing import cast
from unittest.mock import AsyncMock

import pytest
from pyobs.interfaces import IReady
from pyobs.utils import exceptions as exc
from pyobs.utils.enums import MotionStatus

from pyobs_brot import BrotRaDecTelescope


def telescope() -> BrotRaDecTelescope:
    scope = BrotRaDecTelescope(host="localhost", name="test")
    scope.brot.telescope.power_on = AsyncMock()
    scope._error_state = AsyncMock()
    scope.mqtt._connected = True
    scope.mqtt._last_message_at = time.monotonic()
    return scope


@pytest.mark.asyncio
async def test_init_waits_through_preparation_calibration_and_positioning(monkeypatch: pytest.MonkeyPatch) -> None:
    scope = telescope()
    readings = iter((0.7, 0.8, 0.8, 1.0))
    observed = []

    async def advance(_seconds: float) -> None:
        observed.append(scope.brot.telescope._telemetry.TELESCOPE.READY_STATE)
        scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = next(readings)

    monkeypatch.setattr("pyobs_brot.brottelescope.asyncio.sleep", advance)
    await scope.init()
    assert observed == [0.0, 0.7, 0.8, 0.8]
    cast(AsyncMock, scope.brot.telescope.power_on).assert_awaited_once()


@pytest.mark.asyncio
async def test_calibration_failure_reaches_init_caller(monkeypatch: pytest.MonkeyPatch) -> None:
    scope = telescope()
    scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = 0.8

    async def fail(_seconds: float) -> None:
        scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = -1.0

    monkeypatch.setattr("pyobs_brot.brottelescope.asyncio.sleep", fail)
    with pytest.raises(exc.InitError, match="Error during powerup"):
        await scope.init()
    cast(AsyncMock, scope._error_state).assert_awaited_once()


@pytest.mark.asyncio
async def test_cancellation_while_calibrating_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    scope = telescope()
    scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = 0.8

    async def cancel(_seconds: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr("pyobs_brot.brottelescope.asyncio.sleep", cancel)
    with pytest.raises(asyncio.CancelledError):
        await scope.init()
    cast(AsyncMock, scope._error_state).assert_not_awaited()


@pytest.mark.asyncio
async def test_calibrating_publishes_initializing_and_not_ready() -> None:
    scope = telescope()
    scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = 0.8
    scope._change_motion_status = AsyncMock()
    scope.comm.set_state = AsyncMock()

    await scope._update()

    scope._change_motion_status.assert_awaited_once_with(MotionStatus.INITIALIZING)
    ready_states = [call.args[1].ready for call in scope.comm.set_state.await_args_list if call.args[0] is IReady]
    assert ready_states == [False]


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown_state", [-2.0, 0.8000000001, 0.99])
async def test_unknown_status_clears_previous_ready_state(unknown_state: float) -> None:
    scope = telescope()
    scope._change_motion_status = AsyncMock()
    scope.comm.set_state = AsyncMock()
    scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = 1.0
    await scope._update()

    scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = unknown_state
    await scope._update()

    scope._change_motion_status.assert_awaited_with(MotionStatus.UNKNOWN)
    ready_states = [call.args[1].ready for call in scope.comm.set_state.await_args_list if call.args[0] is IReady]
    assert ready_states == [True, False]


@pytest.mark.asyncio
async def test_init_joins_running_startup_without_new_power_on(monkeypatch: pytest.MonkeyPatch) -> None:
    scope = telescope()
    scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = 0.8

    async def finish(_seconds: float) -> None:
        scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = 1.0

    monkeypatch.setattr("pyobs_brot.brottelescope.asyncio.sleep", finish)
    await scope.init()
    cast(AsyncMock, scope.brot.telescope.power_on).assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("aborted", [-2.0, 0.0, 0.3])
async def test_aborted_startup_fails_init(monkeypatch: pytest.MonkeyPatch, aborted: float) -> None:
    scope = telescope()
    readings = iter((0.7, 0.8, aborted))

    async def advance(_seconds: float) -> None:
        scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = next(readings)

    monkeypatch.setattr("pyobs_brot.brottelescope.asyncio.sleep", advance)
    with pytest.raises(exc.InitError, match="interrupted"):
        await scope.init()


@pytest.mark.asyncio
async def test_stale_not_operational_value_before_startup_is_not_an_abort(monkeypatch: pytest.MonkeyPatch) -> None:
    scope = telescope()
    scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = -2.0
    readings = iter((-2.0, 0.7, 1.0))

    async def advance(_seconds: float) -> None:
        scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = next(readings)

    monkeypatch.setattr("pyobs_brot.brottelescope.asyncio.sleep", advance)
    await scope.init()
    cast(AsyncMock, scope.brot.telescope.power_on).assert_awaited_once()


@pytest.mark.asyncio
async def test_init_after_transient_interlock_waits_for_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    # -2 before any pending state is stale or an interlock, not an abort: init must keep waiting for 1.0.
    # The PLC ignores the power_on request while its startup is still complete, so no second calibration runs.
    scope = telescope()
    scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = -2.0
    readings = iter((-2.0, -2.0, 1.0))

    async def advance(_seconds: float) -> None:
        scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = next(readings)

    monkeypatch.setattr("pyobs_brot.brottelescope.asyncio.sleep", advance)
    await scope.init()
    cast(AsyncMock, scope.brot.telescope.power_on).assert_awaited_once()
    cast(AsyncMock, scope._error_state).assert_not_awaited()


@pytest.mark.asyncio
async def test_init_when_already_online_does_not_power_on() -> None:
    scope = telescope()
    scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = 1.0
    await scope.init()
    cast(AsyncMock, scope.brot.telescope.power_on).assert_not_awaited()


@pytest.mark.asyncio
async def test_init_with_error_state_refuses_power_on() -> None:
    scope = telescope()
    scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = -1.0
    with pytest.raises(exc.InitError, match="has errors"):
        await scope.init()
    cast(AsyncMock, scope.brot.telescope.power_on).assert_not_awaited()
