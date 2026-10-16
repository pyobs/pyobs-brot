"""The PLC owns startup completion; the driver waits through both pending states."""

import asyncio
import time
from typing import cast
from unittest.mock import AsyncMock, PropertyMock, patch

import pytest
from pybrotlib.components.telescope import TelescopeStatus
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
async def test_calibrating_with_older_pybrotlib_unknown_status() -> None:
    scope = telescope()
    scope.brot.telescope._telemetry.TELESCOPE.READY_STATE = 0.8
    scope._change_motion_status = AsyncMock()
    scope.comm.set_state = AsyncMock()
    with patch.object(
        type(scope.brot.telescope), "status", new_callable=PropertyMock, return_value=TelescopeStatus.UNKNOWN
    ):
        await scope._update()
    scope._change_motion_status.assert_awaited_once_with(MotionStatus.INITIALIZING)
    assert any(call.args[0] is IReady and not call.args[1].ready for call in scope.comm.set_state.await_args_list)


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
