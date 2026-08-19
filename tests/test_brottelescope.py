"""Unit tests for the non-hardware logic in the BROT telescope drivers: constructor
state, focus-offset composition, and the RA/Dec offset scaling + convergence wait.

The BROT/MQTT backend (self.brot) is mocked out; hardware I/O itself is out of scope.
"""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pyobs.interfaces import IFocuser, IOffsetsRaDec

from pyobs_brot import BrotRaDecTelescope


def test_constructor_defaults() -> None:
    telescope = BrotRaDecTelescope(host="localhost", name="telescope")
    assert telescope.temperatures == {}
    assert telescope.focus_offset == 0.0
    assert telescope._roof == "None"


def test_constructor_threads_kwargs_cooperatively() -> None:
    """Regression test for the cooperative-super()-chain fix: BrotBaseTelescope used to call
    BaseTelescope.__init__() then a separate, redundant FitsNamespaceMixin.__init__(self,
    **kwargs) -- BaseTelescope's own cooperative chain already reaches FitsNamespaceMixin (it's
    the last base), so the second call re-threads the same kwargs (location, comm, ...) through
    a chain that's now cooperative all the way to object.__init__(), which raises on any
    leftover. This is exactly the shape deployed in pyobs-monet's telescope configs (host, name,
    fits_headers, temperatures, comm, location, timezone all set)."""
    telescope = BrotRaDecTelescope(
        host="localhost",
        name="telescope",
        fits_headers={"TEL-FOCL": (8400.0, "Focal length")},
        temperatures={"M1": "TEMP_M1"},
        location={"longitude": 20.81, "latitude": -32.37, "elevation": 1798.0},
        timezone="Africa/Johannesburg",
    )
    assert telescope.motion_status().name == "UNKNOWN"
    assert telescope.temperatures == {"M1": "TEMP_M1"}


def test_constructor_temperatures_and_roof() -> None:
    telescope = BrotRaDecTelescope(
        host="localhost",
        name="telescope",
        temperatures={"ccd": "T1"},
        roof="some-roof",
    )
    assert telescope.temperatures == {"ccd": "T1"}
    assert telescope._roof == "some-roof"


@pytest.mark.asyncio
async def test_set_focus_offset_composes_and_publishes() -> None:
    telescope = BrotRaDecTelescope(host="localhost", name="telescope")
    telescope.brot.focus = MagicMock()
    telescope.brot.focus.position = 10.0
    telescope.brot.focus.set = AsyncMock()
    telescope._wait_for_focus = AsyncMock()  # type: ignore[method-assign]
    telescope.comm.set_state = AsyncMock()  # type: ignore[method-assign]

    await telescope.set_focus_offset(0.25)

    # the hardware call adds the offset onto the current position...
    telescope.brot.focus.set.assert_awaited_once_with(10.25)
    assert telescope.focus_offset == 0.25

    # ...and the published focus subtracts the offset back out of the new position
    assert telescope.comm.set_state.await_args is not None
    interface, state = telescope.comm.set_state.await_args.args
    assert interface is IFocuser
    assert (state.focus, state.focus_offset) == (9.75, 0.25)


def _telemetry(ha_dist: float, dec_dist: float) -> Any:
    return SimpleNamespace(
        POSITION=SimpleNamespace(
            INSTRUMENTAL=SimpleNamespace(
                HA=SimpleNamespace(TARGETDISTANCE=ha_dist),
                DEC=SimpleNamespace(TARGETDISTANCE=dec_dist),
            )
        )
    )


@pytest.mark.asyncio
async def test_set_offsets_radec_sends_scaled_and_sign_flipped_offsets() -> None:
    telescope = BrotRaDecTelescope(host="localhost", name="telescope")
    telescope.brot.telescope.set_offset_ha = AsyncMock()  # type: ignore[method-assign]
    telescope.brot.telescope.set_offset_dec = AsyncMock()  # type: ignore[method-assign]
    telescope.brot.telescope._telemetry = _telemetry(0.0, 0.0)  # already converged
    telescope.comm.set_state = AsyncMock()  # type: ignore[method-assign]

    await telescope.set_offsets_radec(0.001, 0.002)

    # HA is sign-flipped (RA east is HA west) and both are scaled deg -> arcsec
    telescope.brot.telescope.set_offset_ha.assert_awaited_once_with(-3.6)
    telescope.brot.telescope.set_offset_dec.assert_awaited_once_with(7.2)
    assert telescope.comm.set_state.await_args is not None
    interface, state = telescope.comm.set_state.await_args.args
    assert interface is IOffsetsRaDec
    assert (state.ra, state.dec) == (0.001, 0.002)


@pytest.mark.asyncio
async def test_set_offsets_radec_waits_until_both_axes_converge(monkeypatch: pytest.MonkeyPatch) -> None:
    telescope = BrotRaDecTelescope(host="localhost", name="telescope")
    telescope.brot.telescope.set_offset_ha = AsyncMock()  # type: ignore[method-assign]
    telescope.brot.telescope.set_offset_dec = AsyncMock()  # type: ignore[method-assign]
    telescope.comm.set_state = AsyncMock()  # type: ignore[method-assign]

    # still slewing for two polls (HA, then DEC, still off-target), converged on the third
    readings = [(1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]
    telescope.brot.telescope._telemetry = _telemetry(*readings[0])

    sleep_calls = 0

    async def fake_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        telescope.brot.telescope._telemetry = _telemetry(*readings[sleep_calls])

    monkeypatch.setattr("pyobs_brot.brottelescope.asyncio.sleep", fake_sleep)

    await telescope.set_offsets_radec(0.001, 0.002)

    assert sleep_calls == 2
