"""Unit tests for the roof-status -> motion-status mapping in BrotRoof.

The BROT/MQTT backend is mocked out; only the state mapping is exercised.
"""

from unittest.mock import MagicMock

import pytest
from pybrotlib.components.roof import RoofStatus
from pyobs.utils.enums import MotionStatus

from pyobs_brot import BrotRoof


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (RoofStatus.ERROR, MotionStatus.ERROR),
        (RoofStatus.CLOSED, MotionStatus.PARKED),
        (RoofStatus.OPENING, MotionStatus.INITIALIZING),
        (RoofStatus.CLOSING, MotionStatus.PARKING),
        (RoofStatus.OPEN, MotionStatus.POSITIONED),
        (RoofStatus.STOPPED, MotionStatus.IDLE),
    ],
)
@pytest.mark.asyncio
async def test_update_status_mapping(status: RoofStatus, expected: MotionStatus) -> None:
    roof = BrotRoof(host="localhost", name="roof")
    roof.brot.roof = MagicMock()
    roof.brot.roof.status = status

    await roof._update_status()

    assert roof.motion_status() == expected


@pytest.mark.asyncio
async def test_update_status_plc_offline_sets_error() -> None:
    roof = BrotRoof(host="localhost", name="roof")
    roof.brot.roof = MagicMock()
    roof.brot.roof.status = RoofStatus.OPEN  # frozen telemetry
    roof.mqtt.plc_online = False

    await roof._update_status()

    assert roof.motion_status() == MotionStatus.ERROR


@pytest.mark.asyncio
async def test_update_status_plc_back_online_recovers() -> None:
    roof = BrotRoof(host="localhost", name="roof")
    roof.brot.roof = MagicMock()
    roof.brot.roof.status = RoofStatus.OPEN
    roof.mqtt.plc_online = False
    await roof._update_status()

    roof.mqtt.plc_online = True
    await roof._update_status()

    assert roof.motion_status() == MotionStatus.POSITIONED
