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
