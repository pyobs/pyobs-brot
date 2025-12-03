import asyncio
import logging
from typing import Any

from pyobs.events import RoofOpenedEvent, RoofClosingEvent
from pyobs.interfaces import IDome, IRoof, IMotion
from pyobs.modules.roof.basedome import BaseDome
from pyobs.modules import timeout
from pyobs.utils.enums import MotionStatus

from pybrotlib import BROT
from pybrotlib.mqtttransport import MQTTTransport
from pybrotlib.dome import DomeStatus, DomeShutterStatus

log = logging.getLogger(__name__)


class BrotDome(BaseDome, IDome):

    def __init__(
        self,
        host: str,
        name: str,
        port: int = 1883,
        keepalive: int = 60,
        **kwargs: Any,
    ):
        BaseDome.__init__(self, **kwargs)
        self.mqtt = MQTTTransport(host, port)
        self.brot = BROT(self.mqtt, name)
        # add thread for pulling the status constantly
        self.add_background_task(self._update_status)

    async def open(self) -> None:
        await BaseDome.open(self)
        asyncio.create_task(self.mqtt.run())
        await asyncio.sleep(2)
        await self.comm.register_event(RoofOpenedEvent)
        await self.comm.register_event(RoofClosingEvent)
        # check whats up
        if self.brot.dome.status == DomeStatus.ERROR:
            await self._error_state()
        elif self.brot.dome.in_motion:
            await self._change_motion_status(MotionStatus.SLEWING)
            log.info("Dome is already in motion. Please make sure it is not used by another instance!")
        elif self.brot.dome.status == DomeStatus.PARKED and self.brot.dome.shutter == DomeShutterStatus.CLOSED:
            await self._change_motion_status(MotionStatus.PARKED)
            log.info("Dome is closed and parked.")
        else:
            await self._change_motion_status(MotionStatus.POSITIONED)
            log.info("Dome is already online. Please make sure it is not used by another instance!")

    async def get_altaz(self, **kwargs: Any) -> tuple[float, float]:
        """Returns current Alt and Az.

        Returns:
            Tuple of current Alt and Az in degrees.
        """
        return 0.0, self.brot.dome.azimuth

    async def _update_status(self) -> None:
        while True:
            try:
                current_state = await self.get_motion_status()
                new_state = current_state
                # first two can only be updated by the init/park method
                if current_state == MotionStatus.INITIALIZING:
                    pass
                elif current_state == MotionStatus.PARKING:
                    pass
                elif self.brot.dome.status == DomeStatus.ERROR:
                    new_state = MotionStatus.ERROR
                elif self.brot.dome.status == DomeStatus.PARKED and self.brot.dome.shutter == DomeShutterStatus.CLOSED:
                    new_state = MotionStatus.PARKED
                elif self.brot.dome.in_motion:
                    new_state = MotionStatus.SLEWING
                else:
                    new_state = MotionStatus.POSITIONED
                if new_state != current_state:
                    await self._change_motion_status(new_state)
            except:
                pass
            await asyncio.sleep(1)

    @timeout(300)
    async def init(self, **kwargs: Any) -> None:
        log.info("Opening roof")
        if self.brot.dome.shutter == DomeShutterStatus.OPEN:
            return
        elif self.brot.dome.status == DomeStatus.ERROR:
            await self._error_state("Dome is in error state. Cannot open.")
            return

        await self._change_motion_status(MotionStatus.INITIALIZING)

        # send open command
        await self.brot.dome.open()

        while True:
            match self.brot.dome.shutter:
                case DomeShutterStatus.OPEN:
                    log.info("Dome is open.")
                    break
                case _:
                    pass
            await asyncio.sleep(1)
        # send tracking command
        await self.brot.dome.start_tracking()
        while True:
            match self.brot.dome.status:
                case DomeStatus.TRACKING:
                    log.info("Dome is tracking the telescope azimuth.")
                    break
                case DomeStatus.ERROR:
                    await self._error_state()
                    return
                case _:
                    pass
            await asyncio.sleep(1)
        await self._change_motion_status(MotionStatus.POSITIONED)
        await self.comm.send_event(RoofOpenedEvent())

    @timeout(300)
    async def park(self, **kwargs: Any) -> None:
        if self.brot.dome.status == DomeStatus.PARKED and self.brot.some.shutter == DomeShutterStatus.CLOSED:
            return
        elif self.brot.dome.status == DomeStatus.ERROR:
            await self._error_state("Dome is in error state. Cannot close/park.")
            return

        await self._change_motion_status(MotionStatus.PARKING)
        await self.comm.send_event(RoofClosingEvent())
        # stop tracking
        await self.brot.dome.stop_tracking()
        while True:
            match self.brot.dome.status:
                case DomeStatus.TRACKING:
                    pass
                case DomeStatus.ERROR:
                    await self._error_state()
                    return
                case _:
                    break
            await asyncio.sleep(1)
        # close shutter
        await self.brot.dome.close()
        while True:
            match self.brot.dome.shutter:
                case DomeShutterStatus.CLOSED:
                    log.info("Dome shutter is closed.")
                    break
                case _:
                    pass
            await asyncio.sleep(1)

        # go to parking position
        await self.brot.dome.park()
        while True:
            match self.brot.dome.status:
                case DomeStatus.PARKED:
                    log.info("Dome is parked.")
                    break
                case DomeStatus.ERROR:
                    await self._error_state()
                    return
                case _:
                    pass
            await asyncio.sleep(1)
        await self._change_motion_status(MotionStatus.PARKED)

    async def stop_motion(self, device: str | None = None, **kwargs: Any) -> None:
        pass  # no stopping of the roof possible

    async def move_altaz(self, alt: float, az: float, **kwargs: Any) -> None:
        """Moves to given coordinates.

        Args:
            alt: Alt in deg to move to.
            az: Az in deg to move to.

        Raises:
            MoveError: If device could not be moved.
        """
        pass

    async def _error_state(self, mess: str = "Dome is in error state.") -> None:
        log.error(mess)
        await self._change_motion_status(MotionStatus.ERROR)


__all__ = ["BrotDome"]
