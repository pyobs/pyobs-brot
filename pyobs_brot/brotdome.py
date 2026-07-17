import asyncio
import logging
from typing import Any

from pybrotlib import BROT
from pybrotlib.components.dome import DomeShutterStatus, DomeStatus
from pybrotlib.transport import MQTTTransport
from pyobs.events import RoofClosingEvent, RoofOpenedEvent
from pyobs.interfaces import AltAzState, IDome, IPointingAltAz
from pyobs.modules import timeout
from pyobs.modules.roof.basedome import BaseDome
from pyobs.utils import exceptions as exc
from pyobs.utils.enums import MotionStatus

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
        self.add_background_task(self._update_status)

    async def open(self) -> None:
        await BaseDome.open(self)
        asyncio.create_task(self.mqtt.run())
        await asyncio.sleep(2)
        await self.comm.register_event(RoofOpenedEvent)
        await self.comm.register_event(RoofClosingEvent)
        await self.comm.set_state(IPointingAltAz, AltAzState(alt=0.0, az=self.brot.dome.azimuth))
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

    async def _update_status(self) -> None:
        while True:
            try:
                current_state = self.motion_status()
                new_state = current_state
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
                await self.comm.set_state(IPointingAltAz, AltAzState(alt=0.0, az=self.brot.dome.azimuth))
            except asyncio.CancelledError:
                return
            except Exception:
                pass
            await asyncio.sleep(1)

    @timeout(300)
    async def init(self, **kwargs: Any) -> None:
        log.info("Opening roof")
        if self.brot.dome.shutter == DomeShutterStatus.OPEN:
            return
        elif self.brot.dome.status == DomeStatus.ERROR:
            await self._error_state("Dome is in error state. Cannot open.")
            raise exc.InitError("Dome is in error state. Cannot open.")

        await self._change_motion_status(MotionStatus.INITIALIZING)

        await self.brot.dome.open()
        while True:
            match self.brot.dome.shutter:
                case DomeShutterStatus.OPEN:
                    log.info("Dome is open.")
                    break
                case _:
                    pass
            await asyncio.sleep(1)
        await self.brot.dome.start_tracking()
        while True:
            match self.brot.dome.status:
                case DomeStatus.TRACKING:
                    log.info("Dome is tracking the telescope azimuth.")
                    break
                case DomeStatus.ERROR:
                    await self._error_state()
                    raise exc.InitError("Dome entered error state while starting tracking.")
                case _:
                    pass
            await asyncio.sleep(1)
        await self._change_motion_status(MotionStatus.POSITIONED)
        await self.comm.send_event(RoofOpenedEvent())

    @timeout(300)
    async def park(self, **kwargs: Any) -> None:
        if self.brot.dome.status == DomeStatus.PARKED and self.brot.dome.shutter == DomeShutterStatus.CLOSED:
            return
        elif self.brot.dome.status == DomeStatus.ERROR:
            await self._error_state("Dome is in error state. Cannot close/park.")
            raise exc.ParkError("Dome is in error state. Cannot close/park.")

        await self._change_motion_status(MotionStatus.PARKING)
        await self.comm.send_event(RoofClosingEvent())
        await self.brot.dome.stop_tracking()
        while True:
            match self.brot.dome.status:
                case DomeStatus.TRACKING:
                    pass
                case DomeStatus.ERROR:
                    await self._error_state()
                    raise exc.ParkError("Dome entered error state while stopping tracking.")
                case _:
                    break
            await asyncio.sleep(1)
        await self.brot.dome.close()
        while True:
            match self.brot.dome.shutter:
                case DomeShutterStatus.CLOSED:
                    log.info("Dome shutter is closed.")
                    break
                case _:
                    pass
            await asyncio.sleep(1)
        await self.brot.dome.park()
        while True:
            match self.brot.dome.status:
                case DomeStatus.PARKED:
                    log.info("Dome is parked.")
                    break
                case DomeStatus.ERROR:
                    await self._error_state()
                    raise exc.ParkError("Dome entered error state while parking.")
                case _:
                    pass
            await asyncio.sleep(1)
        await self._change_motion_status(MotionStatus.PARKED)

    async def stop_motion(self, device: str | None = None, **kwargs: Any) -> None:
        pass  # no stopping of the roof possible

    async def move_altaz(self, alt: float, az: float, **kwargs: Any) -> None:
        pass  # dome tracks telescope automatically

    async def _error_state(self, mess: str = "Dome is in error state.") -> None:
        log.error(mess)
        await self._change_motion_status(MotionStatus.ERROR)


__all__ = ["BrotDome"]
