import asyncio
from dataclasses import dataclass, field
import logging
from typing import Tuple, Dict, Any, Optional, get_type_hints, List

import async_timer
import qasync

from pyobs.events import RoofOpenedEvent, RoofClosingEvent
from pyobs.mixins import FitsNamespaceMixin
from pyobs.interfaces import IDome
from pyobs.modules.roof.baseroof import BaseDome
from pyobs.utils.enums import MotionStatus
from pyobs.utils.time import Time
from pyobs.utils.exceptions import MoveError

from brot.mqtttransport import MQTTTransport
from brot import Transport, BROT, TelescopeStatus

log = logging.getLogger(__name__)

class BrotDome(
    BaseDome,
    IDome,
    FitsNamespaceMixin
    ):

    def __init__(
        self,
        host: str,
        name: str,
        port: int = 1883,
        keepalive: int = 60,
        **kwargs: Any,
    ):
        BaseDome.__init__(self,**kwargs)
        self.mqtt = MQTTTransport(host, port)
        self.brot = BROT(self.mqtt, name)
        # mixins
        FitsNamespaceMixin.__init__(self, **kwargs)

    @qasync.asyncSlot()
    async def open(self):
        await BaseRoof.open(self)
        asyncio.create_task(self.mqtt.run())
        await asyncio.sleep(2)
        # check whats up
        match self.brot.telescope.status.value:
            case TelescopeStatus.PARKED, TelescopeStatus.INITPARK:
                await self._change_motion_status(MotionStatus.PARKED)
            case TelescopeStatus.ONLINE:
                log.info("Telescope is already online. Please make sure it is not used by another instance!")
                await self._change_motion_status(MotionStatus.POSITIONED)
            case TelescopeStatus.ERROR:
                log.info("Telescope is in error state.")
                await self._change_motion_status(MotionStatus.ERROR)

    async def get_altaz(self) -> float:
        return self.brot.dome.azimuth

    @timeout(5*60)
    async def init(self, **kwargs: Any) -> None:
        """Open the roof.

        Raises:
            AcquireLockFailed: If current motion could not be aborted.
        """

        if self.brot.dome.shutter == self.brot.dome.DomeShutterStatus.OPEN:
            return
        elif self.brot.dome.status == self.brot.dome.DomeStatus.ERROR:
            await self._change_motion_status(MotionStatus.ERROR)
            log.info("Dome is in error state. Cannot open.")
            return

        async with LockWithAbort(self._lock_motion, self._abort_motion):
            await self._change_motion_status(MotionStatus.INITIALIZING)

            #send open command
            await self.brot.dome.open()

            while True:
                match self.brot.dome.shutter:
                    case self.brot.dome.DomeShutterStatus.OPEN:
                        log.info("Dome is open.")
                        break
                    case _:
                        pass
                await asyncio.sleep(1)
            # send tracking command
            await self.brot.dome.start_tracking()
            while True:
                match self.brot.dome.status:
                    case self.brot.dome.DomeStatus.TRACKING:
                        log.info("Dome is tracking the telescope azimuth.")
                        break
                    case self.brot.dome.DomeStatus.ERROR:
                        log.info("Dome is in error state.")
                        await self._change_motion_status(MotionStatus.ERROR)
                        return
                    case _:
                        pass
                await asyncio.sleep(1)
            await self._change_motion_status(MotionStatus.IDLE)
            await self.comm.send_event(RoofOpenedEvent())


    @timeout(5*60)
    async def park(self, **kwargs: Any) -> None:
        """Close the roof. (stop tracking, close and go to parking position for dome)

        Raises:
            AcquireLockFailed: If current motion could not be aborted.
        """

        if self.brot.dome.status == self.brot.dome.DomeStatus.PARKED and self.brot.some.shutter == self.brot.dome.DomeShutterStatus.CLOSED:
            return
        elif self.brot.dome.status == self.brot.dome.DomeStatus.ERROR:
            await self._change_motion_status(MotionStatus.ERROR)
            log.info("Dome is in error state. Cannot close/park.")
            return

        async with LockWithAbort(self._lock_motion, self._abort_motion):
            await self._change_motion_status(MotionStatus.PARKING)
            await self.comm.send_event(RoofClosingEvent())
            # stop tracking
            await self.brot.dome.stop_tracking()
            while True:
                match self.brot.dome.status:
                    case self.brot.dome.DomeStatus.TRACKING:
                        pass
                    case self.brot.dome.DomeStatus.ERROR:
                        log.info("Dome is in error state.")
                        await self._change_motion_status(MotionStatus.ERROR)
                        return
                    case _:
                        break
                await asyncio.sleep(1)
            # close shutter
            await self.brot.dome.close()
            while True:
                match self.brot.dome.shutter:
                    case self.brot.dome.DomeShutterStatus.CLOSED:
                        log.info("Dome shutter is closed.")
                        break
                    case _:
                        pass
                await asyncio.sleep(1)

            # go to parking position
            await self.brot.dome.park()
            while True:
                match self.brot.dome.status:
                    case self.brot.dome.DomeStatus.PARKED:
                        log.info("Dome is parked.")
                        break
                    case self.brot.dome.DomeStatus.ERROR:
                        log.info("Dome is in error state.")
                        await self._change_motion_status(MotionStatus.ERROR)
                        return
                    case _:
                        pass
                await asyncio.sleep(1)
            await self._change_motion_status(MotionStatus.PARKED)

        async def stop_motion(self, device: Optional[str] = None, **kwargs: Any) -> None:
            """Stop the motion.

            Args:
                device: Name of device to stop, or None for all.

            Raises:
                AcquireLockFailed: If current motion could not be aborted.
            """
            pass # no stopping of the roof possible

__all__ = ["BrotDome"]
