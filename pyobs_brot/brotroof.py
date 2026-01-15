import asyncio
import logging
from typing import Any

from pyobs.events import RoofOpenedEvent, RoofClosingEvent
from pyobs.modules.roof.baseroof import BaseRoof
from pyobs.modules import timeout
from pyobs.utils.enums import MotionStatus

from pybrotlib import BROT
from pybrotlib.mqtttransport import MQTTTransport
from pybrotlib.roof import RoofStatus

log = logging.getLogger(__name__)


class BrotRoof(BaseRoof):
    def __init__(
        self,
        host: str,
        name: str,
        port: int = 1883,
        **kwargs: Any,
    ):
        BaseRoof.__init__(self, **kwargs)
        self.mqtt = MQTTTransport(host, port)
        self.brot = BROT(self.mqtt, name)

        # add thread for pulling the status constantly
        self.add_background_task(self._update_status_task)

    async def open(self) -> None:
        await BaseRoof.open(self)
        asyncio.create_task(self.mqtt.run())

        await self.comm.register_event(RoofOpenedEvent)
        await self.comm.register_event(RoofClosingEvent)

    async def _update_status_task(self) -> None:
        while True:
            if self.mqtt.connected:
                await self._update_status()
            await asyncio.sleep(1)

    async def _update_status(self) -> None:
        # check whats up
        match self.brot.roof.status:
            case RoofStatus.ERROR:
                await self._error_state()
            case RoofStatus.CLOSED:
                await self._change_motion_status(MotionStatus.PARKED)
            case RoofStatus.OPENING:
                await self._change_motion_status(MotionStatus.INITIALIZING)
            case RoofStatus.CLOSING:
                await self._change_motion_status(MotionStatus.PARKING)
            case RoofStatus.OPEN:
                await self._change_motion_status(MotionStatus.POSITIONED)
            case RoofStatus.STOPPED:
                await self._change_motion_status(MotionStatus.IDLE)

    @timeout(300)
    async def init(self, **kwargs: Any) -> None:
        log.info("Opening roof")
        if self.brot.roof.status == RoofStatus.OPEN:
            return
        elif self.brot.roof.status == RoofStatus.ERROR:
            await self._error_state("Roof is in error state. Cannot open.")
            return

        await self._change_motion_status(MotionStatus.INITIALIZING)

        # send open command
        await self.brot.roof.open()
        while self.brot.roof.status != RoofStatus.OPEN:
            await asyncio.sleep(1)
        log.info("Roof is open.")

        await self._change_motion_status(MotionStatus.POSITIONED)
        await self.comm.send_event(RoofOpenedEvent())

    @timeout(300)
    async def park(self, **kwargs: Any) -> None:
        if self.brot.roof.status == RoofStatus.PARKED:
            return
        elif self.brot.roof.status == RoofStatus.ERROR:
            await self._error_state("Roof is in error state. Cannot close.")
            return

        await self._change_motion_status(MotionStatus.PARKING)
        await self.comm.send_event(RoofClosingEvent())

        # close roof
        await self.brot.roof.close()
        while self.brot.roof.status != RoofStatus.CLOSED:
            await asyncio.sleep(1)
        log.info("Roof is closed.")

        await self._change_motion_status(MotionStatus.PARKED)

    async def stop_motion(self, device: str | None = None, **kwargs: Any) -> None:
        pass

    async def _error_state(self, mess: str = "Roof is in error state.") -> None:
        log.error(mess)
        await self._change_motion_status(MotionStatus.ERROR)


__all__ = ["BrotRoof"]
