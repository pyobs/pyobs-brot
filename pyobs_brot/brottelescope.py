import asyncio
from dataclasses import dataclass, field
import logging
from typing import Tuple, Dict, Any, Optional, get_type_hints, List

import qasync

from pyobs.mixins import FitsNamespaceMixin
from pyobs.interfaces import IFocuser, ITemperatures, IOffsetsAltAz, IOffsetsRaDec, IPointingSeries, IPointingRaDec, IPointingAltAz
from pyobs.modules.telescope.basetelescope import BaseTelescope
from pyobs.utils.enums import MotionStatus
from pyobs.utils.publisher import CsvPublisher
from pyobs.utils.time import Time
from pyobs.utils.exceptions import MoveError

from brot.mqtttransport import MQTTTransport
from brot import Transport, BROT, TelescopeStatus

log = logging.getLogger(__name__)


class BrotBaseTelescope(
    BaseTelescope,
    IFocuser,
    ITemperatures,
    FitsNamespaceMixin,
):
    def __init__(
        self,
        host: str,
        name: str,
        port: int = 1883,
        keepalive: int = 60,
        **kwargs: Any,
    ):
        BaseTelescope.__init__(self, **kwargs, motion_status_interfaces=["ITelescope", "IFocuser"])

        self.mqtt = MQTTTransport(host, port)
        self.brot = BROT(self.mqtt, name)
        self.focus_offset = 0.0
        # mixins
        FitsNamespaceMixin.__init__(self, **kwargs)

    @qasync.asyncSlot()
    async def open(self):
        await BaseTelescope.open(self)
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

    async def close(self):
        await BaseTelescope.close(self)

    @timeout(120)
    async def _move_radec(self, ra: float, dec: float, abort_event: asyncio.Event) -> None:
        # change to slewing
        await self._change_motion_status(MotionStatus.SLEWING)
        # send command
        await self.brot.telescope.track(ra,dec)
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.MOTION_STATE:
                case 0.0, 1.0:
                    # still moving
                    pass
                case 8.0:
                    # tracking -> exit
                    await self._change_motion_status(MotionStatus.TRACKING)
                    return
                case _:
                    # something went wrong
                    await self._change_motion_status(MotionStatus.ERROR)
                    raise MoveError
                    return
            await asyncio.sleep(1)

    @timeout(120)
    async def _move_altaz(self, alt: float, az: float, abort_event: asyncio.Event) -> None:
        # change to slewing
        await self._change_motion_status(MotionStatus.SLEWING)
        # send command
        await self.brot.telescope.move(alt,az)
        await asyncio.sleep(5)
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.MOTION_STATE:
                case 0.0, 1.0:
                    await self._change_motion_status(MotionStatus.POSITIONED)
                    return
                case 1.0,8.0:
                    # still moving
                    pass
                case _:
                    # something went wrong
                    await self._change_motion_status(MotionStatus.ERROR)
                    raise MoveError
                    return
            await asyncio.sleep(1)


    async def get_altaz(self, **kwargs: Any) -> Tuple[float, float]:
        return self.brot.telescope._telemetry.POSITION.HORIZONTAL.ALT, self.brot.telescope._telemetry.POSITION.HORIZONTAL.AZ

    async def get_radec(self, **kwargs: Any) -> Tuple[float, float]:
        return self.brot.telescope._telemetry.POSITION.EQUATORIAL.RA_J2000, self.brot.telescope._telemetry.POSITION.EQUATORIAL.DEC_J2000

    async def get_temperatures(self, **kwargs: Any) -> Dict[str, float]:
        """Returns all temperatures measured by this module.

        Returns:
            Dict containing temperatures.
        """
        pass # not implemented (yet?)

    async def set_focus(self, focus: float, **kwargs: Any) -> None:
        self.brot.focus.set(focus+self.focus_offset)
        await asyncio.sleep(2)

    async def set_focus_offset(self, offset: float, **kwargs: Any) -> None:
        # get current focus position
        focus = self.brot.focus.position
        self.brot.fous.set(focus+offset)
        await asyncio.sleep(2)

    async def get_focus(self, **kwargs: Any) -> float:
        return self.brot.focus.position - self.focus_offset

    async def get_focus_offset(self, **kwargs: Any) -> float:
        return self.focus_offset

    @timeout(120)
    async def init(self, **kwargs: Any) -> None:
        # check whats up
        match self.brot.telescope.status.value:
            case TelescopeStatus.PARKED, TelescopeStatus.INITPARK:
                pass
            case TelescopeStatus.ONLINE:
                log.info("Telescope is already online.")
                await self._change_motion_status(MotionStatus.POSITIONED)
                return
            case TelescopeStatus.ERROR:
                log.info("Telescope can not be initialized, it has errors.")
                await self._change_motion_status(MotionStatus.ERROR)
                return
        await self._change_motion_status(MotionStatus.INITIALIZING)
        log.info("Initializing telescope...")
        # send command
        await self.brot.telescope.power_on()
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.READY_STATE:
                case 1.0:
                    await self._change_motion_status(MotionStatus.POSITIONED)
                    log.info("Telescope powered up and initialized.")
                    return
                case 0.0:
                    # still moving
                    pass
                case _:
                    # something went wrong
                    log.info("Error during powerup of telescope.")
                    await self._change_motion_status(MotionStatus.ERROR)
                    return
            await asyncio.sleep(1)

    @timeout(180)
    async def park(self, **kwargs: Any) -> None:
        # check whats up
        match self.brot.telescope.status.value:
            case TelescopeStatus.PARKED, TelescopeStatus.INITPARK:
                log.info("Telescope is already parked.")
                return
            case TelescopeStatus.ONLINE:
                pass
            case TelescopeStatus.ERROR:
                log.info("Telescope can not be parked, it has errors.")
                await self._change_motion_status(MotionStatus.ERROR)
                return
        await self._change_motion_status(MotionStatus.PARKING)
        log.info("Parking telescope...")
        # send command
        await self.brot.telescope.park()
        while True:
            match self.brot.telemetry.TELESCOPE.READY_STATE:
                case 0.0:
                    await self._change_motion_status(MotionStatus.PARKED)
                    log.info("Parked telescope.")
                    return
                case 1.0:
                    # still moving
                    pass
                case _:
                    # something went wrong
                    log.info("Error during parking of the telescope.")
                    await self._change_motion_status(MotionStatus.ERROR)
                    return
            await asyncio.sleep(1)

    @timeout(20)
    async def stop_motion(self, device: Optional[str] = None, **kwargs: Any) -> None:
        # send command
        await self.brot.telescope.stop()
        while True:
            match self.brot._telemetry.TELESCOPE.MOTION_STATE:
                case 0.0:
                    log.info("Stopped telescope.")
                    return
                case 1.0 | 8.0:
                    # still going
                    pass
                case _:
                    # error
                    log.info("Error during stopping of the telescope.")
                    await self._change_motion_status(MotionStatus.ERROR)
                    return
            await asyncio.sleep(0.5)

    async def is_ready(self, **kwargs: Any) -> bool:
         match self.brot.telescope.global_status:
            case GlobalTelescopeStatus.OPERATIONAL:
                return True
            case GlobalTelescopeStatus.PANIC | GlobalTelescopeStatus.ERROR | _:
                return False


class BrotRaDecTelescope(
    BrotBaseTelescope,
    IPointingRaDec,
    IOffsetsRaDec,
    IPointingSeries
):
     def __init__(
        self,
        host: str,
        name: str,
        port: int = 1883,
        keepalive: int = 60,
        **kwargs: Any,
    ):
        BrotBaseTelescope.__init__(self,*args, **kwargs)
        self._pointing_log = None if pointing_file is None else CsvPublisher(pointing_file)

    async def set_offsets_radec(self, dra: float, ddec: float, **kwargs: Any) -> None:
        # send dra as dha
        self.brot.telescope.set_offset_ha(-1.0*dra)
        self.brot.telescope.ser_offset_dec(ddec)

    async def get_offsets_radec(self, **kwargs: Any) -> Tuple[float, float]:
        return self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.HA.OFFSET*-1.0, self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.DEC.OFFSET

    async def start_pointing_series(self, **kwargs: Any) -> str:
        log.info("Starting pointing series.")

    async def stop_pointing_series(self, **kwargs: Any) -> None:
        log.info("Stopping pointing series.")

    async def add_pointing_measure(self, **kwargs: Any) -> None:
        await self._pointing_log(
            time=Time.now().isot,
            ha=self.brot.telescope._telemetry.OBJECT.EQUATORIAL.HA,
            dec=self.brot.telescope._telemetry.OBJECT.EQUATORIAL.DEC,
            ha_off=self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.HA.OFFSET,
            dec_off=self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.DEC.OFFSET
        )
    async def get_fits_header_before(
        self, namespaces: Optional[List[str]] = None, **kwargs: Any
    ) -> Dict[str, Tuple[Any, str]]:
        """Returns FITS header for the current status of this module.

        Args:
            namespaces: If given, only return FITS headers for the given namespaces.

        Returns:
            Dictionary containing FITS headers.
        """

        # get headers from base
        hdr = await BaseTelescope.get_fits_header_before(self)

        # define values to request
        hdr["TEL-FOCU"] = (self.brot.focus.position, "Focus position [mm]")
        hdr["HAOFF"]: (self.brot._telemetry.POSITION.INSTRUMENTAL.HA.OFFSET, "Hour Angle offset"),
        hdr["DECOFF"]: (self.brot._telemetry.POSITION.INSTRUMENTAL.DEC.OFFSET, "Declination offset"),

        # return it
        return self._filter_fits_namespace(hdr, namespaces=namespaces, **kwargs)

__all__ = ["BrotTelescope"]
