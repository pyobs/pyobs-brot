import asyncio
from dataclasses import dataclass, field
import logging
from typing import Tuple, Dict, Any, Optional, get_type_hints, List

import async_timer

from pyobs.mixins import FitsNamespaceMixin
from pyobs.interfaces import IFocuser, ITemperatures, IOffsetsAltAz, IPointingSeries, IPointingRaDec, IPointingAltAz
from pyobs.modules.telescope.basetelescope import BaseTelescope
from pyobs.utils.enums import MotionStatus
from pyobs.utils.publisher import CsvPublisher
from pyobs.utils.time import Time
from pyobs.utils.exceptions import MoveError

from brot.mqtttransport import MQTTTransport
from .brot import Transport, BROT, TelescopeStatus

log = logging.getLogger(__name__)


class BrotTelescope(
    BaseTelescope,
    IPointingRaDec,
    IPointingAltAz,
    IOffsetsAltAz,
    IOffsetsRaDec,
    IFocuser,
    ITemperatures,
    IPointingSeries,
    FitsNamespaceMixin,
):
    def __init__(
        self,
        host: str,
        port: int = 1883,
        name: str,
        mount: str,
        keepalive: int = 60,
        pointing_file: str = "/pyobs/pointing.csv",
        **kwargs: Any,
    ):
        BaseTelescope.__init__(self, **kwargs, motion_status_interfaces=["ITelescope", "IFocuser"])

        self.mqtt = MQTTTransport(host, port)
        self.brot = BROT(self.mqtt, name)
        self.focus_offset = 0.0
        self._pointing_log = None if pointing_file is None else CsvPublisher(pointing_file)

        # mixins
        FitsNamespaceMixin.__init__(self, **kwargs)

    @qasync.asyncSlot()
    async def open(self):
        await BaseTelescope.open(self)
        asyncio.create_task(self.mqtt.run())

    async def close(self):
        await BaseTelescope.close(self)

    async def _move_radec(self, ra: float, dec: float, abort_event: asyncio.Event) -> None:
        # create timout timer (2 min)
        abort_timer = async_timer.Timer(2*60)
        # change to slewing
        await self._change_motion_status(MotionStatus.SLEWING)
        # send command
        await self.brot.telescope.track(ra,dec)
        # start timer
        abort_timer.start()
        # while not timeout
        while abort_timer.is_running():
            match self.brot.telemetry.TELESCOPE.MOTION_STATE:
                case 0.0, 1.0:
                    # still moving
                    pass
                case 8.0:
                    # tracking -> exit
                    await self._change_motion_status(MotionStatus.TRACKING)
                    return
                case _:
                    # something went wrong
                    break
             await asyncio.sleep(5)
        await self._change_motion_status(MotionStatus.ERROR)
        raise MoveError

    async def _move_altaz(self, alt: float, az: float, abort_event: asyncio.Event) -> None:
        # create timout timer (2 min)
        abort_timer = async_timer.Timer(2*60)
        # change to slewing
        await self._change_motion_status(MotionStatus.SLEWING)
        # send command
        await self.brot.telescope.move(alt,az)
        await asyncio.sleep(5)
        # start timer
        abort_timer.start()
        # while not timeout
        while abort_timer.is_running():
            match self.brot.telemetry.TELESCOPE.MOTION_STATE:
                case 0.0, 1.0:
                    await self._change_motion_status(MotionStatus.POSITIONED)
                    return
                case 1.0,8.0:
                    # still moving
                    pass
                case _:
                    # something went wrong
                    break
             await asyncio.sleep(5)
        await self._change_motion_status(MotionStatus.ERROR)
        raise MoveError

    async def set_offsets_altaz(self, dalt: float, daz: float, **kwargs: Any) -> None:
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command elevationoffset={dalt}")
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command azimuthoffset={daz}")
        await asyncio.sleep(10)

    async def get_offsets_altaz(self, **kwargs: Any) -> Tuple[float, float]:
        return self.telemetry.position.instrumental.alt.offset, self.telemetry.position.instrumental.az.offset

    async def set_focus(self, focus: float, **kwargs: Any) -> None:
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command focus={focus + self.focus_offset}")
        await asyncio.sleep(2)

    async def set_focus_offset(self, offset: float, **kwargs: Any) -> None:
        self.focus_offset = offset
        focus = self.telemetry.focusposition
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command focus={focus + self.focus_offset}")
        await asyncio.sleep(2)

    async def get_focus(self, **kwargs: Any) -> float:
        return self.telemetry.focusposition - self.focus_offset

    async def get_focus_offset(self, **kwargs: Any) -> float:
        return self.focus_offset

    async def init(self, **kwargs: Any) -> None:
        # check whats up
        match self.brot.telescope.status.value:
            case TelescopeStatus.PARKED, TelescopeStatus.INITPARK:
                pass
            case TelescopeStatus.ONLINE:
                log.info("Telescope is already online.")
                return
            case TelescopeStatus.ERROR:
                log.info("Telescope can not be initialized, it has errors.")
                await self._change_motion_status(MotionStatus.ERROR)
                return
        await self._change_motion_status(MotionStatus.INITIALIZING)
        log.info("Initializing telescope...")
        # create timout timer (2 min)
        abort_timer = async_timer.Timer(2*60)
        # send command
        await self.brot.telescope.power_on()
        # start timer
        abort_timer.start()
        # while not timeout
        while abort_timer.is_running():
            match self.brot.telemetry.TELESCOPE.READY_STATE:
                case 1.0:
                    await self._change_motion_status(MotionStatus.POSITIONED)
                    return
                case 0.0:
                    # still moving
                    pass
                case _:
                    # something went wrong
                    break
             await asyncio.sleep(5)
        log.info("Error during powerup of telescope.")
        await self._change_motion_status(MotionStatus.ERROR)

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
        await await self._change_motion_status(MotionStatus.PARKING)
        log.info("Parking telescope...")
        # create timout timer (2 min)
        abort_timer = async_timer.Timer(2*60)
        # send command
        await self.brot.telescope.park()
        # start timer
        abort_timer.start()
        # while not timeout
        while abort_timer.is_running():
            match self.brot.telemetry.TELESCOPE.READY_STATE:
                case 0.0:
                    await self._change_motion_status(MotionStatus.PARKED)
                    return
                case 1.0:
                    # still moving
                    pass
                case _:
                    # something went wrong
                    break
             await asyncio.sleep(5)
        log.info("Error during parking of the telescope.")
        await self._change_motion_status(MotionStatus.ERROR)

    async def stop_motion(self, device: Optional[str] = None, **kwargs: Any) -> None:
         # send command
        await self.brot.telescope.stop()

    async def is_ready(self, **kwargs: Any) -> bool:
         match self.brot.telescope.global_status:
            case GlobalTelescopeStatus.OPERATIONAL:
                return True
            case GlobalTelescopeStatus.PANIC | GlobalTelescopeStatus.ERROR | _:
                return False

    async def get_altaz(self, **kwargs: Any) -> Tuple[float, float]:
        return self.brot.transport.telemetry.POSITION.HORIZONTAL.ALT, self.brot.transport.telemetry.POSITION.HORIZONTAL.AZ

    async def get_radec(self, **kwargs: Any) -> Tuple[float, float]:
        return self.transport.telemetry.POSITION.EQUATORIAL.RA_J2000, self.transport.telemetry.POSITION.EQUATORIAL.DEC_J2000

    async def get_temperatures(self, **kwargs: Any) -> Dict[str, float]:
        """Returns all temperatures measured by this module.

        Returns:
            Dict containing temperatures.
        """
        pass

    async def start_pointing_series(self, **kwargs: Any) -> str:
        log.info("Starting pointing series.")

    async def stop_pointing_series(self, **kwargs: Any) -> None:
        log.info("Stopping pointing series.")

    async def add_pointing_measure(self, **kwargs: Any) -> None:
        await self._pointing_log(
            time=Time.now().isot,
            alt=self.telemetry.position.instrumental.alt.realpos,
            az=self.telemetry.position.instrumental.az.realpos,
            alt_off=self.telemetry.position.instrumental.alt.offset,
            az_off=self.telemetry.position.instrumental.az.offset,
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
        hdr["TEL-FOCU"] = (self.telemetry.focusposition, "Focus position [mm]")
        # "TEL-ROT": ("POSITION.INSTRUMENTAL.DEROTATOR[2].REALPOS", "Derotator instrumental position at end [deg]"),
        # "DEROTOFF": ("POINTING.SETUP.DEROTATOR.OFFSET", "Derotator offset [deg]"),
        # "AZOFF": ("POSITION.INSTRUMENTAL.AZ.OFFSET", "Azimuth offset"),
        # "ALTOFF": ("POSITION.INSTRUMENTAL.ZD.OFFSET", "Altitude offset"),

        # return it
        return self._filter_fits_namespace(hdr, namespaces=namespaces, **kwargs)


__all__ = ["BrotTelescope"]
