import asyncio
import logging
from typing import Any
import numpy as np

from pyobs.mixins import FitsNamespaceMixin
from pyobs.interfaces import (
    IRoof,
    IDome,
    IFocuser,
    ITemperatures,
    IOffsetsAltAz,
    IOffsetsRaDec,
    IPointingSeries,
    IPointingRaDec,
    IPointingAltAz,
)
from pyobs.modules.telescope.basetelescope import BaseTelescope
from pyobs.modules import timeout
from pyobs.utils.enums import MotionStatus
from pyobs.utils.publisher import CsvPublisher
from pyobs.utils.time import Time

from pybrotlib.mqtttransport import MQTTTransport  # type: ignore
from pybrotlib import Transport, BROT  # type: ignore
from pybrotlib.telescope import TelescopeStatus, GlobalTelescopeStatus  # type: ignore

log = logging.getLogger(__name__)


class BrotBaseTelescope(
    BaseTelescope,
    IFocuser,
    ITemperatures,
    IPointingRaDec,
    IPointingAltAz,
    IPointingSeries,
    FitsNamespaceMixin,
):
    def __init__(
        self,
        host: str,
        name: str,
        port: int = 1883,
        keepalive: int = 60,
        roof: str = "None",
        dome: str = "None",
        **kwargs: Any,
    ):
        BaseTelescope.__init__(self, **kwargs, motion_status_interfaces=["ITelescope", "IFocuser"])

        self.mqtt = MQTTTransport(host, port)
        self.brot = BROT(self.mqtt, name)
        self.focus_offset = 0.0
        self._dome = dome
        self._roof = roof

        # mixins
        FitsNamespaceMixin.__init__(self, **kwargs)

    async def open(self) -> None:
        await BaseTelescope.open(self)
        asyncio.create_task(self.mqtt.run())
        await asyncio.sleep(2)
        # check whats up
        match self.brot.telescope.status:
            case TelescopeStatus.PARKED | TelescopeStatus.INITPARK:
                await self._change_motion_status(MotionStatus.PARKED)
            case TelescopeStatus.ONLINE:
                log.info("Telescope is already online. Please make sure it is not used by another instance!")
                await self._change_motion_status(MotionStatus.POSITIONED)
            case TelescopeStatus.ERROR:
                await self._error_state()
        if self.brot.telescope._telemetry.TELESCOPE.MOTION_STATE == 8:
            await self._change_motion_status(MotionStatus.TRACKING)
        if self._dome != "None":
            # check dome
            try:
                await self.proxy(self._dome, IDome)
                log.info("Dome was found.")
            except ValueError:
                log.warning("Dome does not exist or is not of correct type at the moment.")
        if self._roof != "None":
            # check dome
            try:
                await self.proxy(self._roof, IRoof)
            except ValueError:
                log.warning("Roof does not exist or is not of correct type at the moment.")

    async def close(self) -> None:
        await BaseTelescope.close(self)

    async def _error_state(self, mess: str = "Telescope is in error state.") -> None:
        log.error(mess)
        await self._change_motion_status(MotionStatus.ERROR)
        return

    async def _wait_for_tracking(self) -> None:
        await asyncio.sleep(10)
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.MOTION_STATE:
                case 0.0, 1.0:
                    # still moving
                    pass
                case 8.0:
                    # tracking -> exit
                    return
                case -1.0:
                    # something went wrong
                    await self._error_state("The telescope experienced an error during track command.")
                    return
                case _:
                    pass
            await asyncio.sleep(1)

    @timeout(5 * 60)
    async def _move_radec(self, ra: float, dec: float, abort_event: asyncio.Event) -> None:
        # change to slewing
        await self._change_motion_status(MotionStatus.SLEWING)
        # send command
        await self.brot.telescope.track(ra, dec)
        await asyncio.sleep(10)
        await self._wait_for_tracking()
        if self._dome != "None":
            try:
                dome = await self.proxy(self._dome, IDome)
                while True:
                    match await dome.get_motion_status():
                        case MotionStatus.POSITIONED:
                            await self._change_motion_status(MotionStatus.TRACKING)
                            return
                        case MotionStatus.ERROR:
                            await self._error_state("The dome experienced an error during track command.")
                            return
                        case MotionStatus.PARKED:
                            await self._change_motion_status(MotionStatus.TRACKING)
                            log.info("The dome is parked, tracking but not onsky.")
                            return
                    await asyncio.sleep(1)
            except:
                log.warning("Dome module cannot be reached.")

    @timeout(120)
    async def _move_altaz(self, alt: float, az: float, abort_event: asyncio.Event) -> None:
        # change to slewing
        await self._change_motion_status(MotionStatus.SLEWING)
        # send command
        await self.brot.telescope.move(alt, az)
        await asyncio.sleep(5)
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.MOTION_STATE:
                case 0.0, 1.0:
                    break
                case 1.0, 8.0:
                    # still moving
                    pass
                case _:
                    # something went wrong
                    await self._change_motion_status(MotionStatus.ERROR)
                    return
            await asyncio.sleep(1)
        if self._dome != "None":
            try:
                dome = await self.proxy(self._dome, IDome)
                while True:
                    match await dome.get_motion_status():
                        case MotionStatus.POSITIONED:
                            await self._change_motion_status(MotionStatus.POSITIONED)
                            return
                        case MotionStatus.ERROR:
                            await self._error_state("The dome experienced an error during track command.")
                            return
                        case MotionStatus.PARKED:
                            await self._change_motion_status(MotionStatus.POSITIONED)
                            log.info("The dome is parked, tracking but not onsky.")
                            return
                    await asyncio.sleep(1)
            except:
                log.warning("Dome module cannot be reached.")

    async def get_altaz(self, **kwargs: Any) -> tuple[float, float]:
        return (
            self.brot.telescope._telemetry.POSITION.HORIZONTAL.ALT,
            self.brot.telescope._telemetry.POSITION.HORIZONTAL.AZ,
        )

    async def get_radec(self, **kwargs: Any) -> tuple[float, float]:
        return (
            self.brot.telescope._telemetry.POSITION.EQUATORIAL.RA_J2000 * 15,
            self.brot.telescope._telemetry.POSITION.EQUATORIAL.DEC_J2000,
        )

    async def get_temperatures(self, **kwargs: Any) -> dict[str, float]:
        """Returns all temperatures measured by this module.

        Returns:
            Dict containing temperatures.
        """
        return {}

    async def set_focus(self, focus: float, **kwargs: Any) -> None:
        await self.brot.focus.set(focus + self.focus_offset)
        await asyncio.sleep(2)

    async def set_focus_offset(self, offset: float, **kwargs: Any) -> None:
        # get current focus position
        focus = self.brot.focus.position
        await self.brot.focus.set(focus + offset)
        await asyncio.sleep(2)

    async def get_focus(self, **kwargs: Any) -> float:
        return float(self.brot.focus.position - self.focus_offset)

    async def get_focus_offset(self, **kwargs: Any) -> float:
        return self.focus_offset

    @timeout(120)
    async def init(self, **kwargs: Any) -> None:
        # check whats up
        match self.brot.telescope.status:
            case TelescopeStatus.PARKED, TelescopeStatus.INITPARK:
                pass
            case TelescopeStatus.ONLINE:
                log.info("Telescope is already online.")
                await self._change_motion_status(MotionStatus.POSITIONED)
                return
            case TelescopeStatus.ERROR:
                await self._error_state("Telescope can not be initialized, it has errors.")
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
                case -1.0:
                    # something went wrong
                    await self._error_state("Error during powerup of telescope.")
                    return
                case 0.0:
                    # still moving
                    pass
            await asyncio.sleep(1)

    @timeout(180)
    async def park(self, **kwargs: Any) -> None:
        # check whats up
        match self.brot.telescope.status:
            case TelescopeStatus.PARKED, TelescopeStatus.INITPARK:
                log.info("Telescope is already parked.")
                return
            case TelescopeStatus.ONLINE:
                pass
            case TelescopeStatus.ERROR:
                await self._error_state("Telescope can not be parked, it has errors.")
                return
        await self._change_motion_status(MotionStatus.PARKING)
        log.info("Parking telescope...")
        # send command
        await self.brot.telescope.park()
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.READY_STATE:
                case 0.0:
                    await self._change_motion_status(MotionStatus.PARKED)
                    log.info("Parked telescope.")
                    return
                case -1.0:
                    # something went wrong
                    await self._error_state("Error during parking of the telescope.")
                    return
                case _:
                    # still moving
                    pass
            await asyncio.sleep(1)

    @timeout(20)
    async def stop_motion(self, device: str | None = None, **kwargs: Any) -> None:
        # send command
        await self.brot.telescope.stop()
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.MOTION_STATE:
                case 0.0:
                    log.info("Stopped telescope.")
                    return
                case 1.0 | 8.0:
                    # still going
                    pass
                case _:
                    # error
                    await self._error_state("Error during stopping of the telescope.")
                    return
            await asyncio.sleep(1)

    async def is_ready(self, **kwargs: Any) -> bool:
        match self.brot.telescope.global_status:
            case GlobalTelescopeStatus.OPERATIONAL:
                return True
            case GlobalTelescopeStatus.PANIC | GlobalTelescopeStatus.ERROR | _:
                return False


class BrotRaDecTelescope(BrotBaseTelescope, IOffsetsRaDec):
    def __init__(
        self,
        pointing_file: str = "/pyobs/pointing.csv",
        **kwargs: Any,
    ):
        BrotBaseTelescope.__init__(self, **kwargs)
        self._pointing_log = None if pointing_file is None else CsvPublisher(pointing_file)

    async def set_offsets_radec(self, dra: float, ddec: float, **kwargs: Any) -> None:
        # send dra as dha
        await self.brot.telescope.set_offset_ha(-1.0 * dra * 3600)
        await self.brot.telescope.set_offset_dec(ddec * 3600)

    async def get_offsets_radec(self, **kwargs: Any) -> tuple[float, float]:
        return (
            self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.HA.OFFSET * -1.0,
            self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.DEC.OFFSET,
        )

    async def get_fits_header_before(
        self, namespaces: list[str] | None = None, **kwargs: Any
    ) -> dict[str, tuple[Any, str]]:
        """Returns FITS header for the current status of this module.

        Args:
            namespaces: If given, only return FITS headers for the given namespaces.

        Returns:
            Dictionary containing FITS headers.
        """

        # get headers from base
        hdr = await BrotBaseTelescope.get_fits_header_before(self)

        # define values to request
        hdr["TEL-FOCU"] = (self.brot.focus.position, "Focus position [mm]")
        hdr["HAOFF"] = (self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.HA.OFFSET, "Hour Angle offset")
        hdr["DECOFF"] = (self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.DEC.OFFSET, "Declination offset")

        # return it
        return self._filter_fits_namespace(hdr, namespaces=namespaces, **kwargs)

    async def add_pointing_measurement(self, **kwargs: Any) -> None:
        telemetry = self.brot.telescope._telemetry
        data = {
            "Time": Time.now().isot,
            "Ha": telemetry.OBJECT.EQUATORIAL.HA,
            "Dec": telemetry.OBJECT.EQUATORIAL.DEC,
            "HaOff": telemetry.POSITION.INSTRUMENTAL.HA.OFFSET / np.cos(np.radians(telemetry.OBJECT.EQUATORIAL.DEC))
            + telemetry.POINTING.OFFSETS.HA,
            "DecOff": telemetry.POSITION.INSTRUMENTAL.DEC.OFFSET + telemetry.POINTING.OFFSETS.DEC,
        }
        await self._pointing_log(**data)
        log.info("Pointing measurement written.")


class BrotAltAzTelescope(BrotBaseTelescope, IOffsetsAltAz, IPointingSeries):
    def __init__(
        self,
        pointing_file: str = "/pyobs/pointing.csv",
        **kwargs: Any,
    ):
        BrotBaseTelescope.__init__(self, **kwargs)
        self._pointing_log = None if pointing_file is None else CsvPublisher(pointing_file)

    async def set_offsets_altaz(self, dalt: float, daz: float, **kwargs: Any) -> None:
        """Move an Alt/Az offset.

        Args:
            dalt: Altitude offset in degrees.
            daz: Azimuth offset in degrees.

        Raises:
            MoveError: If device could not be moved.
        """
        await self.brot.telescope.set_offset_alt(dalt * 3600)
        await self.brot.telescope.set_offset_az(daz * 3600)
        await asyncio.sleep(10)
        await self._wait_for_tracking()

    async def get_offsets_altaz(self, **kwargs: Any) -> tuple[float, float]:
        """Get Alt/Az offset.

        Returns:
            Tuple with alt and az offsets.
        """
        return (
            self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.ALT.OFFSET,
            self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.AZ.OFFSET,
        )

    async def get_fits_header_before(
        self, namespaces: list[str] | None = None, **kwargs: Any
    ) -> dict[str, tuple[Any, str]]:
        """Returns FITS header for the current status of this module.

        Args:
            namespaces: If given, only return FITS headers for the given namespaces.

        Returns:
            Dictionary containing FITS headers.
        """

        # get headers from base
        hdr = await BrotBaseTelescope.get_fits_header_before(self)

        # define values to request
        hdr["TEL-FOCU"] = (self.brot.focus.position, "Focus position [mm]")
        hdr["HAOFF"] = (self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.HA.OFFSET, "Hour Angle offset")
        hdr["DECOFF"] = (self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.DEC.OFFSET, "Declination offset")

        # return it
        return self._filter_fits_namespace(hdr, namespaces=namespaces, **kwargs)

    async def add_pointing_measurement(self, **kwargs: Any) -> None:
        telemetry = self.brot.telescope._telemetry
        data = {
            "Time": Time.now().isot,
            "Az": telemetry.OBJECT.HORIZONTAL.AZ,
            "Alt": telemetry.OBJECT.HORIZONTAL.ALT,
            "AzOff": telemetry.POSITION.INSTRUMENTAL.AZ.OFFSET / np.cos(np.radians(telemetry.POSITION.HORIZONTAL.ALT))
            + telemetry.POINTING.OFFSETS.AZ,
            "AltOff": telemetry.POSITION.INSTRUMENTAL.ALT.OFFSET + telemetry.POINTING.OFFSETS.ALT,
        }
        await self._pointing_log(**data)
        log.info("Pointing measurement written.")


__all__ = ["BrotRaDecTelescope", "BrotAltAzTelescope"]
