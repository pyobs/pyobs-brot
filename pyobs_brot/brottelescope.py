import asyncio
import logging
from typing import Any

import numpy as np
from pybrotlib import BROT  # type: ignore
from pybrotlib.components.telescope import MotionState, TelescopeStatus  # type: ignore
from pybrotlib.transport import MQTTTransport  # type: ignore
from pyobs.interfaces import (
    AltAzOffsetState,
    AltAzState,
    FitsHeaderEntry,
    FocuserState,
    IFocuser,
    IOffsetsAltAz,
    IOffsetsRaDec,
    IPointingAltAz,
    IPointingRaDec,
    IPointingSeries,
    IReady,
    ITemperatures,
    RaDecOffsetState,
    RaDecState,
    ReadyState,
    SensorReading,
    TemperaturesState,
)
from pyobs.mixins import FitsNamespaceMixin
from pyobs.modules import timeout
from pyobs.modules.telescope.basetelescope import BaseTelescope
from pyobs.utils.enums import MotionStatus
from pyobs.utils.publisher import CsvPublisher
from pyobs.utils.time import Time

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
        temperatures: dict[str, str] | None = None,
        keepalive: int = 60,
        roof: str = "None",
        dome: str = "None",
        **kwargs: Any,
    ):
        BaseTelescope.__init__(
            self,
            **kwargs,
            motion_status_interfaces=["ITelescope", "IFocuser"],
            wait_for_dome=None if dome == "None" else dome,
        )

        self.mqtt = MQTTTransport(host, port)
        self.brot = BROT(self.mqtt, name)
        self.temperatures: dict[str, str] = {} if temperatures is None else temperatures
        self.focus_offset = 0.0
        self._roof = roof

        FitsNamespaceMixin.__init__(self, **kwargs)
        self.add_background_task(self._update_task)

    @property
    def _position_radec(self) -> tuple[float, float] | None:
        try:
            ra = self.brot.telescope._telemetry.POSITION.EQUATORIAL.RA_ICRS * 15
            dec = self.brot.telescope._telemetry.POSITION.EQUATORIAL.DEC_ICRS
            return ra, dec
        except Exception:
            return None

    async def open(self) -> None:
        await BaseTelescope.open(self)
        asyncio.create_task(self.mqtt.run())
        await asyncio.sleep(2)

        # publish initial states
        await self.comm.set_state(IReady, ReadyState(ready=False))
        await self.comm.set_state(
            IFocuser, FocuserState(focus=float(self.brot.focus.position), focus_offset=self.focus_offset)
        )
        await self.comm.set_state(IOffsetsRaDec, RaDecOffsetState(ra=0.0, dec=0.0))
        await self.comm.set_state(IOffsetsAltAz, AltAzOffsetState(alt=0.0, az=0.0))

    async def close(self) -> None:
        await BaseTelescope.close(self)
        await self.mqtt.close()

    async def _update_task(self) -> None:
        await asyncio.sleep(2)
        while True:
            try:
                await self._update()
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break

    async def _update(self) -> None:
        match self.brot.telescope.status:
            case TelescopeStatus.PARKED:
                await self._change_motion_status(MotionStatus.PARKED)
                await self.comm.set_state(IReady, ReadyState(ready=False))
            case TelescopeStatus.INITIALIZING:
                await self._change_motion_status(MotionStatus.INITIALIZING)
                await self.comm.set_state(IReady, ReadyState(ready=False))
            case TelescopeStatus.PARKING:
                await self._change_motion_status(MotionStatus.PARKING)
                await self.comm.set_state(IReady, ReadyState(ready=False))
            case TelescopeStatus.ONLINE:
                if self.brot.telescope.motion_state == MotionState.SLEWING:
                    await self._change_motion_status(MotionStatus.SLEWING)
                elif self.brot.telescope.motion_state == MotionState.TRACKING:
                    await self._change_motion_status(MotionStatus.TRACKING)
                else:
                    await self._change_motion_status(MotionStatus.POSITIONED)
                await self.comm.set_state(IReady, ReadyState(ready=True))
            case TelescopeStatus.ERROR:
                await self._error_state()
                await self.comm.set_state(IReady, ReadyState(ready=False))

        # publish pointing state
        ra = self.brot.telescope._telemetry.POSITION.EQUATORIAL.RA_ICRS * 15
        dec = self.brot.telescope._telemetry.POSITION.EQUATORIAL.DEC_ICRS
        await self.comm.set_state(IPointingRaDec, RaDecState(ra=ra, dec=dec))
        alt = self.brot.telescope._telemetry.POSITION.HORIZONTAL.ALT
        az = self.brot.telescope._telemetry.POSITION.HORIZONTAL.AZ
        await self.comm.set_state(IPointingAltAz, AltAzState(alt=alt, az=az))

        # publish temperatures
        readings = []
        for name, loc in self.temperatures.items():
            for sensor in self.mqtt.telemetry.AUXILIARY.SENSOR.values():
                if sensor.NAME == loc:
                    readings.append(SensorReading(name=name, value=sensor.VALUE))
        if readings:
            await self.comm.set_state(ITemperatures, TemperaturesState(readings=readings))

    async def _error_state(self, mess: str = "Telescope is in error state.") -> None:
        log.error(mess)
        await self._change_motion_status(MotionStatus.ERROR)

    async def _wait_for_tracking(self) -> None:
        await asyncio.sleep(2)
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.MOTION_STATE:
                case 0.0, 1.0:
                    pass
                case 8.0:
                    return
                case -1.0:
                    await self._error_state("The telescope experienced an error during track command.")
                    return
                case _:
                    pass
            await asyncio.sleep(1)

    @timeout(5 * 60)
    async def _move_radec(self, ra: float, dec: float, abort_event: asyncio.Event) -> None:
        await self.brot.telescope.track(ra, dec)
        await asyncio.sleep(10)
        await self._wait_for_tracking()

    @timeout(120)
    async def _move_altaz(self, alt: float, az: float, abort_event: asyncio.Event) -> None:
        await self.brot.telescope.move(alt, az)
        await asyncio.sleep(5)
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.MOTION_STATE:
                case 0.0, 1.0:
                    break
                case 1.0, 8.0:
                    pass
                case _:
                    await self._change_motion_status(MotionStatus.ERROR)
                    return
            await asyncio.sleep(1)

    async def set_focus(self, focus: float, **kwargs: Any) -> None:
        await self._change_motion_status(MotionStatus.SLEWING, interface="IFocuser")
        await self.brot.focus.set(focus + self.focus_offset)
        await self._wait_for_focus()
        await self._change_motion_status(MotionStatus.POSITIONED, interface="IFocuser")
        await self.comm.set_state(IFocuser, FocuserState(focus=focus, focus_offset=self.focus_offset))

    async def set_focus_offset(self, offset: float, **kwargs: Any) -> None:
        await self._change_motion_status(MotionStatus.SLEWING, interface="IFocuser")
        focus = self.brot.focus.position
        await self.brot.focus.set(focus + offset)
        await self._wait_for_focus()
        self.focus_offset = offset
        await self._change_motion_status(MotionStatus.POSITIONED, interface="IFocuser")
        await self.comm.set_state(
            IFocuser, FocuserState(focus=float(self.brot.focus.position - self.focus_offset), focus_offset=offset)
        )

    async def _wait_for_focus(self) -> None:
        await asyncio.sleep(2.0)
        MAX_TARGET_DISTANCE = 0.01
        while self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.FOCUS.TARGETDISTANCE > MAX_TARGET_DISTANCE:
            await asyncio.sleep(0.1)

    @timeout(120)
    async def init(self, **kwargs: Any) -> None:
        match self.brot.telescope.status:
            case TelescopeStatus.PARKED:
                pass
            case TelescopeStatus.ONLINE:
                log.info("Telescope is already online.")
                return
            case TelescopeStatus.ERROR:
                await self._error_state("Telescope can not be initialized, it has errors.")
                return

        log.info("Initializing telescope...")
        await self.brot.telescope.power_on()
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.READY_STATE:
                case 1.0:
                    log.info("Telescope powered up and initialized.")
                    return
                case -1.0:
                    await self._error_state("Error during powerup of telescope.")
                    return
                case 0.0:
                    pass
            await asyncio.sleep(1)

    @timeout(180)
    async def park(self, **kwargs: Any) -> None:
        match self.brot.telescope.status:
            case TelescopeStatus.PARKED:
                log.info("Telescope is already parked.")
                return
            case TelescopeStatus.ONLINE:
                pass
            case TelescopeStatus.ERROR:
                await self._error_state("Telescope can not be parked, it has errors.")
                return

        log.info("Parking telescope...")
        await self.brot.telescope.park()
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.READY_STATE:
                case 0.0:
                    log.info("Parked telescope.")
                    return
                case -1.0:
                    await self._error_state("Error during parking of the telescope.")
                    return
                case _:
                    pass
            await asyncio.sleep(1)

    @timeout(20)
    async def stop_motion(self, device: str | None = None, **kwargs: Any) -> None:
        await self.brot.telescope.stop()
        while True:
            match self.brot.telescope._telemetry.TELESCOPE.MOTION_STATE:
                case 0.0:
                    log.info("Stopped telescope.")
                    return
                case 1.0 | 8.0:
                    pass
                case _:
                    await self._error_state("Error during stopping of the telescope.")
                    return
            await asyncio.sleep(1)

    async def start_pointing_series(self, **kwargs: Any) -> None:
        pass

    async def stop_pointing_series(self, **kwargs: Any) -> None:
        pass


class BrotRaDecTelescope(BrotBaseTelescope, IOffsetsRaDec):
    def __init__(
        self,
        pointing_file: str = "/pyobs/pointing.csv",
        **kwargs: Any,
    ):
        BrotBaseTelescope.__init__(self, **kwargs)
        self._pointing_log = None if pointing_file is None else CsvPublisher(pointing_file)

    async def set_offsets_radec(self, dra: float, ddec: float, **kwargs: Any) -> None:
        await self.brot.telescope.set_offset_ha(-1.0 * dra * 3600)
        await self.brot.telescope.set_offset_dec(ddec * 3600)
        MAX_TARGET_DISTANCE = 2.0 / 3600.0
        while (
            self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.HA.TARGETDISTANCE < MAX_TARGET_DISTANCE
            and self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.DEC.TARGETDISTANCE < MAX_TARGET_DISTANCE
        ):
            await asyncio.sleep(0.1)
        await self.comm.set_state(IOffsetsRaDec, RaDecOffsetState(ra=dra, dec=ddec))

    async def get_fits_header_before(
        self, namespaces: list[str] | None = None, **kwargs: Any
    ) -> dict[str, FitsHeaderEntry]:
        hdr = await BrotBaseTelescope.get_fits_header_before(self)
        tel = self.brot.telescope._telemetry
        hdr["TEL-FOCU"] = FitsHeaderEntry(self.brot.focus.position, "Focus position [mm]")
        hdr["HAOFF"] = FitsHeaderEntry(tel.POSITION.INSTRUMENTAL.HA.OFFSET, "Hour Angle offset")
        hdr["DECOFF"] = FitsHeaderEntry(tel.POSITION.INSTRUMENTAL.DEC.OFFSET, "Declination offset")
        return self._filter_fits_namespace(hdr, namespaces=namespaces, **kwargs)

    async def add_pointing_measurement(self, **kwargs: Any) -> None:
        telemetry = self.brot.telescope._telemetry
        data = {
            "Time": Time.now().isot,
            "Ha": telemetry.OBJECT.EQUATORIAL.HA_APPARENT,
            "Dec": telemetry.OBJECT.EQUATORIAL.DEC_APPARENT,
            "HaOff": telemetry.POSITION.INSTRUMENTAL.HA.OFFSET
            / np.cos(np.radians(telemetry.OBJECT.EQUATORIAL.DEC_APPARENT))
            + telemetry.POINTING.OFFSETS.HA,
            "DecOff": telemetry.POSITION.INSTRUMENTAL.DEC.OFFSET + telemetry.POINTING.OFFSETS.DEC,
        }
        if self._pointing_log is not None:
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

    @timeout(120)
    async def set_offsets_altaz(self, dalt: float, daz: float, **kwargs: Any) -> None:
        await self.brot.telescope.set_offset_alt(dalt * 3600)
        await self.brot.telescope.set_offset_az(daz * 3600)
        await asyncio.sleep(1.0)
        MAX_TARGET_DISTANCE = 2.0 / 3600.0
        while (
            self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.ALT.TARGETDISTANCE > MAX_TARGET_DISTANCE
            or self.brot.telescope._telemetry.POSITION.INSTRUMENTAL.AZ.TARGETDISTANCE > MAX_TARGET_DISTANCE
        ):
            await asyncio.sleep(0.1)
        await self.comm.set_state(IOffsetsAltAz, AltAzOffsetState(alt=dalt, az=daz))

    async def get_fits_header_before(
        self, namespaces: list[str] | None = None, **kwargs: Any
    ) -> dict[str, FitsHeaderEntry]:
        tel = self.brot.telescope._telemetry
        hdr = await BrotBaseTelescope.get_fits_header_before(self)
        hdr["TEL-FOCU"] = FitsHeaderEntry(self.brot.focus.position, "Focus position [mm]")
        hdr["HAOFF"] = FitsHeaderEntry(tel.POSITION.INSTRUMENTAL.HA.OFFSET, "Hour Angle offset")
        hdr["DECOFF"] = FitsHeaderEntry(tel.POSITION.INSTRUMENTAL.DEC.OFFSET, "Declination offset")
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
        if self._pointing_log is not None:
            await self._pointing_log(**data)
        log.info("Pointing measurement written.")


__all__ = ["BrotRaDecTelescope", "BrotAltAzTelescope"]
