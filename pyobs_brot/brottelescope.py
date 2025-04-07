import asyncio
from dataclasses import dataclass
import logging
from typing import Tuple, Dict, Any, Optional, get_type_hints, List
import paho.mqtt.client as mqtt

from pyobs.mixins import FitsNamespaceMixin
from pyobs.interfaces import IFocuser, ITemperatures, IOffsetsAltAz, IPointingSeries
from pyobs.modules.telescope.basetelescope import BaseTelescope
from pyobs.utils.enums import MotionStatus

log = logging.getLogger(__name__)


@dataclass
class Telemetry:
    error: bool = False
    ready: bool = False
    busy: bool = False
    sliding: bool = False
    tracking: bool = False
    stopped: bool = False
    homed: bool = False
    parked: bool = False
    Azimuth: float = 0.0
    Elevation: float = 0.0
    azimuth_offset: float = 0.0
    elevation_offset: float = 0.0
    RightAscension: float = 0.0
    Declination: float = 0.0
    FocusPosition: float = 0.0


class BrotTelescope(BaseTelescope, IOffsetsAltAz, IFocuser, ITemperatures, IPointingSeries, FitsNamespaceMixin):
    def __init__(
        self,
        host: str,
        port: int = 1883,
        keepalive: int = 60,
        **kwargs: Any,
    ):
        BaseTelescope.__init__(self, **kwargs, motion_status_interfaces=["ITelescope", "IFocuser"])

        self.mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqttc.on_connect = self._on_connect
        self.mqttc.on_message = self._on_message
        self.mqttc.connect(host, port, keepalive)

        self.telemetry = Telemetry()

        # update loop
        self.add_background_task(self._update)

        # mixins
        FitsNamespaceMixin.__init__(self, **kwargs)

    async def open(self):
        await BaseTelescope.open(self)
        self.mqttc.loop_start()

    async def close(self):
        await BaseTelescope.close(self)
        self.mqttc.loop_stop()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        print(f"Connected with result code {reason_code}")
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe("MONETN/Telemetry/#")

    def _on_message(self, client, userdata, msg):
        key, value = msg.payload.decode("utf-8").split(" ")[1].split("=")
        if hasattr(self.telemetry, key):
            typ = get_type_hints(self.telemetry)[key]
            if typ == bool:
                value = value.lower() == "true"
            else:
                value = float(value)
            setattr(self.telemetry, key, value)

    async def _update(self):
        while True:
            if self.telemetry.error:
                await self._change_motion_status(MotionStatus.ERROR)
            elif self.telemetry.sliding:
                await self._change_motion_status(MotionStatus.SLEWING)
            elif self.telemetry.tracking:
                await self._change_motion_status(MotionStatus.TRACKING)
            elif self.telemetry.parked:
                await self._change_motion_status(MotionStatus.PARKED)
            else:
                await self._change_motion_status(MotionStatus.IDLE)
            await asyncio.sleep(5)

    async def _move_radec(self, ra: float, dec: float, abort_event: asyncio.Event) -> None:
        # await self._change_motion_status(MotionStatus.SLEWING)
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command rightascension={ra}")
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command declination={dec}")
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command track=1")
        # await self._change_motion_status(MotionStatus.TRACKING)

    async def _move_altaz(self, alt: float, az: float, abort_event: asyncio.Event) -> None:
        # await self._change_motion_status(MotionStatus.SLEWING)
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command elevation={alt}")
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command azimuth={az}")
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command slew=1")
        # await self._change_motion_status(MotionStatus.POSITIONED)

    async def set_offsets_altaz(self, dalt: float, daz: float, **kwargs: Any) -> None:
        pass

    async def get_offsets_altaz(self, **kwargs: Any) -> Tuple[float, float]:
        return 0, 0

    async def set_focus(self, focus: float, **kwargs: Any) -> None:
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command focus={focus}")
        await asyncio.sleep(2)

    async def set_focus_offset(self, offset: float, **kwargs: Any) -> None:
        pass

    async def get_focus(self, **kwargs: Any) -> float:
        return self.telemetry.FocusPosition

    async def get_focus_offset(self, **kwargs: Any) -> float:
        return 0

    async def init(self, **kwargs: Any) -> None:
        # await self._change_motion_status(MotionStatus.INITIALIZING)
        log.info("Initializing telescope...")
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command power=true")

    async def park(self, **kwargs: Any) -> None:
        # await self._change_motion_status(MotionStatus.PARKING)
        log.info("Parking telescope...")
        self.mqttc.publish("MONETN/Telescope/SET", payload=f"command park=true")

    async def stop_motion(self, device: Optional[str] = None, **kwargs: Any) -> None:
        pass

    async def is_ready(self, **kwargs: Any) -> bool:
        return True

    async def get_altaz(self, **kwargs: Any) -> Tuple[float, float]:
        return self.telemetry.Elevation, self.telemetry.Azimuth

    async def get_radec(self, **kwargs: Any) -> Tuple[float, float]:
        return self.telemetry.RightAscension, self.telemetry.Declination

    async def get_temperatures(self, **kwargs: Any) -> Dict[str, float]:
        pass

    async def start_pointing_series(self, **kwargs: Any) -> str:
        pass

    async def stop_pointing_series(self, **kwargs: Any) -> None:
        pass

    async def add_pointing_measure(self, **kwargs: Any) -> None:
        pass

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
        hdr["TEL-FOCU"] = (self.telemetry.FocusPosition, "Focus position [mm]")
        # "TEL-ROT": ("POSITION.INSTRUMENTAL.DEROTATOR[2].REALPOS", "Derotator instrumental position at end [deg]"),
        # "DEROTOFF": ("POINTING.SETUP.DEROTATOR.OFFSET", "Derotator offset [deg]"),
        # "AZOFF": ("POSITION.INSTRUMENTAL.AZ.OFFSET", "Azimuth offset"),
        # "ALTOFF": ("POSITION.INSTRUMENTAL.ZD.OFFSET", "Altitude offset"),

        # return it
        return self._filter_fits_namespace(hdr, namespaces=namespaces, **kwargs)


__all__ = ["BrotTelescope"]
