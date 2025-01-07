import asyncio
from datetime import datetime, timezone
import logging
import os.path
import time
from typing import Tuple, List, Dict, Any, Optional, NamedTuple, Union
from astropy.coordinates import SkyCoord
import astropy.units as u
import numpy as np
import paho.mqtt.client as mqtt

from pyobs.mixins import FitsNamespaceMixin
from pyobs.events import FilterChangedEvent, OffsetsAltAzEvent
from pyobs.interfaces import IFilters, IFocuser, ITemperatures, IOffsetsAltAz, IPointingSeries
from pyobs.modules import timeout
from pyobs.modules.telescope.basetelescope import BaseTelescope
from pyobs.utils.enums import MotionStatus, ModuleState
from pyobs.utils.threads import LockWithAbort
from pyobs.utils import exceptions as exc
from pyobs.utils.time import Time


log = logging.getLogger(__name__)


class BrotTelescope(BaseTelescope, IOffsetsAltAz, IFocuser, ITemperatures, IPointingSeries, FitsNamespaceMixin):
    def __init__(
        self,
        host: str,
        port: int,
        keepalive: int = 60,
        **kwargs: Any,
    ):
        BaseTelescope.__init__(self, **kwargs, motion_status_interfaces=["ITelescope", "IFocuser"])

        mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        mqttc.on_connect = self._on_connect
        mqttc.on_message = self._on_message
        mqttc.connect(host, port, keepalive)

    def _on_connect(self, client, userdata, flags, rc, properties):
        pass

    def _on_message(self, client, userdata, msg):
        pass


__all__ = ["BrotTelescope"]
