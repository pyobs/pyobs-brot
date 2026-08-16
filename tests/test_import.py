"""Smoke tests: import every public module and instantiate each driver class without
hardware, asserting the interfaces they claim.

pybrotlib is a pure-Python MQTT client that connects at connect() time, not at
construction, so instantiation is safe with no broker running.
"""

from pyobs.interfaces import (
    IDome,
    IFocuser,
    IOffsetsAltAz,
    IOffsetsRaDec,
    IPointingAltAz,
    IPointingRaDec,
    IRoof,
    ITemperatures,
)
from pyobs.modules import Module

from pyobs_brot import BrotDome, BrotRaDecTelescope, BrotRoof
from pyobs_brot.brottelescope import BrotAltAzTelescope


def test_instantiate_dome() -> None:
    dome = BrotDome(host="localhost", name="dome")
    assert isinstance(dome, Module)
    assert isinstance(dome, IDome)


def test_instantiate_roof() -> None:
    roof = BrotRoof(host="localhost", name="roof")
    assert isinstance(roof, Module)
    assert isinstance(roof, IRoof)


def test_instantiate_radec_telescope() -> None:
    telescope = BrotRaDecTelescope(host="localhost", name="telescope")
    assert isinstance(telescope, Module)
    assert isinstance(telescope, IFocuser)
    assert isinstance(telescope, ITemperatures)
    assert isinstance(telescope, IPointingRaDec)
    assert isinstance(telescope, IPointingAltAz)
    assert isinstance(telescope, IOffsetsRaDec)


def test_instantiate_altaz_telescope() -> None:
    telescope = BrotAltAzTelescope(host="localhost", name="telescope")
    assert isinstance(telescope, Module)
    assert isinstance(telescope, IOffsetsAltAz)
