BROTlib module for *pyobs*
==========================

This is a [pyobs](https://www.pyobs.org) module for telescopes, domes and roll-off roofs controlled via
[BROTlib](https://pypi.org/project/pybrotlib/) over MQTT.


Install *pyobs-brot*
---------------------
Clone the repository:

    git clone https://github.com/pyobs/pyobs-brot.git
    cd pyobs-brot

Install it with [uv](https://docs.astral.sh/uv/):

    uv sync

Alternatively, with plain `venv`/`pip`:

    python3 -m venv .venv
    source .venv/bin/activate
    pip install .


Configuration
-------------
*BrotRaDecTelescope* (or its base class *BrotBaseTelescope*), *BrotDome* and *BrotRoof* all connect to the same
MQTT broker used by BROTlib:

    host:
        Hostname of the MQTT broker.
    name:
        Name of the BROT device.
    port:
        MQTT port (default: 1883).

A basic telescope configuration would look like this:

    class: pyobs_brot.BrotRaDecTelescope
    name: My telescope
    host: 1.2.3.4
    roof: roof
    dome: dome

    # communication
    comm:
      jid: test@example.com
      password: ***

*BrotDome* and *BrotRoof* follow the same pattern, just with `class: pyobs_brot.BrotDome` respectively
`class: pyobs_brot.BrotRoof`.


Dependencies
------------
* [pyobs-core](https://github.com/pyobs/pyobs-core) for the core functionality.
* [pybrotlib](https://pypi.org/project/pybrotlib/) for communicating with BROT devices.
* [paho-mqtt](https://pypi.org/project/paho-mqtt/) as the MQTT client used by BROTlib.
* [Astropy](http://www.astropy.org/) for FITS file handling.

### Calibration status

The telescope driver treats READY_STATE 0.7 (initializing) and 0.8 (calibrating,
including automatic startup positioning) as initializing/not ready. `init()`
continues waiting for 1.0. PLC firmware must reserve 1.0 for completed startup. pybrotlib adds a named
CALIBRATING status; the driver also recognizes raw 0.8 with older pybrotlib
versions during staged upgrades. Upgrade Python consumers before enabling the
new PLC state.
