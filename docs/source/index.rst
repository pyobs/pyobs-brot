pyobs-brot
##########

This is a `pyobs <https://www.pyobs.org>`_ (`documentation <https://docs.pyobs.org>`_) module for telescopes,
domes and roll-off roofs controlled via `BROTlib <https://pypi.org/project/pybrotlib/>`_ over MQTT.


Example configuration
*********************

This is an example configuration for a telescope::

    class: pyobs_brot.BrotRaDecTelescope
    host: 1.2.3.4
    name: My telescope
    roof: roof
    dome: dome

    # communication
    comm:
      jid: test@example.com
      password: ***

The dome and roof connect to the same BROT device and MQTT broker::

    class: pyobs_brot.BrotDome
    host: 1.2.3.4
    name: My telescope

    # communication
    comm:
      jid: test@example.com
      password: ***

::

    class: pyobs_brot.BrotRoof
    host: 1.2.3.4
    name: My telescope

    # communication
    comm:
      jid: test@example.com
      password: ***


Available classes
*****************

:class:`~pyobs_brot.BrotRaDecTelescope` (based on :class:`~pyobs_brot.BrotBaseTelescope`) drives a telescope,
:class:`~pyobs_brot.BrotDome` a dome, and :class:`~pyobs_brot.BrotRoof` a roll-off roof.

BrotBaseTelescope
=================
.. autoclass:: pyobs_brot.BrotBaseTelescope
   :members:
   :show-inheritance:

BrotRaDecTelescope
==================
.. autoclass:: pyobs_brot.BrotRaDecTelescope
   :members:
   :show-inheritance:

BrotDome
========
.. autoclass:: pyobs_brot.BrotDome
   :members:
   :show-inheritance:

BrotRoof
========
.. autoclass:: pyobs_brot.BrotRoof
   :members:
   :show-inheritance:
