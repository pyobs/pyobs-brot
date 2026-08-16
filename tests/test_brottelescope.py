"""Unit tests for the non-hardware constructor logic in the BROT telescope drivers."""

from pyobs_brot import BrotRaDecTelescope


def test_constructor_defaults() -> None:
    telescope = BrotRaDecTelescope(host="localhost", name="telescope")
    assert telescope.temperatures == {}
    assert telescope.focus_offset == 0.0
    assert telescope._roof == "None"


def test_constructor_temperatures_and_roof() -> None:
    telescope = BrotRaDecTelescope(
        host="localhost",
        name="telescope",
        temperatures={"ccd": "T1"},
        roof="some-roof",
    )
    assert telescope.temperatures == {"ccd": "T1"}
    assert telescope._roof == "some-roof"
