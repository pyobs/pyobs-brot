"""Unit tests for build_weather_publisher()'s config validation and wiring."""

import pytest
from pybrotlib.transport.transport import Transport
from pybrotlib.weather import FileWeatherSource, PyobsWeatherSource, WeatherPublisher

from pyobs_brot._weather import build_weather_publisher


def test_no_source_returns_none() -> None:
    assert build_weather_publisher(Transport(), "telescope", source=None) is None


def test_pyobs_source_builds_publisher() -> None:
    publisher = build_weather_publisher(Transport(), "telescope", source="pyobs", url="https://weather.example.org")
    assert isinstance(publisher, WeatherPublisher)
    assert publisher.site == "telescope"
    assert isinstance(publisher.source, PyobsWeatherSource)


def test_pyobs_source_without_url_raises() -> None:
    with pytest.raises(ValueError, match="weather_url"):
        build_weather_publisher(Transport(), "telescope", source="pyobs")


def test_file_source_builds_publisher() -> None:
    publisher = build_weather_publisher(Transport(), "telescope", source="file", path="/tmp/weather.json")
    assert isinstance(publisher, WeatherPublisher)
    assert isinstance(publisher.source, FileWeatherSource)


def test_file_source_without_path_raises() -> None:
    with pytest.raises(ValueError, match="weather_path"):
        build_weather_publisher(Transport(), "telescope", source="file")


def test_unknown_source_raises() -> None:
    with pytest.raises(ValueError, match="Unknown weather_source"):
        build_weather_publisher(Transport(), "telescope", source="bogus")


def test_interval_and_max_age_are_threaded_through() -> None:
    publisher = build_weather_publisher(
        Transport(), "telescope", source="pyobs", url="https://weather.example.org", interval=30.0, max_age=120.0
    )
    assert publisher is not None
    assert publisher.interval == 30.0
    assert publisher.max_age == 120.0
