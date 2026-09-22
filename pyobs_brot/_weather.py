from pybrotlib.transport.transport import Transport
from pybrotlib.weather import FileWeatherSource, PyobsWeatherSource, WeatherPublisher, WeatherSource


def build_weather_publisher(
    transport: Transport,
    telescope_name: str,
    *,
    source: str | None,
    url: str | None = None,
    path: str | None = None,
    interval: float = 60.0,
    max_age: float | None = 300.0,
) -> WeatherPublisher | None:
    """Build a WeatherPublisher from module config, or None if `source` isn't set.

    `source` is "pyobs" (queries `url`, a pyobs-weather instance's /api/current/) or "file"
    (reads `path`, a local JSON/YAML file). Absent `source` -> no weather publishing, e.g. sites
    without a source yet -- only one BROT module per site should be given a weather_source, since
    every BrotBaseTelescope/BrotDome/BrotRoof instance opens its own MQTTTransport to the same
    broker and would otherwise publish the same reading redundantly.
    """
    if source is None:
        return None

    weather_source: WeatherSource
    if source == "pyobs":
        if url is None:
            raise ValueError("weather_source='pyobs' requires weather_url to be set.")
        weather_source = PyobsWeatherSource(url)
    elif source == "file":
        if path is None:
            raise ValueError("weather_source='file' requires weather_path to be set.")
        weather_source = FileWeatherSource(path)
    else:
        raise ValueError(f"Unknown weather_source: {source!r}")

    return WeatherPublisher(transport, telescope_name, weather_source, interval=interval, max_age=max_age)
