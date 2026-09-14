import asyncio
import time
from collections.abc import Awaitable, Callable

from pybrotlib.transport.transport import Transport
from pyobs.utils import exceptions as exc

DEFAULT_RESEND_INTERVAL = 20.0
DEFAULT_STALE_AFTER = 5.0
DEFAULT_POLL_INTERVAL = 0.1


def _check_alive(transport: Transport, stale_after: float) -> None:
    age = transport.telemetry_age()
    if not transport.connected or age is None or age > stale_after:
        raise exc.MoveError("Telemetry stream stalled while waiting to settle.")


async def wait_until_settled(
    condition: Callable[[], bool],
    transport: Transport,
    *,
    resend: Callable[[], Awaitable[None]] | None = None,
    resend_interval: float = DEFAULT_RESEND_INTERVAL,
    stale_after: float = DEFAULT_STALE_AFTER,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
) -> None:
    """Wait for `condition()` to become true, driven by `transport`'s telemetry.

    Unlike a bare polling loop, this fails fast with a distinct `MoveError` when the
    telemetry stream has stalled (disconnected, or no message for `stale_after` seconds)
    instead of silently spinning on frozen data until an outer method timeout fires. If
    `resend` is given, it is called every `resend_interval` seconds while still waiting,
    to recover from a command that was silently dropped -- only safe for idempotent
    absolute-setpoint commands, not one-shot triggers.
    """
    last_resend = time.monotonic()
    while not condition():
        _check_alive(transport, stale_after)
        if resend is not None and time.monotonic() - last_resend >= resend_interval:
            await resend()
            last_resend = time.monotonic()
        await asyncio.sleep(poll_interval)


async def check_settling_alive(transport: Transport, stale_after: float = DEFAULT_STALE_AFTER) -> None:
    """Raise MoveError if `transport`'s telemetry has stalled.

    For polling loops that need their own per-iteration logic beyond a single boolean
    condition (e.g. also detecting a hardware ERROR state) -- call this once per iteration
    alongside the loop's own checks, instead of using `wait_until_settled`.
    """
    _check_alive(transport, stale_after)
