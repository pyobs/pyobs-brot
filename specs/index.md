# Design/planning docs

This repo has no `specs/` structure of its own. Design docs, implementation plans, and ADRs that
concern `pyobs-brot` — including ones actually implemented here — live in `pyobs-core`'s `specs/`
tree instead (`specs/design/`, `specs/plans/`, `specs/adrs/`), each tagged with a `Repos:` line
naming every repo it concerns. See `pyobs-core/CLAUDE.md`'s "Cross-repo docs" section.

Relevant so far:

- `pyobs-core/specs/plans/ejabberd-throughput-benchmarking.md` — XMPP/ejabberd throughput
  benchmarking, including a production incident investigation involving `BrotDome`/
  `BrotRaDecTelescope` on `pyobs-iag50` (disco#info capability fetches silently timing out after a
  few modules restart together) and the `pybrotlib` `MQTTTransport.run()` event-loop-starvation fix
  found along the way.
- `pyobs-core/specs/plans/enforce-state-publishing.md` — the state-publishing convention every
  `Module` follows; `BrotRaDecTelescope`'s `ITemperatures` state had the same gap this doc's
  convention exists to catch (2026-07-27 addendum).
- `pyBROT`'s own `specs/plans/mqtt-reconnect.md` (`BROTLib/pyBROT`, not this repo) — `MQTTTransport`
  auto-reconnect fix closing #68; `pyobs-brot`'s `pybrotlib` floor bumped to `>=1.2.1` and released
  as 2.0.3 to match.
- `pyBROT`'s own `specs/plans/2026-09-22-weather-sources.md` (`BROTLib/pyBROT`, not this repo) —
  pluggable `WeatherSource` (pyobs-weather + local JSON/YAML) and a `WeatherPublisher`, closing
  pyBROT#32; wired up here as `BrotBaseTelescope`'s optional `weather_source`/`weather_url`/
  `weather_path` params (`pyobs_brot/_weather.py`) -- telescope only, not dome/roof, since every
  BROT module opens its own MQTT connection to the same broker and would otherwise publish the
  same reading redundantly. `pybrotlib` floor bumped to `>=1.3.0` to match.
- `pyobs-core/specs/plans/2026-09-14-brot-settle-loop-staleness-and-resend.md` — shared
  `wait_until_settled()` helper for telescope/dome/roof settle loops (staleness detection + setpoint
  resend), closing #61 (`pybrotlib` 1.2.2 / `pyobs-brot` 2.0.4). Root-cause investigation (resend
  confirmed not to explain #61's actual symptom, per the real PLC source) split out to #71.
