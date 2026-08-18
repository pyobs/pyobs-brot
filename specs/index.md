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
