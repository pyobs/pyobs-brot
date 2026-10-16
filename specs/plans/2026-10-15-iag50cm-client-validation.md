# Plan: pyobs-brot client validation on IAG50cm

Status: proposed. The site-level half of this work — deployment config, credentials, camera/
plate-solve chain, pointing model — is tracked separately where those private site details live;
this plan covers only the `pyobs-brot` module itself.

Repos: pyobs-brot, pybrotlib (`BROTLib/pyBROT`, not on the `pyobs` GitHub org)

Issues: none yet. File findings against `pyobs/pyobs-brot` as they arise. Prior art:
`pyobs-core/specs/plans/2026-09-14-brot-settle-loop-staleness-and-resend.md` (shipped in pybrotlib
1.2.2 / pyobs-brot 2.0.4).

## Problem

`pyobs-brot` has never been validated end-to-end against a live BROT telescope. What exists today is
unit tests against a mocked `pybrotlib` client and incidental daylight exposure through the GUI; the
telescope-facing behaviour that an acquisition/pointing loop depends on has not been exercised.
Before `BrotRaDecTelescope` (or `BrotAltAzTelescope`) is trusted to acquire and track a target by
plate-solving and applying offsets, the client itself must be validated — otherwise a client bug and
a mount defect are indistinguishable from the single observable symptom ("the star ended up
off-centre").

The sharpest coupling is the offset path. pyobs' acquisition loop converges by calling
`set_offsets_radec(dra, ddec)`; `BrotRaDecTelescope` maps that to the mount's HA/Dec offsets
(`ha_offset = -dra*3600`, `dec_offset = ddec*3600`, then `set_offset_ha`/`set_offset_dec`,
`brottelescope.py`), waits for both axes to settle to ≤2″ (`wait_until_settled`), and re-sends the
*absolute* setpoint every 20 s to recover from a silently dropped command. On IAG50cm those offsets
are applied only while tracking and are reset to 0 by every new goto/track (`FB_TelescopeControl`,
lines ~720/970). The 20 s re-send covers the dropped-command case *within one settle*; it does not
cover the offset being cleared by a later goto/track — that cross-command lifetime is exactly what
has to be pinned before the acquisition loop is trusted.

## Scope

- `BrotBaseTelescope` / `BrotRaDecTelescope`: `move_radec`, `move_altaz`, `set_offsets_radec`,
  `set_focus`/`set_focus_offset`, `init`, `park`, `stop_motion`, state publishing (`_update`),
  `get_fits_header_before`.
- `BrotDome` / `BrotRoof`: smoke only — they share the settle/resend helper.
- Out of scope: the site's deployment config, credentials, camera/plate-solve chain and pointing
  model (validated separately), and the telescope's own on-sky pointing/tracking acceptance.

## Method

Drive the module headlessly through a logged `XmppComm` client script (the same RPC layer
`pyobs-gui` uses — the GUI is not required), with the mount's own MQTT/ADS traffic captured in
parallel as ground truth so every observation can be attributed to either `pyobs-brot` or the PLC.

## Layer 1 — offline (no telescope)

- [ ] `pytest` green (10 tests in `tests/test_brottelescope.py`, ~33 across the package), including
  `test_set_offsets_radec_sends_scaled_and_sign_flipped_offsets` (dra → `-dra*3600`, ddec →
  `ddec*3600`) and the settle-convergence wait.
- [ ] Pin the offset contract in a mock test: `set_offsets_radec` sends the absolute offset, waits
  to ≤2″ target distance, and re-sends on staleness — and does **not** claim success just because
  the send completed.
- [ ] Mock tests for the not-ready / ERROR / refused-target paths: `move_radec`/`move_altaz`/
  `park`/`init` must raise or refuse truthfully, never report "reached" against an error state.

## Layer 2 — on the telescope, daylight (no sky needed)

Mount powered, covers open, E-stop within reach; the telescope's own safety rules apply. Most steps
below need no stars.

- [ ] `move_radec` to a catalog target; `IPointingRaDec`/`IPointingAltAz` state matches the PLC's
  reported RA/Dec and Alt/Az (no hours/degrees factor-15, no sign error).
- [ ] `move_altaz` to Az 180 / El 30 and back exercises `_move_altaz`.
- [ ] `set_offsets_radec` while tracking: the mount's HA/Dec offsets move by the right amount and
  sign (check against the PLC's `hourangleoffset`/`declinationoffset`), and `RaDecOffsetState`
  reports the offset back.
- [ ] Offset lifetime: after `set_offsets_radec` has settled, issue a fresh `move_radec`/`track` and
  confirm the PLC cleared the offset; decide and pin whether `pyobs-brot` re-sends it or reports it
  lost (the acquisition loop must then be driven accordingly).
- [ ] Preemption against the mount's limits (IAG50cm #101: a second slew does not replace a running
  slew; goto outranks slew; `gohome` during tracking waits, #72): confirm the client doesn't stall
  and the acquisition loop can't get stuck here.
- [ ] Error/power handling: with the mount in ERROR (or after an E-stop recovery), `init`/`park`/
  `stop_motion`/`move_*` behave truthfully; `ReadyState` and `MotionStatus` follow the PLC.
- [ ] `publish_dut1` and `multi_field_commands`: enable both, confirm UT1-UTC reaches the PLC and a
  single-message `track`/`move` with RA+Dec is accepted (BROTLib#39).
- [ ] FITS headers (`get_fits_header_before`): `HAOFF`/`DECOFF` (instrumental offsets) and the
  pointing-model fields `PNTHAOF`/`PNTDCOF` present and correct; focus-offset composition matches
  the published telescope state.

## Layer 3 — on-sky go/no-go

Only after layers 1–2 pass. The acquisition loop (plate-solve → offsets → `set_offsets_radec`) is
its own acceptance test: a target acquired and tracked, with the *pre-correction* offset logged
before the loop converges, is the pass criterion. The telescope's on-sky pointing/tracking
acceptance (refraction, drift, pole) depends on this and is tracked in the telescope's own test
plan — it must not be credited to `pyobs-brot` before this plan closes.

## Acceptance

- Layers 1–2 green; findings recorded as `pyobs/pyobs-brot` issues; any code fixes merged and
  released.
- The offset-lifetime and preemption contracts are pinned by tests, not just observed once.
- A written go/no-go for letting the acquisition loop drive the telescope, with client-side and
  PLC-side evidence kept separate.

## Open questions

- Should `set_offsets_radec` re-read the mount's offset as authoritative instead of trusting the
  value it sent (decided by the layer-2 offset-survival observation)?
- Is a `BrotDome`/`BrotRoof` smoke test enough, or do they need the same treatment?
