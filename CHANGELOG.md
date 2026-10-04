# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Entries for releases before this file existed were generated from commit subjects.

## [2.1.0] - 2026-09-22

- Fix pyrefly: use a real Transport() in test_weather.py, not object()
- Wire up optional weather publishing on BrotBaseTelescope
- Point specs index at pyobs-brot#71, split from #61

## [2.0.4] - 2026-09-14

- Detect stalled telemetry and resend idempotent setpoints in settle loops
- Point specs index at pyBROT's mqtt-reconnect plan for #68

## [2.0.3] - 2026-09-14

- Add CLAUDE.md entry point pointing to specs/ conventions and tooling

## [2.0.2] - 2026-09-03

- Add DOMESHUT/FOCOFF/PNTHAOF/PNTDCOF/TEMP-<sensor> FITS headers (#872)

## [2.0.1] - 2026-09-01

- Maintenance release (dependency and metadata updates only).

## [2.0.0] - 2026-08-26

- Require stable pyobs-core>=2.0.0
- Gate auto-merge on the PR author, not the event actor
- Enable Dependabot auto-merge for patch/minor updates
- Give get_fits_header_before() an explicit sender default
- Convert BrotBaseTelescope to cooperative super().__init__() chain
- Gate init() on good weather in telescope, roof, and dome
- brottelescope: fix inverted convergence-wait in set_offsets_radec
- Add baseline test suite and CI (pytest, pyrefly), grouped Dependabot
- Rename specs/README.md to index.md
- Upgrade uv.lock to clear open Dependabot alerts
- Require pyobs-core>=2.0.0.dev48
- Add specs/README.md pointer to pyobs-core's cross-repo design docs
- Publish an ITemperatures placeholder synchronously in open()
- Require pybrotlib>=1.1.5
- Log when configured temperature sensors go missing from telemetry
- Add dependabot.yml, targeting develop for PRs
- Periodically republish IFocuser state in BrotBaseTelescope
- Stop repeated error-state log spam from roof/telescope status polling
- Raise InitError/ParkError instead of silently returning on hardware error
- Update to pyobs-core 2.0.0.dev10, apply FitsHeaderEntry to get_fits_header_before
- Add Sphinx documentation
- Fix ruff workflow checking the wrong directory
- Write proper README with install and configuration docs
- update pyobs-core to 2.0.0.dev6
- new pyobs-core
- refactor BrotTelescope: improve readability in state publishing logic
- remove DEVELOPMENT.md from pyobs-brot module
- Add Ruff CI workflows for pyobs-qhyccd and pyobs-zaber
- refactor BrotTelescope and BrotDome: standardize state publishing with updated state classes
- refactor BrotTelescope: use `await` for MQTT close, refine status match cases, and add null checks for `_pointing_log`
- fix BrotDome: correct typo in shutter status attribute
- add pyrefly to dev dependencies and upgrade pyobs-core to 2.0.0.dev3
- refactor BrotTelescope: optimize imports, clean up comments, improve status handling, and enhance telemetry updates
- optimize imports in BrotRoof module
- refactor BrotDome to clean up comments, optimize imports, and improve status updates
- add pre-commit configuration with Black and Ruff
- migrate to pyobs-core 2.0, switch to ruff for linting, and update dependencies
- add DEVELOPMENT.md for pyobs 2.0 migration steps

## [1.0.0] - 2026-05-26

- require new brotlib version
- changed underscore parameters for vfs, comm, etc

## [0.2.0] - 2026-05-16

- changes for pyobs 1.42
- renamed Object parameters (comm, observer, ...) to start with an underscore
- ICRS update

## [0.1.20] - 2026-04-14

- new is_ready method

## [0.1.19] - 2026-03-22

- wait for focus

## [0.1.18] - 2026-03-22

- wait for focus

## [0.1.17] - 2026-03-22

- added temperatures
- fixed bug with wrong enum

## [0.1.16] - 2026-03-22

- focus status
- added stop

## [0.1.15] - 2026-03-10

- removed state changes

## [0.1.14] - 2026-03-10

- close mqtt connection

## [0.1.13] - 2026-03-10

- fixed bug

## [0.1.12] - 2026-03-10

- checking movement only in ONLINE state

## [0.1.11] - 2026-03-10

- if to elif

## [0.1.10] - 2026-03-10

- regular updates

## [0.1.9] - 2026-03-10

- changed imports

## [0.1.8] - 2026-03-10

- changed import

## [0.1.7] - 2026-03-10

- upgraded deps, changed import
- added timeout
- changed and to or
- use TARGET_DISTANCE for waiting for set_offsets_altaz

## [0.1.6] - 2026-01-20

- use TARGET_DISTANCE for waiting for set_offsets_altaz

## [0.1.5] - 2026-01-15

- wait for tracking

## [0.1.4] - 2026-01-15

- fixed bug with unknown variable

## [0.1.0] - 2026-01-07

- added BrotRoof
- new BrotRoof

## [0.0.7] - 2025-12-24

- cleaned parent classes
- logging
- fixed bug
- implemented add_pointing_measurement
- only kept add_pointing_measurement in IPointintSeries
- can init to tracking
- wait for tracking in set_offsets_altaz

## [0.0.6] - 2025-11-10

- removed Qt

## [0.0.5] - 2025-11-10

- removed Qt

## [0.0.4] - 2025-11-10

- pypi action

## [0.0.3] - 2025-11-10

- Maintenance release (dependency and metadata updates only).

## [0.0.2] - 2025-11-10

- cleaned up
- to uv
- telescope update
- brotdome update
- wait for dome
- brotdome v1.1
- bug fixes
- update of the telescope control
- dome control module v1.0
- little update
- Create .gitignore
- update test 1
- telemetry
- pointing model
- fixed bug
- hierarchical variable names
- nested variable names
- sleep longer on offset
- added missing base classes
- temps
- fixed focus offset
- focus offset
- alt/az offsets
- sleep a little after setting focus
- focus in fits header
- focus position
- park
- topic
- logging
- slew/track
- MotionStatus
- basic telescope module for fetching telemetry from a BROT telescope
- initial commit
