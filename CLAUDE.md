# CLAUDE.md

Entry points for working in this repo.

## What this is

`pyobs-brot` is a `pyobs` module for telescopes, domes, and roll-off roofs controlled via
[BROTlib](https://pypi.org/project/pybrotlib/) over MQTT (`BrotRaDecTelescope`/`BrotBaseTelescope`,
`BrotDome`, `BrotRoof`).

## Design history and planning

This repo has no `specs/` structure of its own — design docs, implementation plans, and ADRs that
concern `pyobs-brot` (including ones actually implemented here) live in `pyobs-core`'s `specs/`
tree instead, tagged with a `Repos:` line. See `specs/index.md` for what's relevant so far, and
`pyobs-core/CLAUDE.md`'s "Cross-repo docs" section for the convention.

## Tooling

- Lint: `ruff` (config in `pyproject.toml`)
- Format: `black`
- Type checking: `pyrefly`
- Tests: `pytest` (`asyncio_mode = "strict"`)
