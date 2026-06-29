# pyobs 2.0 — Phase 5: External Hardware Modules

This document covers the migration of this pyobs hardware module to the
pyobs 2.0 API.

---

## Step 0 — Branch main/master into 1.x for legacy support

Before making any changes, preserve the current state as a legacy branch.
First check which branch is the default — it may be `main` or `master`:

```bash
git remote show origin | grep "HEAD branch"
```

**If the default branch is `master`, rename it to `main` first:**

```bash
git checkout master
git branch -m master main
git push origin main
git push origin --delete master
# Update the default branch in GitHub repository settings to main
```

Then branch off for legacy support:

```bash
git checkout main
git checkout -b 1.x
git push origin 1.x
```

Create `develop` for all 2.0 work:

```bash
git checkout main
git checkout -b develop
git push origin develop
```

Set `develop` as the default branch in GitHub repository settings.

---

## Step 1 — Add ruff, update tooling

### pyproject.toml changes

1. Bump `pyobs-core` to `>=2.0.0.dev1`.

2. In `[dependency-groups] dev`: add `ruff>=0.9.0`, remove `flake8` and any
   mypy stub packages (`pandas-stubs`, `pyside6-stubs`, etc.) — pyrefly
   doesn't use `.pyi` stubs from PyPI.

3. Add config sections (match pyobs-core develop exactly):

```toml
[tool.ruff]
target-version = "py311"
line-length = 120

[tool.ruff.lint]
select = ["E", "F", "W", "I", "UP", "G"]

[tool.pyrefly]
python-version = "3.11"
```

> **Note on the `G` rule**: it flags `log.info(f"...")` — all logging calls
> must use `%`-style formatting: `log.info("value: %s", val)`.

### .pre-commit-config.yaml

Replace the flake8 hook with ruff:

```yaml
repos:
  - repo: https://github.com/psf/black-pre-commit-mirror
    rev: 25.1.0
    hooks:
      - id: black
        language_version: python3.11
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.9.0
    hooks:
      - id: ruff
```

### Delete .flake8

```bash
rm .flake8
```

### Run once to fix existing issues

```bash
uv run ruff check --fix .
```

---

## Step 2 — Replace get_* with state and capabilities

### Remove getter methods

```python
# BEFORE (pyobs 1.x)
async def get_binning(self) -> tuple[int, int]:
    return self._binning

async def list_binnings(self) -> list[IBinning.State]:
    return [IBinning.State(x=1, y=1), IBinning.State(x=2, y=2)]

async def get_full_frame(self) -> IWindow.State:
    return IWindow.State(x=0, y=0, width=self._width, height=self._height)
```

```python
# AFTER (pyobs 2.0)
# In open():
await self.comm.set_capabilities(IWindow.Capabilities(
    full_frame=IWindow.State(x=0, y=0, width=self._width, height=self._height)
))
await self.comm.set_capabilities(IBinning.Capabilities(
    binnings=[IBinning.State(x=1, y=1), IBinning.State(x=2, y=2)]
))
# Initial states also in open():
await self.comm.set_state(IWindow.State(x=0, y=0, width=self._width, height=self._height))
await self.comm.set_state(IBinning.State(x=self._binning[0], y=self._binning[1]))
# In set_binning():
await self.comm.set_state(IBinning.State(x=x, y=y))
```

### Interface → change pattern

| Old getter | New pattern |
|---|---|
| `get_full_frame()` | `set_capabilities(IWindow.Capabilities(full_frame=...))` in `open()` |
| `get_window()` | remove — no equivalent; current window tracked in `_window` and published via `set_state(IWindow.State(...))` |
| `list_binnings()` | `set_capabilities(IBinning.Capabilities(binnings=[...]))` in `open()` |
| `list_image_formats()` | `set_capabilities(IImageFormat.Capabilities(image_formats=[...]))` in `open()` |
| `get_binning()` | `set_state(IBinning.State(...))` in `set_binning()` + `open()` |
| `get_exposure_status()` | `set_state(IExposure.State(...))` in `_change_exposure_status()` |
| `get_exposure_time()` | `set_state(IExposureTime.State(...))` in `set_exposure_time()` + `open()` |
| `get_image_type()` | `set_state(IImageType.State(...))` in `set_image_type()` + `open()` |
| `get_filter()` | `set_state(IFilters.State(...))` in `set_filter()` + `open()` |
| `list_filters()` | `set_capabilities(IFilters.Capabilities(filters=[...]))` in `open()` |
| `get_focus()` | `set_state(IFocuser.State(...))` in `set_focus()` + `open()` |
| `get_motion_status()` | handled by `MotionStatusMixin` — just call `_change_motion_status()` |
| `is_ready()` | `set_state(IReady.State(ready=...))` in `open()` and when it changes |
| `get_cooling()` | `set_state(ICooling.State(setpoint=..., power=..., enabled=...))` in `set_cooling()` and background task |
| `get_temperatures()` | `set_state(ITemperatures.State(readings=[ITemperatures.Temperature(name=..., value=...)]))` in background polling task |
| `get_gain()` / `get_offset()` | `set_state(IGain.State(gain=..., offset=...))` in `set_gain()`, `set_offset()` + `open()` |
| `get_video()` | `set_capabilities(IVideo.Capabilities(url=...))` in `open()` |

### ICooling and ITemperatures

`ICooling` extends `ITemperatures` but they have separate `State` dataclasses.
Both must be published independently:

```python
# temperatures (from background polling loop)
await self.comm.set_state(
    ITemperatures.State(readings=[ITemperatures.Temperature(name="CCD", value=temp)])
)
# cooling control state
await self.comm.set_state(
    ICooling.State(setpoint=self._setpoint, power=round(power_pct), enabled=True)
)
```

`ICooling.State.power` is typed `int | None` — convert the raw PWM fraction
(`raw / 255 * 100`) with `round()` before passing it.

### __init__.py re-exports

Ruff's `F401` rule requires explicit re-exports. Change:

```python
# before
from .mymodule import MyClass
```

```python
# after
from .mymodule import MyClass as MyClass
```

---

## Step 3 — Add pyobs-independent GUI

Each hardware module should have a standalone Qt GUI that works without pyobs
running — useful for testing hardware directly (e.g. during commissioning).

The pattern from **pyobs-qhyccd** (`pyobs_qhyccd/gui.py`) is the reference.
It uses reusable widgets from `pyobs.utils.gui.*` and talks directly to the
hardware driver without going through pyobs Comm.

### Typical structure

```python
# mymodule/gui.py
import asyncio
import sys
import qasync
from PySide6 import QtWidgets
from pyobs.utils.gui.camera import (
    DataDisplayWidget, BinningWidget, ImageFormatWidget,
    ExposureTimeWidget, ExposeWidget,
)
from .driver import MyDriver


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.driver = MyDriver()
        self.driver.open()
        # ... build UI from pyobs.utils.gui widgets ...

    @qasync.asyncSlot()
    async def _expose_clicked(self) -> None:
        # ... call driver directly ...


def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    asyncio.run(async_main(app), loop_factory=qasync.QEventLoop)


if __name__ == "__main__":
    main()
```

Register as a console script in `pyproject.toml`:

```toml
[project.scripts]
mymodule-gui = "mymodule.gui:main"
```

### Available pyobs GUI widgets (`pyobs.utils.gui`)

- `camera.DataDisplayWidget` — displays FITS images
- `camera.BinningWidget` — binning selector
- `camera.ImageFormatWidget` — INT8/INT16 selector
- `camera.ExposureTimeWidget` — exposure time spinner
- `camera.ExposeWidget` — expose/abort buttons with progress
- `camera.windowingwidget.WindowingWidget` — ROI selector

---

## Step 4 — Testing

Each module should have a `test/` directory with a `local.yaml` config that
uses `LocalComm` so it can be tested without an XMPP server:

```yaml
# test/local.yaml
class: pyobs.modules.MultiModule

vfs: &vfs
  class: pyobs.vfs.VirtualFileSystem
  roots:
    cache:
      class: pyobs.vfs.LocalFile
      root: /tmp/pyobs-test/

modules:
  camera:  # or telescope, focuser, etc.
    class: mymodule.MyCamera
    name: camera
    vfs: *vfs
    comm:
      class: pyobs.comm.local.LocalComm
      name: camera

  gui:
    class: pyobs_gui.GUI
    name: gui
    vfs: *vfs
    comm:
      class: pyobs.comm.local.LocalComm
      name: gui
```

Run with:
```bash
pyobs test/local.yaml
```

---

## Repo checklist

For each external module repo, in order:

- [ ] Branch `main` → `1.x` (legacy)
- [ ] Create `develop` branch
- [ ] Bump `pyobs-core` to `>=2.0.0.dev1` in `pyproject.toml`
- [ ] Add `ruff>=0.9.0` to dev deps; remove `flake8` and mypy stub packages
- [ ] Add `[tool.ruff]`, `[tool.ruff.lint]`, `[tool.pyrefly]` sections to `pyproject.toml`
- [ ] Update `.pre-commit-config.yaml`: replace flake8 hook with ruff
- [ ] Delete `.flake8`
- [ ] Run `ruff check --fix .`
- [ ] Fix re-exports in `__init__.py` (`Foo as Foo`)
- [ ] Convert f-string log calls to `%`-style (`log.info("x: %s", x)`)
- [ ] Remove all `get_*` / `list_*` / `is_ready` methods
- [ ] Add `set_capabilities(...)` calls in `open()`
- [ ] Add `set_state(...)` calls in `open()` (initial values) and wherever values change
- [ ] Add standalone GUI in `<package>/gui.py`
- [ ] Add `test/local.yaml`
- [ ] Update `README.md` to mention pyobs 2.0 requirement

---

## Key pyobs-core 2.0 APIs

```python
# In open() — publish static hardware info
await self.comm.set_capabilities(IWindow.Capabilities(...))
await self.comm.set_capabilities(IBinning.Capabilities(...))

# In open() and wherever values change — publish live state
await self.comm.set_state(IBinning.State(x=2, y=2))
await self.comm.set_state(ICooling.State(setpoint=-20, power=80, enabled=True))

# Presence — set by Module.set_state() automatically
# No manual action needed

# Reading remote state (e.g. telescope pointing for camera WCS)
await self.comm.subscribe_state("telescope", IPointingRaDec, self._on_pointing)
```