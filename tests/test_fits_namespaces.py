from unittest.mock import MagicMock, patch

import pytest
from pyobs.interfaces import FitsHeaderEntry
from pyobs.modules.telescope.basetelescope import BaseTelescope

from pyobs_brot.brottelescope import BrotAltAzTelescope, BrotRaDecTelescope


@pytest.mark.asyncio
@pytest.mark.parametrize("klass", [BrotRaDecTelescope, BrotAltAzTelescope])
@pytest.mark.parametrize("sender, namespaces", [("camera", None), ("other", ["camera"]), (None, ["camera"])])
async def test_header_overrides_forward_context_and_filter_all_driver_cards(klass, sender, namespaces):
    telescope = klass(host="localhost", name="telescope", fits_namespaces={"camera": ["TEL-RA", "TEL-FOCU"]})
    telescope.brot.telescope = MagicMock()
    telescope.brot.focus = MagicMock()
    telescope.brot.focus.position = 12
    seen = []

    async def filtered_base(self, namespaces=None, **kwargs):
        seen.append((namespaces, kwargs["sender"]))
        return self._filter_fits_namespace({"TEL-RA": FitsHeaderEntry(10, "RA")}, namespaces=namespaces, **kwargs)

    kwargs = {} if sender is None else {"sender": sender}
    with patch.object(BaseTelescope, "get_fits_header_before", filtered_base):
        header = await telescope.get_fits_header_before(namespaces=namespaces, **kwargs)
    assert seen == [(namespaces, sender or "")]
    assert set(header) == {"TEL-RA", "TEL-FOCU"}
    assert header["TEL-RA"].value == 10
    assert header["TEL-FOCU"].value == 12
