"""Central registry of all provider plugins and parser specs.

Provider plugins are defined in each provider package and imported here
explicitly.  ``ALL_PLUGINS`` is the authoritative list of registered
providers; ``ALL_PARSERS`` is the flat list of parser specs derived from it
and consumed by the generic runner.

To add a new bank, create a ``plugin.py`` in its provider package that
exports a ``PLUGIN: ProviderPlugin`` instance, then add it to ``ALL_PLUGINS``
below.
"""

from fintl.etl.common.schemas import ParserSpec, ProviderPlugin
from fintl.etl.providers.dkb.plugin import PLUGIN as DKB_PLUGIN
from fintl.etl.providers.gls.plugin import PLUGIN as GLS_PLUGIN
from fintl.etl.providers.postbank.plugin import PLUGIN as POSTBANK_PLUGIN
from fintl.etl.providers.scalable.plugin import PLUGIN as SCALABLE_PLUGIN

ALL_PLUGINS: list[ProviderPlugin] = [
    DKB_PLUGIN,
    POSTBANK_PLUGIN,
    SCALABLE_PLUGIN,
    GLS_PLUGIN,
]

ALL_PARSERS: list[ParserSpec] = [
    spec for plugin in ALL_PLUGINS for spec in plugin.all_parsers()
]
