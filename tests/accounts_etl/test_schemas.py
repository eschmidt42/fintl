"""Unit tests for ServicePlugin and ProviderPlugin in schemas.py."""

from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

import pytest

from fintl.accounts_etl.schemas import (
    Case,
    ParserSpec,
    ProviderPlugin,
    ServicePlugin,
)


def _spec(provider: str, service: str, parser: str, precedence: int = 0) -> ParserSpec:
    return ParserSpec(
        case=Case(provider=provider, service=service, parser=parser),
        applies=MagicMock(return_value=True),
        run=MagicMock(),
        precedence=precedence,
    )


# ── ServicePlugin ─────────────────────────────────────────────────────────────


def test_service_plugin_stores_name_and_parsers():
    spec = _spec("dkb", "giro", "giro0")
    svc = ServicePlugin(name="giro", parsers=(spec,))

    assert svc.name == "giro"
    assert svc.parsers == (spec,)


def test_service_plugin_empty_parsers():
    svc = ServicePlugin(name="giro", parsers=())
    assert svc.parsers == ()


def test_service_plugin_is_frozen():
    svc = ServicePlugin(name="giro", parsers=())
    with pytest.raises(FrozenInstanceError):
        setattr(svc, "name", "credit")


# ── ProviderPlugin ────────────────────────────────────────────────────────────


def test_provider_plugin_stores_name_and_services():
    svc = ServicePlugin(name="giro", parsers=())
    plugin = ProviderPlugin(name="dkb", services=(svc,))

    assert plugin.name == "dkb"
    assert plugin.services == (svc,)


def test_provider_plugin_is_frozen():
    plugin = ProviderPlugin(name="dkb", services=())
    with pytest.raises(FrozenInstanceError):
        setattr(plugin, "name", "postbank")


# ── ProviderPlugin.all_parsers ────────────────────────────────────────────────


def test_all_parsers_returns_empty_for_no_services():
    plugin = ProviderPlugin(name="dkb", services=())
    assert plugin.all_parsers() == ()


def test_all_parsers_returns_empty_for_services_with_no_parsers():
    plugin = ProviderPlugin(
        name="dkb",
        services=(
            ServicePlugin(name="giro", parsers=()),
            ServicePlugin(name="credit", parsers=()),
        ),
    )
    assert plugin.all_parsers() == ()


def test_all_parsers_flattens_single_service():
    spec_a = _spec("dkb", "giro", "giro0", precedence=0)
    spec_b = _spec("dkb", "giro", "giro202312", precedence=20)
    plugin = ProviderPlugin(
        name="dkb",
        services=(ServicePlugin(name="giro", parsers=(spec_a, spec_b)),),
    )

    assert plugin.all_parsers() == (spec_a, spec_b)


def test_all_parsers_flattens_multiple_services_in_order():
    giro_spec = _spec("dkb", "giro", "giro0")
    credit_spec = _spec("dkb", "credit", "credit0")
    tagesgeld_spec = _spec("dkb", "tagesgeld", "tagesgeld0")

    plugin = ProviderPlugin(
        name="dkb",
        services=(
            ServicePlugin(name="giro", parsers=(giro_spec,)),
            ServicePlugin(name="credit", parsers=(credit_spec,)),
            ServicePlugin(name="tagesgeld", parsers=(tagesgeld_spec,)),
        ),
    )

    assert plugin.all_parsers() == (giro_spec, credit_spec, tagesgeld_spec)


def test_all_parsers_preserves_parser_order_within_service():
    spec_0 = _spec("dkb", "giro", "giro0", precedence=0)
    spec_10 = _spec("dkb", "giro", "giro202307", precedence=10)
    spec_20 = _spec("dkb", "giro", "giro202312", precedence=20)

    plugin = ProviderPlugin(
        name="dkb",
        services=(ServicePlugin(name="giro", parsers=(spec_0, spec_10, spec_20)),),
    )

    result = plugin.all_parsers()
    assert [s.case.parser for s in result] == ["giro0", "giro202307", "giro202312"]
