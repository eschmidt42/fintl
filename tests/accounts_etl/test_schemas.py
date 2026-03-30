"""Unit tests for ServicePlugin and ProviderPlugin in schemas.py."""

from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from fintl.accounts_etl.schemas import (
    Case,
    Config,
    Logging,
    ParserSpec,
    Provider,
    ProviderPlugin,
    ServicePlugin,
    Sources,
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


# ── Provider ──────────────────────────────────────────────────────────────────


def test_provider_check_path_is_valid_none():
    """When a Provider field is explicitly set to None the validator must return None
    without attempting to normalise or sanity-check the path."""
    provider = Provider(giro=None)
    assert provider.giro is None


def test_sources_at_least_one_source_raises_when_all_none():
    """Sources with every provider set to None must raise a ValidationError."""
    with pytest.raises(ValidationError):
        Sources(dkb=None, postbank=None, scalable=None, gls=None)


# ── Config ────────────────────────────────────────────────────────────────────

_LOGGER_PATH = Path(__file__).parent.parent / "logger-config.json"


def _config(tmp_path: Path) -> Config:
    return Config(
        target_dir=tmp_path,
        sources=Sources(dkb=Provider(giro=tmp_path)),
        logging=Logging(config_file=_LOGGER_PATH),
    )


def test_config_repr_rich(tmp_path: Path):
    """__repr_rich__ must yield (key, value) pairs without raising."""
    config = _config(tmp_path)
    items = list(config.__repr_rich__())
    keys = [item[0] for item in items]
    assert "sources" in keys
    assert "target" in keys


def test_config_get_logger_config_path_no_config_file(tmp_path: Path):
    """get_logger_config_path must log an error and return None when
    logging.config_file is None (the default)."""
    config = Config(
        target_dir=tmp_path,
        sources=Sources(dkb=Provider(giro=tmp_path)),
        logging=Logging(),  # config_file defaults to None
    )
    result = config.get_logger_config_path()
    assert result is None


def test_config_get_logger_config_path_with_config_file(tmp_path: Path):
    """get_logger_config_path must return the resolved path when config_file is set."""
    config_file = tmp_path / "logger.json"
    config_file.write_text("{}")

    config = Config(
        sources=Sources(dkb=Provider(giro=tmp_path)),
        target_dir=tmp_path,
        logging=Logging(config_file=config_file),
    )
    result = config.get_logger_config_path()
    assert result == config_file.resolve().absolute()
