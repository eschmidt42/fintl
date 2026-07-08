"""Configuration models for fintl using Pydantic-Settings."""

import logging
import os
from pathlib import Path

import rich.repr
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from fintl.common.extraction.constants import ModelProvider
from fintl.common.logging import Logging
from fintl.common.paths import normalize_path, sanity_check_path
from fintl.etl.common.labels import LabelRule

logger = logging.getLogger(__name__)


class Provider(BaseModel):
    """Provider source directory configuration for all account services."""

    giro: Path | None = None
    tagesgeld: Path | None = None
    credit: Path | None = None
    broker: Path | None = None
    festgeld: Path | None = None

    @field_validator("giro", "tagesgeld", "credit", "broker", "festgeld")
    @classmethod
    def check_path_is_valid(cls, p: Path) -> Path:
        """Validate and normalize each service path."""
        if p is not None:
            p = normalize_path(p)
            sanity_check_path(p)
        return p


class Sources(BaseModel):
    """Collection of provider configurations for all supported banks."""

    dkb: Provider | None = None
    postbank: Provider | None = None
    scalable: Provider | None = None
    gls: Provider | None = None

    @model_validator(mode="after")
    def at_least_one_source(self) -> "Sources":
        """Ensure that at least one source is given."""
        if all(v is None for v in [self.dkb, self.postbank, self.scalable, self.gls]):
            raise ValueError("At least one source must be given")
        return self


class OllamaConfig(BaseModel):
    """Configuration for connecting to a local Ollama instance."""

    model: str
    base_url: str = "http://localhost:11434/v1"


class LlamaSwapConfig(BaseModel):
    """Configuration for connecting to a local Ollama instance."""

    model: str
    base_url: str = "http://0.0.0.0:8080"


class Case(BaseModel):
    """Logical identity of a parser (provider, service, parser name)."""

    provider: str
    service: str
    parser: str

    @property
    def name(self) -> str:
        """Return the fully qualified name of this case."""
        return f"{self.provider}->{self.service}->{self.parser}"


class Config(BaseSettings):
    """Top-level application configuration loaded from TOML via Pydantic-Settings."""

    # https://docs.pydantic.dev/latest/concepts/pydantic_settings/#other-settings-source
    target_dir: Path = Field(default=...)
    sources: Sources = Field(default=...)
    logging: Logging = Logging()
    label_rules: list[LabelRule] = Field(default_factory=list)
    model_provider: ModelProvider = Field(default=ModelProvider.ollama)
    model_timeout: int = Field(default=2 * 60)
    ollama: OllamaConfig | None = None
    llama_swap: LlamaSwapConfig | None = None

    model_config = SettingsConfigDict()

    @field_validator("target_dir")
    @classmethod
    def path_valid(cls, p: Path) -> Path:
        """Validate and normalize the target directory path."""
        p = normalize_path(p)
        sanity_check_path(p)
        return p

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Customise settings sources to use init settings and TOML config file."""
        toml_file = os.environ.get("FINTL_CONFIG", "~/.config/petprojects/fintl.toml")
        return (
            init_settings,
            TomlConfigSettingsSource(settings_cls, toml_file=toml_file),
        )

    def __repr_rich__(self) -> rich.repr.Result:
        """Yield key fields for Rich repr display."""
        yield "sources", self.sources
        yield "target", self.target_dir

    def get_logger_config_path(self) -> Path | None:
        """Return the resolved path of the logging config file, if set."""
        if self.logging is None or self.logging.config_file:
            return Path(self.logging.config_file).resolve().absolute()
        else:
            logger.error("logging.config_file was not set in the config, cannot return value.")

    def get_source_dir(self, provider: str, service: str) -> Path:
        """Return the source directory for the given provider and service."""
        return getattr(getattr(self.sources, provider), service)

    def get_source_dir_from_case(self, case: Case) -> Path:
        """Return the source directory for the given Case."""
        return getattr(getattr(self.sources, case.provider), case.service)

    def get_provider(self, provider: str) -> Provider:
        """Return the Provider configuration for the given provider name."""
        return getattr(self.sources, provider)

    def get_parser_dir(self, case: Case) -> Path:
        """Return the parser output directory for the given Case."""
        return self.target_dir / case.provider / case.service / case.parser

    def get_raw_dir(self, case: Case) -> Path:
        """Return the raw input directory for the given Case."""
        parser_dir = self.get_parser_dir(case)
        return parser_dir / "raw"

    def get_parsed_dir(self, case: Case) -> Path:
        """Return the parsed output directory for the given Case."""
        parser_dir = self.get_parser_dir(case)
        return parser_dir / "parsed"
