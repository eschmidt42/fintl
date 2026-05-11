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

from fintl.common.logging import Logging
from fintl.common.paths import normalize_path, sanity_check_path
from fintl.etl.common.labels import LabelRule

logger = logging.getLogger(__name__)


class Provider(BaseModel):
    giro: Path | None = None
    tagesgeld: Path | None = None
    credit: Path | None = None
    broker: Path | None = None
    festgeld: Path | None = None

    @field_validator("giro", "tagesgeld", "credit", "broker", "festgeld")
    @classmethod
    def check_path_is_valid(cls, p: Path) -> Path:
        if p is not None:
            p = normalize_path(p)
            sanity_check_path(p)
        return p


class Sources(BaseModel):
    dkb: Provider | None = None
    postbank: Provider | None = None
    scalable: Provider | None = None
    gls: Provider | None = None

    @model_validator(mode="after")
    def at_least_one_source(self) -> "Sources":
        "ensure that at least one source is given"
        if all(v is None for v in [self.dkb, self.postbank, self.scalable, self.gls]):
            raise ValueError("At least one source must be given")
        return self


class OllamaConfig(BaseModel):
    model: str
    base_url: str = "http://localhost:11434/v1"


class Case(BaseModel):
    provider: str
    service: str
    parser: str

    @property
    def name(self) -> str:
        return f"{self.provider}->{self.service}->{self.parser}"


class Config(BaseSettings):
    # https://docs.pydantic.dev/latest/concepts/pydantic_settings/#other-settings-source
    target_dir: Path = Field(default=...)
    sources: Sources = Field(default=...)
    logging: Logging = Logging()
    label_rules: list[LabelRule] = Field(default_factory=list)
    ollama: OllamaConfig | None = None

    model_config = SettingsConfigDict()

    @field_validator("target_dir")
    @classmethod
    def path_valid(cls, p: Path) -> Path:
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
        toml_file = os.environ.get("FINTL_CONFIG", "~/.config/petprojects/fintl.toml")
        return (
            init_settings,
            TomlConfigSettingsSource(settings_cls, toml_file=toml_file),
        )

    def __repr_rich__(self) -> rich.repr.Result:
        yield "sources", self.sources
        yield "target", self.target_dir

    def get_logger_config_path(self) -> Path | None:
        if self.logging is None or self.logging.config_file:
            return Path(self.logging.config_file).resolve().absolute()
        else:
            logger.error(
                f"logging.config_file was not set in the config, cannot return value."
            )

    def get_source_dir(self, provider: str, service: str) -> Path:
        return getattr(getattr(self.sources, provider), service)

    def get_source_dir_from_case(self, case: Case) -> Path:
        return getattr(getattr(self.sources, case.provider), case.service)

    def get_provider(self, provider: str) -> Provider:
        return getattr(self.sources, provider)

    def get_parser_dir(self, case: Case) -> Path:
        return self.target_dir / case.provider / case.service / case.parser

    def get_raw_dir(self, case: Case) -> Path:
        parser_dir = self.get_parser_dir(case)
        return parser_dir / "raw"

    def get_parsed_dir(self, case: Case) -> Path:
        parser_dir = self.get_parser_dir(case)
        return parser_dir / "parsed"
