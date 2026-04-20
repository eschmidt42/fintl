import datetime
import logging
import os
import typing as T
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Literal

import polars as pl
import rich.repr
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from fintl.fine_logging import Logging
from fintl.path_utils import normalize_path, sanity_check_path

logger = logging.getLogger(__name__)


class ServiceEnum(str, Enum):
    # entries need to match the attributes of `Provider`
    giro = "giro"
    tagesgeld = "tagesgeld"
    credit = "credit"
    broker = "broker"
    festgeld = "festgeld"


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


class ProviderEnum(str, Enum):
    # entries need to match the attributes of `Sources`
    dkb = "dkb"
    postbank = "postbank"
    scalable = "scalable"
    gls = "gls"


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


class Case(BaseModel):
    provider: str
    service: str
    parser: str

    @property
    def name(self) -> str:
        return f"{self.provider}->{self.service}->{self.parser}"


class LabelConditionOp(str, Enum):
    contains = "contains"
    not_contains = "not_contains"
    equals = "equals"
    not_equals = "not_equals"


class LabelCondition(BaseModel):
    column: Literal["source", "recipient", "description", "provider"]
    op: LabelConditionOp
    value: str


class LabelRule(BaseModel):
    label: str
    conditions: list[LabelCondition]


class OllamaConfig(BaseModel):
    model: str
    base_url: str = "http://localhost:11434/v1"


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
        settings_cls: T.Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> T.Tuple[PydanticBaseSettingsSource, ...]:
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


class BalanceInfo(BaseModel):
    date: datetime.date
    amount: float
    currency: str
    provider: str
    service: str
    parser: str
    file: str


BALANCE_COLUMNS = [
    "date",
    "amount",
    "currency",
    "provider",
    "service",
    "parser",
    "file",
]

BALANCE_SCHEMA = pl.Schema(
    {
        "date": pl.Date,
        "amount": pl.Float64,
        "currency": pl.String,
        "provider": pl.String,
        "service": pl.String,
        "parser": pl.String,
        "file": pl.String,
    }
)


class DKBGiroParserEnum(str, Enum):
    giro0 = "giro0"
    giro202307 = "giro202307"
    giro202312 = "giro202312"


class PostbankGiroParserEnum(str, Enum):
    giro0 = "giro0"
    giro202305 = "giro202305"


class DKBCreditParserEnum(str, Enum):
    credit0 = "credit0"


class DKBFestgeltParserEnum(str, Enum):
    festgeld0 = "festgeld0"


class DKBTagesgeldParserEnum(str, Enum):
    tagesgeld0 = "tagesgeld0"
    tagesgeld202307 = "tagesgeld202307"
    tagesgeld202312 = "tagesgeld202312"


class GLSGiroParserEnum(str, Enum):
    giro0 = "giro0"


class GLSCreditParserEnum(str, Enum):
    credit0 = "credit0"


class ScalableBrokerParserEnum(str, Enum):
    broker0 = "broker0"
    broker20231028 = "broker20231028"
    broker20260309 = "broker20260309"


class TransactionColumnsEnum(str, Enum):
    source = "source"
    recipient = "recipient"
    amount = "amount"
    description = "description"
    date = "date"
    provider = "provider"
    service = "service"
    parser = "parser"
    file = "file"
    hash = "hash"


TRANSACTION_COLUMNS = [
    TransactionColumnsEnum.source.value,
    TransactionColumnsEnum.recipient.value,
    TransactionColumnsEnum.amount.value,
    TransactionColumnsEnum.description.value,
    TransactionColumnsEnum.date.value,
    TransactionColumnsEnum.provider.value,
    TransactionColumnsEnum.service.value,
    TransactionColumnsEnum.parser.value,
    TransactionColumnsEnum.file.value,
    TransactionColumnsEnum.hash.value,
]
HASH_COLUMNS = [
    TransactionColumnsEnum.date.value,
    TransactionColumnsEnum.provider.value,
    TransactionColumnsEnum.service.value,
    TransactionColumnsEnum.amount.value,
]


@dataclass(frozen=True)
class ParserSpec:
    """Describes one parser implementation that can participate in ETL routing.

    Attributes:
        case: Logical identity of the parser output, including provider, service,
            and parser name. Used for output paths, logging, and concatenation.
        applies: Predicate that receives a candidate source file path and returns
            True when this parser version should claim that file. Expected to be
            deterministic and specific enough that overlap with sibling parsers
            can be detected and rejected.
        run: Callable that executes the parser pipeline given the shared ETL
            config. Typically the parser module's ``main(config)`` function.
        precedence: Explicit ordering used when multiple parser versions exist
            for the same provider and service. Lower values run first.
        source_files_getter: Optional override for source-file discovery. When
            None the standard CSV-based helper from ``file_helper`` is used.
            Provide an alternative for parsers whose source files are not CSVs
            (e.g. Scalable's HTML/PNG files).
    """

    case: Case
    applies: Callable[[Path], bool]
    run: Callable[["Config"], None]
    precedence: int = 0
    source_files_getter: (
        Callable[[Case, "Config", Callable[[Path], bool]], list[Path]] | None
    ) = None


@dataclass(frozen=True)
class ServicePlugin:
    """Groups parser specs that belong to one provider service.

    Attributes:
        name: Service identifier matching the attribute name on ``Provider``
            (e.g. ``"giro"``, ``"credit"``).
        parsers: All ``ParserSpec`` instances for this service. They do not
            need to be pre-sorted; the runner sorts by ``precedence`` at
            execution time.
    """

    name: str
    parsers: tuple[ParserSpec, ...]


@dataclass(frozen=True)
class ProviderPlugin:
    """Owns the complete ETL definition for one bank provider.

    Each provider package exposes a single ``PLUGIN`` instance of this type.
    The central registry aggregates plugins from all providers into
    ``ALL_PLUGINS`` and derives ``ALL_PARSERS`` from them.

    Attributes:
        name: Provider identifier matching the attribute name on ``Sources``
            (e.g. ``"dkb"``, ``"postbank"``).
        services: All ``ServicePlugin`` instances supported by this provider.
    """

    name: str
    services: tuple[ServicePlugin, ...]

    def all_parsers(self) -> tuple[ParserSpec, ...]:
        """Return a flat tuple of all parser specs across every service."""
        return tuple(spec for svc in self.services for spec in svc.parsers)
