import datetime
import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable

import polars as pl
from pydantic import BaseModel

from fintl.common import Case, Config

logger = logging.getLogger(__name__)


class ServiceEnum(str, Enum):
    # entries need to match the attributes of `Provider`
    giro = "giro"
    tagesgeld = "tagesgeld"
    credit = "credit"
    broker = "broker"
    festgeld = "festgeld"


class ProviderEnum(str, Enum):
    # entries need to match the attributes of `Sources`
    dkb = "dkb"
    postbank = "postbank"
    scalable = "scalable"
    gls = "gls"


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
