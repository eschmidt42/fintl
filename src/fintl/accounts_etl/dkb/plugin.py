"""DKB provider plugin.

Defines the complete ETL structure for DKB: all services and their parser
specs.  Import ``PLUGIN`` in the central registry to register DKB with the
generic runner.
"""

from fintl.accounts_etl.dkb import (
    credit0,
    festgeld0,
    giro0,
    giro202307,
    giro202312,
    tagesgeld0,
    tagesgeld202307,
    tagesgeld202312,
)
from fintl.accounts_etl.schemas import ParserSpec, ProviderPlugin, ServicePlugin

GIRO = ServicePlugin(
    name="giro",
    parsers=(
        ParserSpec(
            case=giro0.CASE,
            applies=giro0.check_if_parser_applies,
            run=giro0.main,
            precedence=0,
        ),
        ParserSpec(
            case=giro202307.CASE,
            applies=giro202307.check_if_parser_applies,
            run=giro202307.main,
            precedence=10,
        ),
        ParserSpec(
            case=giro202312.CASE,
            applies=giro202312.check_if_parser_applies,
            run=giro202312.main,
            precedence=20,
        ),
    ),
)

CREDIT = ServicePlugin(
    name="credit",
    parsers=(
        ParserSpec(
            case=credit0.CASE,
            applies=credit0.check_if_parser_applies,
            run=credit0.main,
            precedence=0,
        ),
    ),
)

TAGESGELD = ServicePlugin(
    name="tagesgeld",
    parsers=(
        ParserSpec(
            case=tagesgeld0.CASE,
            applies=tagesgeld0.check_if_parser_applies,
            run=tagesgeld0.main,
            precedence=0,
        ),
        ParserSpec(
            case=tagesgeld202307.CASE,
            applies=tagesgeld202307.check_if_parser_applies,
            run=tagesgeld202307.main,
            precedence=10,
        ),
        ParserSpec(
            case=tagesgeld202312.CASE,
            applies=tagesgeld202312.check_if_parser_applies,
            run=tagesgeld202312.main,
            precedence=20,
        ),
    ),
)

FESTGELD = ServicePlugin(
    name="festgeld",
    parsers=(
        ParserSpec(
            case=festgeld0.CASE,
            applies=festgeld0.check_if_parser_applies,
            run=festgeld0.main,
            precedence=0,
        ),
    ),
)

PLUGIN = ProviderPlugin(
    name="dkb",
    services=(GIRO, CREDIT, TAGESGELD, FESTGELD),
)
