"""GLS provider plugin.

Defines the complete ETL structure for GLS: all services and their parser
specs.  Import ``PLUGIN`` in the central registry to register GLS with the
generic runner.
"""

from fintl.accounts_etl.gls import credit0, giro0
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

PLUGIN = ProviderPlugin(
    name="gls",
    services=(GIRO, CREDIT),
)
