"""Postbank provider plugin.

Defines the complete ETL structure for Postbank: all services and their parser
specs.  Import ``PLUGIN`` in the central registry to register Postbank with
the generic runner.
"""

from fintl.accounts_etl.postbank import giro0, giro202305
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
            case=giro202305.CASE,
            applies=giro202305.check_if_parser_applies,
            run=giro202305.main,
            precedence=10,
        ),
    ),
)

PLUGIN = ProviderPlugin(
    name="postbank",
    services=(GIRO,),
)
