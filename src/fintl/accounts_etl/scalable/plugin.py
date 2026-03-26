"""Scalable provider plugin.

Defines the complete ETL structure for Scalable: all services and their parser
specs.  Source files are HTML/PNG rather than CSV, so each spec carries a
custom ``source_files_getter``.

Import ``PLUGIN`` in the central registry to register Scalable with the
generic runner.
"""

from fintl.accounts_etl.scalable import broker0, broker20231028, broker20260309
from fintl.accounts_etl.scalable.files import (
    get_parser_source_files as scalable_get_source_files,
)
from fintl.accounts_etl.schemas import ParserSpec, ProviderPlugin, ServicePlugin

BROKER = ServicePlugin(
    name="broker",
    parsers=(
        ParserSpec(
            case=broker0.CASE,
            applies=broker0.check_if_parser_applies,
            run=broker0.main,
            precedence=0,
            source_files_getter=scalable_get_source_files,
        ),
        ParserSpec(
            case=broker20231028.CASE,
            applies=broker20231028.check_if_parser_applies,
            run=broker20231028.main,
            precedence=10,
            source_files_getter=scalable_get_source_files,
        ),
        ParserSpec(
            case=broker20260309.CASE,
            applies=broker20260309.check_if_parser_applies,
            run=broker20260309.main,
            precedence=20,
            source_files_getter=scalable_get_source_files,
        ),
    ),
)

PLUGIN = ProviderPlugin(
    name="scalable",
    services=(BROKER,),
)
