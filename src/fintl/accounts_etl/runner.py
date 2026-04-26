"""ETL runner.

A single table-driven flow that works for any provider and service registered in
``fintl.accounts_etl.registry``.

Execution flow
--------------
1. ``run_enabled_services`` iterates every provider in ``config.sources`` and
   skips those whose value is ``None`` (not configured by the user).
2. For each enabled provider it calls ``run_provider``, which iterates the
   provider's services and skips those whose source path is ``None``.
3. For each enabled service it calls ``run_service``, which:

   a. Looks up all ``ParserSpec`` instances registered for that
      ``(provider, service)`` pair and sorts them by ascending ``precedence``.
   b. Resolves the source files each parser would claim via ``_get_source_files``,
      using the spec's ``source_files_getter`` when provided (e.g. Scalable's
      HTML/PNG discovery) and falling back to the standard CSV helper otherwise.
   c. Calls ``error_if_overlap`` to ensure no source file is claimed by more
      than one parser — a hard error that surfaces misconfigured applicability
      predicates early.
   d. Executes ``spec.run(config)`` for each parser in order.

Overlap detection
-----------------
``check_service_overlap`` performs steps (a)–(c) above without executing the
parsers.  It is kept as a standalone function so existing tests can assert that
a given set of source files does not trigger an overlap error without running
a full ETL pipeline.
"""

import logging
from pathlib import Path

from rich.console import Console
from rich.table import Table

from fintl.accounts_etl.file_helper import (
    get_parser_source_files as csv_get_source_files,
)
from fintl.accounts_etl.files import error_if_overlap
from fintl.accounts_etl.registry import ALL_PARSERS, ALL_PLUGINS
from fintl.accounts_etl.schemas import Case, Config, ParserSpec

logger = logging.getLogger(__name__)


def parsers_for(provider: str, service: str) -> list[ParserSpec]:
    """Return registered specs for a provider/service pair, sorted by precedence.

    Args:
        provider (str): Provider name (e.g. ``"dkb"``).
        service (str): Service name within the provider (e.g. ``"giro"``).

    Returns:
        list[ParserSpec]: All matching specs ordered by ascending precedence.
    """
    return sorted(
        (
            spec
            for spec in ALL_PARSERS
            if spec.case.provider == provider and spec.case.service == service
        ),
        key=lambda spec: spec.precedence,
    )


def print_etl_overview(config: Config, console: Console | None = None) -> None:
    """Print a Rich table summarising the enabled providers, services, and parsers."""
    console = console or Console()
    table = Table(title="ETL Plan", show_header=True, header_style="bold")
    table.add_column("Provider", style="cyan", no_wrap=True)
    table.add_column("Service", style="green", no_wrap=True)
    table.add_column("Parsers (by precedence)")

    for plugin in ALL_PLUGINS:
        provider_sources = getattr(config.sources, plugin.name, None)
        if provider_sources is None:
            continue
        for svc in plugin.services:
            path = getattr(provider_sources, svc.name, None)
            if path is None:
                continue
            specs = parsers_for(plugin.name, svc.name)
            parser_names = (
                ", ".join(s.case.parser for s in specs)
                if specs
                else "(no parsers registered)"
            )
            table.add_row(plugin.name, svc.name, parser_names)

    console.print(table)


def _get_source_files(spec: ParserSpec, config: Config) -> list[Path]:
    """Resolve the source files claimed by *spec* using its preferred getter.

    Falls back to the standard CSV-based helper when ``spec.source_files_getter``
    is ``None``.

    Args:
        spec (ParserSpec): The parser spec whose applicability predicate and
            optional custom getter are used for discovery.
        config (Config): Shared ETL configuration supplying source directories.

    Returns:
        list[Path]: Source file paths that ``spec.applies`` selects.
    """
    getter = (
        spec.source_files_getter
        if spec.source_files_getter is not None
        else csv_get_source_files
    )
    return getter(spec.case, config, spec.applies)


def check_service_overlap(config: Config, provider: str, service: str) -> None:
    """Raise if any source file would be claimed by more than one parser.

    Iterates all registered specs for the given provider/service in precedence
    order and calls ``error_if_overlap`` from ``files.py`` for each one.

    Args:
        config (Config): Shared ETL configuration supplying source directories.
        provider (str): Provider name (e.g. ``"dkb"``).
        service (str): Service name within the provider (e.g. ``"giro"``).

    Raises:
        ValueError: If a source file is claimed by more than one parser.
    """
    known_files: set[Path] = set()
    for spec in parsers_for(provider, service):
        source_files = _get_source_files(spec, config)
        error_if_overlap(spec.case.name, known_files, source_files)
        known_files.update(source_files)


def run_service(config: Config, provider: str, service: str) -> None:
    """Run all parsers for a provider/service pair in precedence order.

    Performs an overlap check before executing each parser so that a file
    claimed by two parsers causes an early error rather than silent double
    processing.

    Args:
        config (Config): Shared ETL configuration supplying source directories
            and the output target directory.
        provider (str): Provider name (e.g. ``"dkb"``).
        service (str): Service name within the provider (e.g. ``"giro"``).

    Raises:
        ValueError: If a source file is claimed by more than one parser.
    """
    logger.info(f"Processing parsers for {provider=} -> {service=}")

    specs = parsers_for(provider, service)
    if not specs:
        # Service is enabled (a path was configured) but no parsers are registered
        # for this (provider, service) pair. This would otherwise result in a
        # silent no-op; log a warning to surface potential misconfiguration.
        logger.warning(
            "No parsers registered for %s -> %s; service is configured but will not be processed.",
            provider,
            service,
        )
        return

    known_files: set[Path] = set()

    for spec in specs:
        source_files = _get_source_files(spec, config)
        error_if_overlap(spec.case.name, known_files, source_files)
        known_files.update(source_files)
        spec.run(config)


def run_provider(config: Config, provider: str, console: Console | None = None) -> None:
    """Run all enabled services for a single provider.

    A service is considered enabled when its source path in ``config`` is not
    ``None``.  Services are run in the order they appear on the ``Provider``
    model.

    Args:
        config (Config): Shared ETL configuration supplying source directories
            and the output target directory.
        provider (str): Provider name (e.g. ``"dkb"``).
        console (Console | None): Rich console for progress output.
    """
    console = console or Console()
    console.rule(f"[bold cyan]{provider.upper()}[/bold cyan]")
    provider_sources = config.get_provider(provider)
    for service_name, path in provider_sources:
        logger.info(f"Selecting service={service_name!r}")
        if path is None:
            logger.info(f"Skipping service={service_name!r} because no path was given.")
            continue
        logger.info(f"Processing {provider=} -> service={service_name!r} @ {path=}")
        run_service(config, provider, service_name)
    logger.info(f"Done processing {provider=}")


def run_enabled_services(config: Config, console: Console | None = None) -> None:
    """Run all providers and services that have a configured source path.

    Iterates every provider in ``config.sources`` and delegates to
    ``run_provider`` for each one that is not ``None``.

    Args:
        config (Config): Shared ETL configuration supplying source directories
            and the output target directory.
        console (Console | None): Rich console for progress output.
    """
    for provider_name, provider_sources in config.sources:
        logger.info(f"Selecting {provider_name=}.")
        if provider_sources is None:
            logger.info(f"Skipping {provider_name=} because no path given.")
            continue
        run_provider(config, provider_name, console)


def all_cases(provider: str | None = None) -> list[Case]:
    """Return all registered Cases, optionally filtered by provider.

    Args:
        provider (str | None): When given, only cases whose
            ``case.provider`` matches this value are returned.
            Defaults to ``None`` (return all cases).

    Returns:
        list[Case]: Registered ``Case`` instances in registry order.
    """
    specs = (
        ALL_PARSERS
        if provider is None
        else [s for s in ALL_PARSERS if s.case.provider == provider]
    )
    return [spec.case for spec in specs]
