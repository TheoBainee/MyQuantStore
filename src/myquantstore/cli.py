"""Interface en ligne de commande (CLI) pour MyQuantStore.
# PYTHON: ARGCOMPLETE_OK

Commandes disponibles :

- ``myquantstore init`` : bootstrap XDG (config + dirs + clé optionnelle).
- ``myquantstore doctor`` : diagnostic install / config / chemins.
- ``myquantstore setup-key`` : clé API Massive dans ``~/.config/myquantstore/.env``.
- ``myquantstore schedule`` : job périodique (systemd user timer et/ou cron).
- ``myquantstore config`` : affiche la config résolue (clé masquée) + chemin du fichier.
- ``myquantstore config add`` : ajoute des tickers à ``config.toml`` (lookup type via cache).
- ``myquantstore status`` : snapshot par instrument (adaptatif au type).
- ``myquantstore fetch`` : historise les OHLCV (cascade auto, multi-type).
- ``myquantstore aggregate`` : régénère le cache agrégé (cascade auto, générique).
- ``myquantstore query <instrument>`` : interroge l'historique (cascade auto).
- ``myquantstore chart [instrument]`` : serveur de visualisation interactive.
- ``myquantstore futures contracts`` : liste/rafraîchit le cache contrats futures.
- ``myquantstore options contracts`` : scaffold (``NotImplementedError``).
- ``myquantstore tickers refresh|types|values`` : cache référentiel ``/v3/reference/tickers``.
- ``myquantstore search`` : recherche locale (+ join types, ``--add`` conf).

**Multi-type** : les instruments sont référencés par symbole nu (ex: ``ES``,
``AAPL``, ``EURUSD``). Le type est résolu depuis la config ; en cas d'ambiguïté
(symbole présent dans plusieurs types), utiliser ``--type``. On peut aussi
passer la clé complète ``type:symbol`` (ex: ``futures:ES``).

Utilise ``argparse`` (stdlib). Autocompletion shell via ``argcomplete`` (optionnel).
"""

from __future__ import annotations

import argparse
import getpass
import sys
from datetime import UTC, datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table

from myquantstore.chains import InstrumentChain
from myquantstore.config import (
    Settings,
    get_repo_config_path,
    get_user_config_path,
    get_user_env_path,
    load_settings,
    resolve_config_path,
)
from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.logging_setup import setup_logging

console = Console()

# Types implémentés (pour le choices de --type)
_INSTRUMENT_TYPE_CHOICES = [t.value for t in InstrumentType]


def _render_df(
    df: object,
    settings: Settings,
    sort_col: str | None = None,
    max_rows: int | None = None,
) -> None:
    """Affiche un DataFrame Polars avec limites + tri décroissant optionnel.

    :param max_rows: Override de ``display_max_rows`` (ex: ``search|query --limit``).
        Polars gère la troncature visuelle avec des ``…`` (pas de pré-coupe head).
    """
    import polars as pl

    if df is None or not isinstance(df, pl.DataFrame) or df.is_empty():
        console.print("[yellow]Aucune donnée[/yellow]")
        return

    rendered = df
    if sort_col and sort_col in rendered.columns:
        rendered = rendered.sort(sort_col, descending=True)

    total_rows = rendered.height
    total_cols = rendered.width
    rows_cap = max_rows if max_rows is not None else settings.display_max_rows
    cols_cap = settings.display_max_columns

    if rendered.width > cols_cap:
        rendered = rendered[:, :cols_cap]

    # Ne pas head() : laisser Polars afficher des … via set_tbl_rows
    with pl.Config(
        set_tbl_rows=rows_cap,
        set_tbl_cols=cols_cap,
    ):
        console.print(rendered)

    if total_rows > rows_cap:
        console.print(
            f"[dim]… affichage limité à {rows_cap} / {total_rows} lignes "
            f"(display_max_rows / --limit)[/dim]"
        )
    if total_cols > cols_cap:
        console.print(
            f"[dim]… affichage limité à {cols_cap} / {total_cols} colonnes[/dim]"
        )


def _resolve_instrument_arg(
    settings: Settings, arg: str | None, type_override: str | None
) -> Instrument:
    """Résout un argument instrument (symbole nu ou clé ``type:symbol``).

    :raises ValueError: Si non trouvé ou ambigu.
    """
    if arg is None:
        raise ValueError("Instrument requis.")
    # Format clé complète "type:symbol"
    if ":" in arg:
        type_str, symbol = arg.split(":", 1)
        t = InstrumentType(type_str)
        return Instrument(type=t, symbol=symbol)
    if type_override:
        return settings.resolve_instrument(arg, InstrumentType(type_override))
    return settings.resolve_instrument(arg)


def _resolve_instruments(
    settings: Settings, arg: str | None, type_override: str | None
) -> list[Instrument]:
    """Résout un argument instrument optionnel en liste.

    - Pas d'arg + pas de ``--type`` → tous les instruments configurés.
    - Pas d'arg + ``--type`` → uniquement les instruments de ce type.
    - Arg (+ ``--type`` optionnel si ambigu) → un seul instrument.
    """
    if arg is None:
        if type_override:
            return settings.instruments_of_type(InstrumentType(type_override))
        return settings.all_instruments()
    return [_resolve_instrument_arg(settings, arg, type_override)]


def main(argv: list[str] | None = None) -> int:
    """Point d'entrée principal du CLI."""
    parser = _build_parser()

    try:
        import argcomplete

        argcomplete.autocomplete(parser)
    except ImportError:
        pass

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    # Commandes sans config.toml obligatoire
    if args.command == "setup-key":
        return _cmd_setup_key(args)
    if args.command == "init":
        return _cmd_init(args)
    if args.command == "doctor":
        return _cmd_doctor(args)
    if args.command == "schedule" and getattr(args, "schedule_command", None) != "run":
        # install/show/status/uninstall n'exigent pas la config métier
        return _cmd_schedule(None, args)

    try:
        settings = load_settings()
    except FileNotFoundError as e:
        console.print(f"[red]Erreur:[/red] {e}")
        console.print("[dim]Lancez `myquantstore init` pour créer la configuration.[/dim]")
        return 1
    except Exception as e:
        console.print(f"[red]Erreur de configuration:[/red] {e}")
        return 1

    setup_logging(level=settings.log_level, log_dir=settings.log_dir)

    if args.command == "config":
        if getattr(args, "config_command", None) == "add":
            return _cmd_config_add(settings, args)
        return _cmd_config(settings, args)
    elif args.command == "fetch":
        return _cmd_fetch(settings, args)
    elif args.command == "aggregate":
        return _cmd_aggregate(settings, args)
    elif args.command == "query":
        return _cmd_query(settings, args)
    elif args.command == "chart":
        return _cmd_chart(settings, args)
    elif args.command == "portfolio":
        return _cmd_portfolio(settings, args)
    elif args.command == "status":
        return _cmd_status(settings, args)
    elif args.command == "schedule":
        return _cmd_schedule(settings, args)
    elif args.command == "futures":
        if getattr(args, "futures_command", None) == "contracts":
            return _cmd_futures_contracts(settings, args)
        parser.print_help()
        return 0
    elif args.command == "options":
        if getattr(args, "options_command", None) == "contracts":
            return _cmd_options_contracts(settings, args)
        parser.print_help()
        return 0
    elif args.command == "tickers":
        if getattr(args, "tickers_status", False):
            _print_tickers_cache_status(settings)
            return 0
        if getattr(args, "tickers_command", None) == "refresh":
            return _cmd_tickers_refresh(settings, args)
        if getattr(args, "tickers_command", None) == "types":
            return _cmd_tickers_types(settings, args)
        if getattr(args, "tickers_command", None) == "values":
            return _cmd_tickers_values(settings, args)
        parser.print_help()
        return 0
    elif args.command == "search":
        return _cmd_search(settings, args)
    else:
        parser.print_help()
        return 0


_HELP_FMT = argparse.RawDescriptionHelpFormatter

_INSTRUMENT_HELP = (
    "Symbole nu (ES, AAPL, EURUSD) ou clé type:symbol (stocks:AAPL). "
    "Défaut: tous les instruments configurés (ou le type si --type)."
)
_TYPE_HELP = (
    "Filtre par type d'instrument (futures|stocks|forex|indices|options). "
    "Sans -i/--instrument: tous les symboles de ce type. "
    "Avec -i: lève l'ambiguïté si le symbole existe dans plusieurs types."
)
_TIMEFRAME_FETCH_HELP = (
    "Résolution(s) de stockage à historiser: "
    "1min (Massive intraday), 1day (Yahoo daily multi-type), "
    "all (défaut = 1min + 1day)."
)
_TIMEFRAME_AGG_HELP = (
    "Résolution(s) à ré-agréger depuis les dumps: 1min | 1day | all (défaut)."
)
_NO_CASCADE_HELP = (
    "Désactive la cascade auto (pas de refresh caches listing / fetch préalable "
    "si dumps absents). Erreur claire si un prérequis manque."
)
_FORCE_FETCH_HELP = (
    "Ignore le skip « déjà fait aujourd'hui » et relance le fetch "
    "(utile si agrégé STALE alors qu'un dump du jour existe)."
)


def _add_instrument_filter(
    parser: argparse.ArgumentParser,
    *,
    instrument_help: str = _INSTRUMENT_HELP,
    type_help: str = _TYPE_HELP,
) -> None:
    """Ajoute ``-i/--instrument`` et ``--type`` (filtre multi-instrument)."""
    parser.add_argument(
        "-i",
        "--instrument",
        default=None,
        metavar="SYMBOL",
        help=instrument_help,
    )
    parser.add_argument(
        "--type",
        default=None,
        choices=_INSTRUMENT_TYPE_CHOICES,
        help=type_help,
    )


def _build_parser() -> argparse.ArgumentParser:
    """Construit le parseur d'arguments CLI."""
    parser = argparse.ArgumentParser(
        prog="myquantstore",
        description=(
            "Historisation périodique OHLCV multi-instruments (futures, stocks, forex,\n"
            "indices ; options = scaffold) vers des fichiers Parquet locaux.\n"
            "\n"
            "Deux sources indépendantes (ne pas croiser pour reconstruire un agrégat) :\n"
            "  • 1min  — Massive.com REST (intraday) ; resample à la query (2m, 5m, 1h…)\n"
            "  • 1day  — Yahoo Finance chart (extraday multi-type) ; resample 2d, 1w…\n"
            "Futures : 1min = contrats Massive + rollover maison ; 1day = continu Yahoo (=F).\n"
            "\n"
            "Config XDG : ~/.config/myquantstore/{config.toml,.env}\n"
            "Données    : ~/.local/share/myquantstore/{data,cache,logs}\n"
            "\n"
            "Flux typique : init → doctor → fetch → status → query|chart\n"
            "Automatisation : schedule install (samedi 01h00 ; fetch→aggregate→status --check)"
        ),
        epilog=(
            "Aide détaillée d'une commande :\n"
            "  myquantstore <commande> -h\n"
            "\n"
            "Premiers pas :\n"
            "  myquantstore init && myquantstore doctor\n"
            "  myquantstore fetch --dry-run && myquantstore fetch\n"
            "  myquantstore schedule install\n"
            "\n"
            "Exemples courants :\n"
            "  myquantstore fetch -i AAPL --timeframe 1min --force\n"
            "  myquantstore status -i AAPL --check          # exit 1 si STALE\n"
            "  myquantstore query ES --timescale-unit min --timescale-nb 5\n"
            "  myquantstore query AAPL --no-split           # prix bruts stocks\n"
            "  myquantstore chart                          # dashboard navigateur\n"
            "  myquantstore config add NVDA --type stocks\n"
            "  myquantstore search apple --markets stocks\n"
            "\n"
            "Docs : README.md · docs/TECHNICAL_DESIGN.md · docs/MULTI_TYPE.md · docs/PORTFOLIO.md"
        ),
        formatter_class=_HELP_FMT,
    )
    subparsers = parser.add_subparsers(
        dest="command",
        title="commandes",
        metavar="COMMAND",
        help="Détail : myquantstore COMMAND -h",
    )

    def _sub(name: str, *, help: str, description: str, epilog: str | None = None):
        return subparsers.add_parser(
            name,
            help=help,
            description=description,
            epilog=epilog,
            formatter_class=_HELP_FMT,
        )

    # --- init ---
    p_init = _sub(
        "init",
        help="Crée config XDG, dirs data/cache/logs et clé API optionnelle",
        description=(
            "Crée ~/.config/myquantstore et ~/.local/share/myquantstore/{data,cache,logs},\n"
            "copie une config (minimale par défaut, ou --full), optionnellement la clé API."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore init\n"
            "  myquantstore init --full\n"
            "  myquantstore init --api-key YOUR_KEY\n"
            "  myquantstore init --force --no-key"
        ),
    )
    p_init_profile = p_init.add_mutually_exclusive_group()
    p_init_profile.add_argument(
        "--minimal",
        action="store_true",
        default=True,
        help="Config minimale (défaut: stocks=[AAPL] seulement)",
    )
    p_init_profile.add_argument(
        "--full",
        action="store_true",
        help="Config exemple multi-type (backfill plus lourd)",
    )
    p_init.add_argument(
        "--force",
        action="store_true",
        help="Écrase config.toml / .env existants",
    )
    p_init.add_argument(
        "--api-key",
        "-k",
        default=None,
        metavar="KEY",
        help="Écrit la clé API Massive (sinon prompt TTY sauf --no-key)",
    )
    p_init.add_argument(
        "--no-key",
        action="store_true",
        help="Ne configure pas la clé API",
    )
    p_init.add_argument(
        "--base-url",
        default=None,
        help="URL de base API (défaut: https://api.massive.com)",
    )

    # --- doctor ---
    p_doctor = _sub(
        "doctor",
        help="Vérifie install, config, chemins et clé (exit 1 si bloquant)",
        description=(
            "Vérifie Python, config.toml, chemins data/cache/logs, clé API,\n"
            "binaire PATH et schedule éventuel. Exit 1 si problème bloquant."
        ),
        epilog="Exemple: myquantstore doctor [--ping]",
    )
    p_doctor.add_argument(
        "--ping",
        action="store_true",
        help="Tente un ping HTTP léger vers l'API Massive si clé présente",
    )

    # --- setup-key ---
    p_setup = _sub(
        "setup-key",
        help="Écrit MASSIVE_API_KEY dans ~/.config/myquantstore/.env",
        description=(
            "Écrit la clé API Massive dans ~/.config/myquantstore/.env\n"
            "(jamais commité). Interactif par défaut ; --api-key pour scripts."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore setup-key\n"
            "  myquantstore setup-key --api-key YOUR_KEY --yes"
        ),
    )
    p_setup.add_argument(
        "--base-url",
        default=None,
        help="URL de base de l'API (défaut: https://api.massive.com)",
    )
    p_setup.add_argument(
        "--api-key",
        "-k",
        default=None,
        metavar="KEY",
        help="Clé API (sinon prompt masqué)",
    )
    p_setup.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Écrase une clé existante sans confirmation",
    )

    # --- schedule ---
    p_sched = _sub(
        "schedule",
        help="Timer OS : fetch → aggregate → status (systemd/cron, sam. 01h)",
        description=(
            "Installe un job OS qui exécute fetch → aggregate → status --check.\n"
            "Backends: systemd user timer (recommandé) ou crontab utilisateur.\n"
            "Défaut: samedi 01:00 heure locale."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore schedule install\n"
            "  myquantstore schedule install --backend cron --when '0 1 * * 6'\n"
            "  myquantstore schedule install --when 'Sat *-*-* 01:00:00'\n"
            "  myquantstore schedule run\n"
            "  myquantstore schedule status\n"
            "  myquantstore schedule uninstall"
        ),
    )
    sched_sub = p_sched.add_subparsers(dest="schedule_command", help="Sous-commande schedule")
    p_sched_install = sched_sub.add_parser(
        "install",
        help="Installe le timer/cron",
        formatter_class=_HELP_FMT,
    )
    p_sched_install.add_argument(
        "--backend",
        choices=["auto", "systemd", "cron"],
        default="auto",
        help="Backend (défaut: auto = systemd user si dispo, sinon cron)",
    )
    p_sched_install.add_argument(
        "--when",
        default=None,
        metavar="SPEC",
        help=(
            "Horaires: OnCalendar systemd (ex: 'Sat *-*-* 01:00:00') "
            "ou expression cron (ex: '0 1 * * 6'). Défaut selon backend."
        ),
    )
    p_sched_install.add_argument(
        "--fetch-args",
        default="",
        metavar="ARGS",
        help='Args passés à fetch via schedule run (ex: "--no-cascade")',
    )
    p_sched_install.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche units/crontab sans installer",
    )
    p_sched_run = sched_sub.add_parser(
        "run",
        help="Exécute le job (fetch + aggregate + status --check)",
        formatter_class=_HELP_FMT,
    )
    p_sched_run.add_argument(
        "--fetch-args",
        default="",
        metavar="ARGS",
        help='Args additionnels pour fetch (ex: "--type stocks")',
    )
    p_sched_run.add_argument(
        "--skip-aggregate",
        action="store_true",
        help="Ne pas reconstruire l'agrégat après fetch",
    )
    p_sched_run.add_argument(
        "--skip-status",
        action="store_true",
        help="Ne pas exécuter status --check en fin de job",
    )
    sched_sub.add_parser("status", help="État du schedule installé", formatter_class=_HELP_FMT)
    p_sched_show = sched_sub.add_parser(
        "show",
        help="Affiche units/ligne cron sans installer",
        formatter_class=_HELP_FMT,
    )
    p_sched_show.add_argument(
        "--backend",
        choices=["auto", "systemd", "cron"],
        default="auto",
    )
    p_sched_show.add_argument("--when", default=None, metavar="SPEC")
    p_sched_show.add_argument("--fetch-args", default="", metavar="ARGS")
    p_sched_un = sched_sub.add_parser(
        "uninstall",
        help="Retire timer systemd et/ou bloc cron",
        formatter_class=_HELP_FMT,
    )
    p_sched_un.add_argument(
        "--backend",
        choices=["auto", "systemd", "cron", "all"],
        default="all",
        help="Quoi retirer (défaut: all)",
    )

    # --- config ---
    p_config = _sub(
        "config",
        help="Résumé config / --paths ; config add pour ajouter des tickers",
        description=(
            "Sans sous-commande: affiche un résumé de la config chargée\n"
            "(instruments, fetch, storage, chemins résolus).\n"
            "Sous-commande: config add — ajoute des tickers à config.toml."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore config\n"
            "  myquantstore config --paths\n"
            "  myquantstore config add AAPL MSFT --type stocks\n"
            "  myquantstore config add C:EURUSD I:NDX"
        ),
    )
    p_config.add_argument(
        "--paths",
        action="store_true",
        help="Liste tous les chemins résolus (.env, config.toml, data, cache, logs)",
    )
    config_sub = p_config.add_subparsers(dest="config_command", help="Sous-commande config")
    p_config_add = config_sub.add_parser(
        "add",
        help="Ajoute des tickers à config.toml (lookup type via cache tickers)",
        description=(
            "Ajoute un ou plusieurs symboles dans [instruments] du config.toml.\n"
            "Le type est déduit du cache tickers (ou imposé via --type).\n"
            "Préfixes acceptés: C: (forex), I: (indices), O: (options)."
        ),
        formatter_class=_HELP_FMT,
        epilog="Exemple: myquantstore config add AAPL TSLA --type stocks",
    )
    p_config_add.add_argument(
        "tickers",
        nargs="+",
        help="Symboles nus ou préfixés (AAPL, C:EURUSD, I:NDX)",
    )
    p_config_add.add_argument(
        "--type",
        default=None,
        choices=_INSTRUMENT_TYPE_CHOICES,
        help="Type imposé (sinon lookup via cache tickers)",
    )
    p_config_add.add_argument(
        "--no-cascade",
        action="store_true",
        help="N'auto-refresh pas le cache tickers si absent/périmé",
    )

    # --- fetch ---
    p_fetch = _sub(
        "fetch",
        help="Télécharge OHLCV (1min Massive + 1day Yahoo) → dumps/agrégat",
        description=(
            "Récupère les OHLCV et met à jour dumps pseudo-bruts + agrégat.\n"
            "\n"
            "Dual-source:\n"
            "  1min  → Massive.com (intraday, tous types implémentés)\n"
            "  1day  → Yahoo Finance chart (stocks/forex/indices/futures continu)\n"
            "\n"
            "Premier run: history_months (Massive) ou period=max (Yahoo).\n"
            "Runs suivants: depuis latest − overlap_buffer.\n"
            "Skip si un dump du jour existe déjà (sauf --force).\n"
            "Le résumé affiche latest= / lag= / STALE si données périmées."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore fetch                         # tous instruments, 1min+1day\n"
            "  myquantstore fetch -i SKHYV --timeframe 1min\n"
            "  myquantstore fetch -i ES --type futures --force\n"
            "  myquantstore fetch --type stocks --timeframe 1day --dry-run\n"
            "  myquantstore fetch -i AAPL --force         # re-fetch malgré dump du jour"
        ),
    )
    _add_instrument_filter(p_fetch)
    p_fetch.add_argument(
        "--timeframe",
        default="all",
        metavar="TF",
        help=_TIMEFRAME_FETCH_HELP,
    )
    p_fetch.add_argument("--force", action="store_true", help=_FORCE_FETCH_HELP)
    p_fetch.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche le plan de fetch (plages / segments) sans appeler l'API",
    )
    p_fetch.add_argument("--no-cascade", action="store_true", help=_NO_CASCADE_HELP)

    # --- aggregate ---
    p_agg = _sub(
        "aggregate",
        help="Reconstruit aggregate/*.parquet uniquement depuis les dumps",
        description=(
            "Reconstruit data/aggregate/{type}/{symbol}/{resolution}.parquet\n"
            "en concaténant tous les dumps de la résolution, dédup\n"
            "(window_start, ticker) keep=last.\n"
            "N'appelle pas l'API OHLCV (cascade fetch seulement si dumps absents)."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore aggregate -i AAPL --timeframe 1min\n"
            "  myquantstore aggregate --type futures\n"
            "  myquantstore aggregate --timeframe all"
        ),
    )
    _add_instrument_filter(p_agg)
    p_agg.add_argument(
        "--timeframe",
        default="all",
        metavar="TF",
        help=_TIMEFRAME_AGG_HELP,
    )
    p_agg.add_argument("--no-cascade", action="store_true", help=_NO_CASCADE_HELP)

    # --- query ---
    p_query = _sub(
        "query",
        help="Lit l'agrégé : resample, splits/div, export Parquet",
        description=(
            "Lit l'agrégé, applique resample + ajustements optionnels, affiche\n"
            "ou écrit un Parquet.\n"
            "\n"
            "Tracks (selon --timescale-unit):\n"
            "  min / hour  → agrégé 1min (Massive)\n"
            "  day / week  → agrégé 1day (Yahoo)\n"
            "\n"
            "Stocks: split-adjust ON par défaut (--no-split pour bruts);\n"
            "  --adjust ajoute l'ajustement dividendes.\n"
            "Futures: --adjust = back-adjust rollover; --normalize-tick-size.\n"
            "  Dédup timestamps ON par défaut (--no-dedup-timestamps pour\n"
            "  garder les deux contrats au même window_start le jour de roll)."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore query ES --start 2025-01-01 --timescale-unit min --timescale-nb 5\n"
            "  myquantstore query AAPL --timescale-unit day --adjust\n"
            "  myquantstore query AAPL --no-split --output /tmp/aapl.parquet\n"
            "  myquantstore query EURUSD --type forex --intraday-begin 08:00 --intraday-end 17:00"
        ),
    )
    p_query.add_argument(
        "instrument",
        help="Symbole (ES, AAPL) ou clé type:symbol (stocks:AAPL) — obligatoire",
    )
    p_query.add_argument(
        "--type",
        default=None,
        choices=_INSTRUMENT_TYPE_CHOICES,
        help="Type imposé si le symbole est ambigu",
    )
    p_query.add_argument(
        "--start",
        default=None,
        metavar="DATE",
        help="Date de début inclusive (YYYY-MM-DD). Défaut: début de l'agrégé",
    )
    p_query.add_argument(
        "--end",
        default=None,
        metavar="DATE",
        help="Date de fin inclusive (YYYY-MM-DD). Défaut: fin de l'agrégé",
    )
    p_query.add_argument(
        "--timescale-unit",
        choices=["min", "hour", "day", "week"],
        default="min",
        help="Unité de l'unité de temps (min/hour=1min Massive; day/week=1day Yahoo)",
    )
    p_query.add_argument(
        "--timescale-nb",
        type=int,
        default=1,
        metavar="N",
        help="Multiplicateur d'unité (ex: 5 + min → barres 5 minutes). Défaut: 1",
    )
    p_query.add_argument(
        "--intraday-begin",
        default=None,
        metavar="HH:MM",
        help="Filtre session: heure de début (wrap-around OK, ex: 22:00→06:00)",
    )
    p_query.add_argument(
        "--intraday-end",
        default=None,
        metavar="HH:MM",
        help="Filtre session: heure de fin (doit différer de --intraday-begin)",
    )
    p_query.add_argument(
        "--adjust",
        action="store_true",
        help="Futures: back-adjust rollover. Stocks: ajuste aussi les dividendes",
    )
    p_query.add_argument(
        "--no-split",
        action="store_true",
        help="Stocks: garde les prix bruts (désactive l'ajustement split, ON par défaut)",
    )
    p_query.add_argument(
        "--no-dedup-timestamps",
        action="store_true",
        help=(
            "Futures 1min: conserve deux barres au même window_start au roll "
            "(désactive la dédup ON par défaut ; le contrat le plus récent gagne)"
        ),
    )
    p_query.add_argument(
        "--normalize-tick-size",
        action="store_true",
        help="Futures: convertit les prix en multiples de tick (Int32)",
    )
    p_query.add_argument(
        "--check-ticksize-accuracy",
        action="store_true",
        help="Futures: rapport de conformité des prix au tick size (qualité données)",
    )
    p_query.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help="Écrit le résultat en Parquet (sinon tableau stdout tronqué)",
    )
    p_query.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Max lignes affichées stdout (override display_max_rows; n'altère pas --output)",
    )
    p_query.add_argument("--no-cascade", action="store_true", help=_NO_CASCADE_HELP)

    # --- chart ---
    p_chart = _sub(
        "chart",
        help="Dashboard + charts candlestick (FastAPI / navigateur)",
        description=(
            "Démarre un serveur HTTP local (FastAPI) et ouvre un graphique\n"
            "candlestick interactif. Les données viennent des agrégés locaux\n"
            "(même logique dual-track que query)."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore chart\n"
            "  myquantstore chart AAPL --timescale-unit day\n"
            "  myquantstore chart ES --port 8050 --adjust\n"
            "  myquantstore chart --host 0.0.0.0 --mdns"
        ),
    )
    p_chart.add_argument(
        "instrument",
        nargs="?",
        default=None,
        help="Instrument affiché au démarrage (ES, AAPL…). Défaut: 1er de la config",
    )
    p_chart.add_argument(
        "--type",
        default=None,
        choices=_INSTRUMENT_TYPE_CHOICES,
        help="Type imposé si le symbole est ambigu",
    )
    p_chart.add_argument(
        "--port",
        type=int,
        default=None,
        metavar="N",
        help="Port HTTP (défaut: config [chart].port, souvent 8050)",
    )
    p_chart.add_argument(
        "--host",
        default=None,
        help="Adresse de bind (défaut: config [chart].host, souvent 127.0.0.1)",
    )
    p_chart.add_argument(
        "--mdns",
        action="store_true",
        default=None,
        help="Annonce mDNS pour découverte sur le réseau local",
    )
    p_chart.add_argument("--no-cascade", action="store_true", help=_NO_CASCADE_HELP)
    p_chart.add_argument(
        "--timescale-unit",
        choices=["min", "hour", "day", "week"],
        default=None,
        help="Unité UT initiale (min/hour=intraday; day/week=extraday)",
    )
    p_chart.add_argument(
        "--timescale-nb",
        type=int,
        default=None,
        metavar="N",
        help="Multiplicateur UT initial (ex: 5 → 5min)",
    )
    p_chart.add_argument(
        "--nb-candle",
        type=int,
        default=None,
        metavar="N",
        help="Nombre de chandeliers visibles au chargement",
    )
    p_chart.add_argument(
        "--intraday-begin",
        default=None,
        metavar="HH:MM",
        help="Filtre session début (intraday)",
    )
    p_chart.add_argument(
        "--intraday-end",
        default=None,
        metavar="HH:MM",
        help="Filtre session fin (intraday)",
    )
    p_chart.add_argument(
        "--normalize-tick-size",
        action="store_true",
        help="Futures: prix en multiples de tick (Int32)",
    )
    p_chart.add_argument(
        "--adjust",
        action="store_true",
        help="Futures: back-adjust rollover. Stocks: + dividendes",
    )
    p_chart.add_argument(
        "--no-split",
        action="store_true",
        help="Stocks: prix bruts (désactive split-adjust par défaut)",
    )

    # --- status ---
    p_status = _sub(
        "status",
        help="Couverture OHLCV, lag, STALE ; --check pour monitoring",
        description=(
            "Snapshot de santé:\n"
            "  • cache tickers global (shards market×active, TTL)\n"
            "  • par instrument: caches listing (contrats/splits),\n"
            "    dumps, agrégés avec plage + lag calendaire\n"
            "  • STALE si lag > [health].stale_lag_days_1min|1day\n"
            "  • warn si |lag_1min − lag_1day| trop grand (dual-source)\n"
            "\n"
            "--check: exit code 1 si STALE ou écart multi-résolution (cron)."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore status\n"
            "  myquantstore status -i SKHYV\n"
            "  myquantstore status --type stocks --check\n"
            "  myquantstore status --tickers"
        ),
    )
    _add_instrument_filter(p_status)
    p_status.add_argument(
        "--tickers",
        action="store_true",
        help="N'affiche que le cache référentiel tickers (markets / shards / types)",
    )
    p_status.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 si au moins un agrégé STALE ou un écart 1min/1day (idéal cron)",
    )

    # --- futures (groupe) ---
    p_futures = _sub(
        "futures",
        help="Contrats futures / rollover (cache Massive)",
        description="Sous-commandes réservées aux produits futures (contrats CME, etc.).",
        epilog="Exemple: myquantstore futures contracts --symbol ES --refresh",
    )
    futures_sub = p_futures.add_subparsers(dest="futures_command", help="Sous-commande futures")
    p_fc = futures_sub.add_parser(
        "contracts",
        help="Liste / rafraîchit le cache contrats futures",
        description=(
            "Affiche le cache /futures/v1/contracts (Parquet local).\n"
            "--refresh force un re-fetch API (ignore le TTL instrument_cache)."
        ),
        formatter_class=_HELP_FMT,
        epilog="Exemple: myquantstore futures contracts -i ES --active-only",
    )
    p_fc.add_argument(
        "-i",
        "--symbol",
        "--instrument",
        dest="symbol",
        default=None,
        metavar="SYMBOL",
        help="Code produit futures (ES, NQ…). Défaut: tous les futures configurés",
    )
    p_fc.add_argument(
        "--refresh",
        action="store_true",
        help="Force le re-fetch API du cache contrats (ignore TTL)",
    )
    p_fc.add_argument(
        "--active-only",
        action="store_true",
        help="N'affiche que les contrats encore tradables",
    )

    # --- options (groupe — scaffold) ---
    p_options = _sub(
        "options",
        help="Options (scaffold — NotImplemented)",
        description="Scaffold réservé aux options. Non implémenté (NotImplementedError).",
    )
    options_sub = p_options.add_subparsers(dest="options_command", help="Sous-commande options")
    options_sub.add_parser(
        "contracts",
        help="Liste des contrats options (non implémenté)",
        description="Placeholder — lèvera NotImplementedError.",
        formatter_class=_HELP_FMT,
    )

    # --- tickers (référentiel /v3/reference/tickers) ---
    p_tickers = _sub(
        "tickers",
        help="Référentiel Massive tickers (refresh / types / values)",
        description=(
            "Gère le cache local du référentiel tickers Massive\n"
            "(shards market × active/inactive + types).\n"
            "Utilisé par search et config add pour résoudre le type d'un symbole."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore tickers --status\n"
            "  myquantstore tickers refresh --markets stocks fx --active all\n"
            "  myquantstore tickers types\n"
            "  myquantstore tickers values --column type market"
        ),
    )
    p_tickers.add_argument(
        "--status",
        dest="tickers_status",
        action="store_true",
        help="Affiche l'état du cache tickers (alias de 'status --tickers')",
    )
    tickers_sub = p_tickers.add_subparsers(dest="tickers_command", help="Sous-commande tickers")
    p_tr = tickers_sub.add_parser(
        "refresh",
        help="Fetch/cache tickers par shards market×active (+ types)",
        description=(
            "Télécharge et met en cache les tickers Massive par shard\n"
            "(market × active|inactive). Met aussi à jour le cache des types."
        ),
        formatter_class=_HELP_FMT,
        epilog="Exemple: myquantstore tickers refresh --markets all --active all --force",
    )
    p_tr.add_argument(
        "--markets",
        nargs="+",
        default=None,
        metavar="MKT",
        help="Markets: stocks fx indices otc crypto, ou all. CSV accepté. Défaut: stocks",
    )
    p_tr.add_argument(
        "--active",
        choices=["true", "false", "all"],
        default="true",
        help="Shard active: true|false|all (défaut: true → active.parquet seulement)",
    )
    p_tr.add_argument(
        "--force",
        action="store_true",
        help="Ignore le TTL et re-fetch tous les shards demandés",
    )
    p_tt = tickers_sub.add_parser(
        "types",
        help="Liste / rafraîchit le cache des ticker types",
        description="Cache des codes type Massive (CS, ETF, ADX, …) avec libellés.",
        formatter_class=_HELP_FMT,
    )
    p_tt.add_argument("--force", action="store_true", help="Ignore le TTL et re-fetch")
    p_tv = tickers_sub.add_parser(
        "values",
        help="Valeurs distinctes (market, type, exchange, currency)",
        description=(
            "Liste les valeurs uniques présentes dans le cache tickers local\n"
            "pour faciliter les filtres de search."
        ),
        formatter_class=_HELP_FMT,
        epilog="Exemple: myquantstore tickers values --markets stocks --column type",
    )
    p_tv.add_argument(
        "--markets",
        nargs="+",
        default=None,
        metavar="MKT",
        help="Filtre market(s) des shards lus. Défaut: tous shards présents sur disque",
    )
    p_tv.add_argument(
        "--column",
        nargs="+",
        default=None,
        choices=["market", "type", "primary_exchange", "currency_name"],
        metavar="COL",
        help="Colonnes à lister (défaut: les 4)",
    )
    p_tv.add_argument("--active", action="store_true", help="Uniquement tickers actifs")
    p_tv.add_argument("--inactive", action="store_true", help="Uniquement tickers inactifs")
    p_tv.add_argument(
        "--no-cascade",
        action="store_true",
        help="N'auto-refresh pas le cache tickers si absent/périmé",
    )

    # --- portfolio (MPT) ---
    p_port = _sub(
        "portfolio",
        help="MPT stocks 1day : stats, corr, optim, allocate, frontier",
        description=(
            "Modern Portfolio Theory sur l'univers stocks (track 1day Yahoo).\n"
            "Returns total-return (split + dividend adjust). Optim long-only\n"
            "numpy (equal | min-vol | max-sharpe) + frontière approximée."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore portfolio stats\n"
            "  myquantstore portfolio corr --from 2020-01-01 --export /tmp/corr.parquet\n"
            "  myquantstore portfolio optimize --objective min-vol\n"
            "  myquantstore portfolio optimize --objective max-sharpe -i AAPL -i NVDA\n"
            "  myquantstore portfolio allocate --objective min-vol --value 20000\n"
            "  myquantstore portfolio frontier --timescale week"
        ),
    )
    port_sub = p_port.add_subparsers(dest="portfolio_command", help="Sous-commande portfolio")

    def _add_portfolio_common(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "-i",
            "--instrument",
            action="append",
            default=None,
            metavar="SYMBOL",
            help="Titre à inclure (répétable). Défaut: tous les stocks configurés",
        )
        p.add_argument(
            "--from",
            dest="date_from",
            default=None,
            metavar="DATE",
            help="Début fenêtre (YYYY-MM-DD). Défaut: lookback_years config",
        )
        p.add_argument(
            "--to",
            dest="date_to",
            default=None,
            metavar="DATE",
            help="Fin fenêtre (YYYY-MM-DD). Défaut: aujourd'hui",
        )
        p.add_argument(
            "--timescale",
            choices=["day", "week"],
            default="day",
            help="Fréquence returns (défaut: day)",
        )
        p.add_argument(
            "--rf",
            type=float,
            default=None,
            metavar="RATE",
            help=("Taux sans risque annualisé (fraction, ex. 0.04). "
                "Override CLI ; sinon rf_source=yahoo (^IRX) ou risk_free_rate static"),
        )
        p.add_argument(
            "--log-returns",
            action="store_true",
            help="Returns logarithmiques au lieu de simple",
        )
        p.add_argument(
            "--no-div",
            action="store_true",
            help="Price return only (pas d'ajustement dividendes)",
        )
        p.add_argument(
            "--export",
            default=None,
            metavar="PATH",
            help="Export résultat (.parquet ou .csv)",
        )

    for name, help_txt in (
        ("stats", "μ/σ/Sharpe annualisés par titre"),
        ("corr", "Matrice de corrélation des returns"),
        ("cov", "Matrice de covariance annualisée"),
        ("optimize", "Optimisation long-only (equal|min-vol|max-sharpe)"),
        ("allocate", "Lots entiers à partir des poids + capital"),
        ("frontier", "Frontière efficiente (QP target-return grid)"),
    ):
        pp = port_sub.add_parser(
            name,
            help=help_txt,
            formatter_class=_HELP_FMT,
        )
        _add_portfolio_common(pp)
        if name in ("optimize", "allocate"):
            pp.add_argument(
                "--objective",
                choices=["equal", "min-vol", "max-sharpe"],
                default="max-sharpe",
                help="Fonction objectif (défaut: max-sharpe)",
            )
        if name == "allocate":
            pp.add_argument(
                "--value",
                type=float,
                default=None,
                metavar="V",
                help="Capital à allouer (défaut: config portfolio.default_value)",
            )
        if name == "frontier":
            pp.add_argument(
                "--points",
                type=int,
                default=40,
                metavar="N",
                help="Nombre de targets return sur la grille QP (défaut: 40)",
            )
            pp.add_argument(
                "--method",
                choices=["qp", "sample"],
                default="qp",
                help="qp = SLSQP target-return (défaut) ; sample = Dirichlet legacy",
            )

    # --- search ---
    p_search = _sub(
        "search",
        help="Cherche dans le cache tickers local (--add → config)",
        description=(
            "Filtre le cache tickers local (pas d'appel API si cache frais).\n"
            "Utile pour trouver un symbole avant config add / fetch."
        ),
        epilog=(
            "Exemples:\n"
            "  myquantstore search apple --markets stocks --limit 20\n"
            "  myquantstore search --ticker AAPL\n"
            "  myquantstore search --type ETF --exchange XNAS --add --yes\n"
            "  myquantstore search EUR --markets fx --output /tmp/fx.parquet"
        ),
    )
    p_search.add_argument(
        "query",
        nargs="?",
        default=None,
        help="Sous-chaîne insensible à la casse sur ticker ou name",
    )
    p_search.add_argument(
        "--ticker",
        default=None,
        metavar="T",
        help="Égalité exacte sur le ticker (ex: AAPL)",
    )
    p_search.add_argument(
        "--markets",
        nargs="+",
        default=None,
        metavar="MKT",
        help="Filtre market(s): stocks, fx, indices, otc, crypto. CSV accepté",
    )
    p_search.add_argument(
        "--type",
        dest="ticker_type",
        default=None,
        metavar="CODE",
        help="Code type Massive (CS, ETF, ADX, …) — voir tickers types",
    )
    p_search.add_argument(
        "--exchange",
        default=None,
        metavar="MIC",
        help="MIC primary_exchange (ex: XNYS, XNAS)",
    )
    p_search.add_argument("--active", action="store_true", help="Uniquement actifs")
    p_search.add_argument(
        "--inactive",
        action="store_true",
        help="Uniquement inactifs / delistés",
    )
    p_search.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Max lignes affichées (n'altère pas le total ni --output/--add)",
    )
    p_search.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help="Écrit le résultat complet en Parquet",
    )
    p_search.add_argument(
        "--add",
        action="store_true",
        help="Ajoute les résultats matchés à config.toml [instruments]",
    )
    p_search.add_argument(
        "--yes",
        action="store_true",
        help="Avec --add: confirme sans prompt si plusieurs matches",
    )
    p_search.add_argument(
        "--no-cascade",
        action="store_true",
        help="N'auto-refresh pas le cache tickers si absent/périmé",
    )

    return parser


# --- Commandes ---



def _cmd_setup_key(args: argparse.Namespace) -> int:
    """Commande ``setup-key`` : écrit la clé API dans ``~/.config/myquantstore/.env``."""
    from myquantstore.onboarding import write_api_key

    env_path = get_user_env_path()
    env_path.parent.mkdir(parents=True, exist_ok=True)

    overwrite = bool(getattr(args, "yes", False))
    if env_path.exists() and not overwrite:
        existing_content = env_path.read_text(encoding="utf-8")
        for line in existing_content.splitlines():
            if line.startswith("MASSIVE_API_KEY=") and len(line) > len("MASSIVE_API_KEY="):
                console.print(f"[yellow]Une clé API existe déjà dans {env_path}[/yellow]")
                if args.api_key is not None:
                    console.print("[dim]Utilisez --yes pour écraser.[/dim]")
                    return 1
                confirm = input("Voulez-vous l'écraser ? (o/N) : ").strip().lower()
                if confirm != "o":
                    console.print("Abandon — .env inchangé.")
                    return 0
                overwrite = True
                break

    api_key = (args.api_key or "").strip()
    if not api_key:
        console.print("[bold]Configuration de la clé API Massive.com[/bold]")
        api_key = getpass.getpass("Entrez votre clé API (masquée) : ").strip()
    if not api_key:
        console.print("[red]Clé API vide — abandon[/red]")
        return 1

    base_url = args.base_url or "https://api.massive.com"
    try:
        write_api_key(api_key, base_url=base_url, env_path=env_path, overwrite=True)
    except ValueError as exc:
        console.print(f"[red]Erreur:[/red] {exc}")
        return 1

    console.print(f"[green].env créé avec succès :[/green] {env_path}")
    console.print(f"  Clé API : {'*' * 8}{api_key[-4:]}")
    console.print(f"  Base URL : {base_url}")
    console.print("\n[dim]Le fichier .env n'est jamais committé (.gitignore).[/dim]")
    return 0


def _cmd_init(args: argparse.Namespace) -> int:
    """Commande ``init`` : bootstrap XDG + config."""
    import getpass as _getpass

    from myquantstore.onboarding import init_workspace

    api_key = args.api_key
    no_key = bool(args.no_key)
    if not no_key and api_key is None and sys.stdin.isatty():
        console.print("[bold]Clé API Massive.com[/bold] (Entrée pour ignorer)")
        try:
            typed = _getpass.getpass("Clé API (masquée, optionnel) : ").strip()
        except (EOFError, KeyboardInterrupt):
            typed = ""
            console.print()
        api_key = typed or None
        if api_key is None:
            no_key = True

    try:
        summary = init_workspace(
            full=bool(args.full),
            force=bool(args.force),
            api_key=api_key,
            base_url=args.base_url or "https://api.massive.com",
            no_key=no_key,
        )
    except FileExistsError as exc:
        console.print(f"[red]Erreur:[/red] {exc}")
        return 1
    except ValueError as exc:
        console.print(f"[red]Erreur:[/red] {exc}")
        return 1

    console.print("[bold]== myquantstore init ==[/bold]")
    if summary["config_created"]:
        console.print(
            f"[green]config.toml[/green] ← {summary['template']} → {summary['config_path']}"
        )
    elif summary["config_skipped"]:
        console.print(
            f"[yellow]config.toml inchangé[/yellow] ({summary['config_path']}) — --force pour écraser"
        )
    console.print(f"dirs : {summary['config_dir']} ; {summary['data_root']}/{{data,cache,logs}}")
    if summary["env_created"]:
        console.print(f"[green].env[/green] → {summary['env_path']}")
    elif not no_key and summary.get("env_skipped"):
        console.print(f"[dim].env existant : {summary['env_path']}[/dim]")
    else:
        console.print("[dim]Pas de clé API (setup-key ou init -k plus tard)[/dim]")

    console.print("\n[bold]Prochaines étapes[/bold]")
    console.print("  myquantstore doctor")
    console.print("  myquantstore fetch --dry-run")
    console.print("  myquantstore fetch")
    console.print("  myquantstore schedule install   # samedi 01h00")
    return 0


def _cmd_doctor(args: argparse.Namespace) -> int:
    """Commande ``doctor`` : diagnostic install."""
    from myquantstore.onboarding import run_doctor

    report = run_doctor(ping_api=bool(getattr(args, "ping", False)))
    console.print("[bold]== myquantstore doctor ==[/bold]")
    for check in report.checks:
        if check.ok:
            mark = "[green]OK[/green]"
        elif check.blocking:
            mark = "[red]FAIL[/red]"
        else:
            mark = "[yellow]WARN[/yellow]"
        console.print(f"  {mark}  {check.name}: {check.detail}")
    if report.ok:
        console.print("\n[green]Aucun problème bloquant.[/green]")
        return 0
    console.print("\n[bold red]Problèmes bloquants détectés.[/bold red]")
    return 1


def _cmd_schedule(settings: Settings | None, args: argparse.Namespace) -> int:
    """Commande ``schedule`` : install / run / status / show / uninstall."""
    from myquantstore.schedule import (
        DEFAULT_CRON,
        DEFAULT_ON_CALENDAR,
        cron_status,
        detect_backend,
        install_cron,
        install_systemd,
        render_cron_block,
        render_service_unit,
        render_timer_unit,
        resolve_binary,
        run_scheduled_job,
        systemd_status,
        uninstall_cron,
        uninstall_systemd,
    )

    sub = getattr(args, "schedule_command", None)
    if sub is None:
        console.print("[red]Sous-commande requise:[/red] install|run|status|show|uninstall")
        console.print("[dim]myquantstore schedule -h[/dim]")
        return 1

    if sub == "run":
        console.print("[bold]== schedule run ==[/bold]")
        console.print("  1) fetch → 2) aggregate → 3) status --check")
        rc = run_scheduled_job(
            fetch_args=getattr(args, "fetch_args", "") or "",
            skip_aggregate=bool(getattr(args, "skip_aggregate", False)),
            skip_status=bool(getattr(args, "skip_status", False)),
            main_fn=main,
        )
        if rc == 0:
            console.print("[green]Job terminé avec succès.[/green]")
        else:
            console.print(f"[red]Job terminé avec code {rc}.[/red]")
        return rc

    if sub == "status":
        console.print("[bold]== schedule status ==[/bold]")
        console.print(f"  binary : {resolve_binary()}")
        sd = systemd_status()
        if sd.get("installed"):
            console.print(
                f"  systemd : installed enabled={sd.get('enabled')} "
                f"active={sd.get('active')} next={sd.get('next') or '?'}"
            )
            console.print(f"    timer : {sd.get('timer_path')}")
        else:
            console.print("  systemd : non installé")
        cr = cron_status()
        if cr.get("installed"):
            console.print(f"  cron    : {cr.get('line')}")
        else:
            console.print("  cron    : non installé")
        return 0

    if sub == "show":
        backend = args.backend
        if backend == "auto":
            backend = detect_backend()
        when = args.when
        fetch_args = getattr(args, "fetch_args", "") or ""
        binary = resolve_binary()
        console.print(f"[bold]== schedule show ({backend}) ==[/bold]")
        console.print(f"binary: {binary}")
        if backend == "systemd":
            cal = when or DEFAULT_ON_CALENDAR
            console.print("\n--- myquantstore-fetch.service ---")
            console.print(render_service_unit(binary=binary, fetch_args=fetch_args))
            console.print("--- myquantstore-fetch.timer ---")
            console.print(render_timer_unit(on_calendar=cal))
        else:
            sched = when or DEFAULT_CRON
            console.print("\n--- crontab block ---")
            console.print(render_cron_block(schedule=sched, binary=binary, fetch_args=fetch_args))
        return 0

    if sub == "install":
        backend = args.backend
        if backend == "auto":
            backend = detect_backend()
            console.print(f"[dim]backend auto → {backend}[/dim]")
        when = args.when
        fetch_args = getattr(args, "fetch_args", "") or ""
        dry = bool(getattr(args, "dry_run", False))
        try:
            if backend == "systemd":
                result = install_systemd(
                    on_calendar=when or DEFAULT_ON_CALENDAR,
                    fetch_args=fetch_args,
                    dry_run=dry,
                )
                if dry:
                    console.print(result["service"])
                    console.print(result["timer"])
                    console.print(f"[dim]→ {result['service_path']}[/dim]")
                    console.print(f"[dim]→ {result['timer_path']}[/dim]")
                else:
                    console.print(
                        f"[green]systemd timer installé[/green] "
                        f"({result.get('on_calendar')}) → {result.get('timer_path')}"
                    )
                    console.print(
                        "[dim]Astuce: si la machine est souvent éteinte hors session, "
                        "`loginctl enable-linger $USER`[/dim]"
                    )
            else:
                result = install_cron(
                    schedule=when or DEFAULT_CRON,
                    fetch_args=fetch_args,
                    dry_run=dry,
                )
                if dry:
                    console.print(result["block"])
                else:
                    console.print(
                        f"[green]cron installé[/green] ({result.get('schedule')})\n"
                        f"{result.get('block')}"
                    )
        except RuntimeError as exc:
            console.print(f"[red]Erreur schedule install:[/red] {exc}")
            return 1
        return 0

    if sub == "uninstall":
        backend = getattr(args, "backend", "all")
        targets = ["systemd", "cron"] if backend in ("all", "auto") else [backend]
        for t in targets:
            try:
                if t == "systemd":
                    r = uninstall_systemd()
                    console.print(f"systemd : removed={r.get('removed')}")
                else:
                    r = uninstall_cron()
                    console.print(f"cron : removed={r.get('removed')}")
            except RuntimeError as exc:
                console.print(f"[yellow]{t}:[/yellow] {exc}")
        return 0

    console.print(f"[red]Sous-commande inconnue:[/red] {sub}")
    return 1


def _cmd_config(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``config`` : affiche la configuration résolue + chemin du fichier."""
    try:
        resolved_config = resolve_config_path()
    except FileNotFoundError:
        resolved_config = None

    console.print("[bold]== Configuration MyQuantStore ==[/bold]")
    if resolved_config is not None:
        console.print(f"[dim]Fichier : {resolved_config}[/dim]")
    else:
        console.print("[dim]Fichier : [red]introuvable[/red][/dim]")

    table = Table(show_header=True)
    table.add_column("Paramètre", style="cyan")
    table.add_column("Valeur")

    api_key_display = f"{'*' * 8}{settings.api_key[-4:]}" if settings.api_key else "[red]NON CONFIGURÉE[/red]"

    table.add_row("api_key", api_key_display)
    table.add_row("base_url", settings.base_url)
    # Instruments par type
    table.add_row("instruments.futures", ", ".join(settings.futures) or "[dim](vide)[/dim]")
    table.add_row("instruments.forex", ", ".join(settings.forex) or "[dim](vide)[/dim]")
    table.add_row("instruments.stocks", ", ".join(settings.stocks) or "[dim](vide)[/dim]")
    table.add_row("instruments.indices", ", ".join(settings.indices) or "[dim](vide)[/dim]")
    table.add_row("instruments.options", ", ".join(settings.options) or "[dim](vide)[/dim]")
    # Fetch
    table.add_row("timeframe", settings.timeframe)
    table.add_row("overlap_buffer_days", str(settings.overlap_buffer_days))
    hm = ", ".join(f"{k}={v}" for k, v in settings.history_months.items())
    table.add_row("history_months", hm)
    table.add_row("requests_per_minute", str(settings.requests_per_minute))
    table.add_row("page_limit", str(settings.page_limit))
    table.add_row("max_retries", str(settings.max_retries))
    # Futures
    table.add_row("futures.days_before_expiry", str(settings.days_before_expiry))
    table.add_row("futures.contracts_page_limit", str(settings.contracts_page_limit))
    table.add_row("futures.snapshot_interval_months", str(settings.contracts_snapshot_interval_months))
    # Stocks
    table.add_row("stocks.splits_page_limit", str(settings.splits_page_limit))
    table.add_row("stocks.dividends_page_limit", str(settings.dividends_page_limit))
    # Cache
    table.add_row("instrument_cache.ttl_days", str(settings.instrument_cache_ttl_days))
    # Storage
    table.add_row("data_dir", settings.data_dir)
    table.add_row("cache_dir", settings.cache_dir)
    table.add_row("log_dir", settings.log_dir)
    # Divers
    table.add_row("data_quality_trigger", str(settings.data_quality_trigger))
    table.add_row("log_level", settings.log_level)
    table.add_row("display_max_rows", str(settings.display_max_rows))
    table.add_row("display_max_columns", str(settings.display_max_columns))

    console.print(table)

    if args.paths:
        console.print("\n[bold]== Chemins des fichiers ==[/bold]")
        console.print(f"  config.toml : {resolved_config or get_user_config_path()}")
        console.print(f"  .env        : {get_user_env_path()}")
        console.print(f"  XDG config  : {get_user_config_path()}")
        console.print(f"  fallback    : {get_repo_config_path()}")
        console.print(f"  data_dir    : {Path(settings.data_dir).expanduser().resolve()}")
        console.print(f"  cache_dir   : {Path(settings.cache_dir).expanduser().resolve()}")
        console.print(f"  log_dir     : {Path(settings.log_dir).expanduser().resolve()}")

    return 0


def _cmd_fetch(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``fetch`` : historise les chandeliers OHLCV (multi-type)."""
    from myquantstore.api.client import MassiveClient
    from myquantstore.instruments import RESOLUTION_1MIN
    from myquantstore.pipeline.cascade import ensure_pre_fetch, print_status_snapshot
    from myquantstore.pipeline.historian import resolve_fetch_resolutions, run_fetch

    try:
        instruments = _resolve_instruments(settings, args.instrument, args.type)
        resolutions = resolve_fetch_resolutions(settings, getattr(args, "timeframe", "all"))
    except ValueError as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1

    needs_massive = RESOLUTION_1MIN in resolutions
    if needs_massive and not settings.api_key and not args.dry_run:
        console.print("[red]Erreur:[/red] Aucune clé API configurée. Exécutez 'myquantstore setup-key'.")
        return 1

    # Client Massive optionnel si only 1day
    if needs_massive or not args.dry_run:
        client_cm = MassiveClient(settings)
    else:
        client_cm = MassiveClient(settings)

    with client_cm as client:
        if not args.no_cascade:
            print_status_snapshot(instruments, settings)

        if needs_massive:
            for inst in instruments:
                if inst.type.implemented:
                    try:
                        ensure_pre_fetch(inst, client, settings, no_cascade=args.no_cascade)
                    except Exception as e:
                        console.print(f"[red]Erreur cascade pour {inst.key}:[/red] {e}")
                        return 1

        results = run_fetch(
            settings,
            client,
            instruments=instruments,
            force=args.force,
            dry_run=args.dry_run,
            resolutions=resolutions,
        )

    console.print("\n[bold]== Résumé ==[/bold]")
    stale_count = 0
    for key, result in results.items():
        status = result.get("status", "unknown")
        candles = result.get("candles", 0)
        cov_suffix = _format_coverage_suffix(result)
        if result.get("stale"):
            stale_count += 1
        if status == "skipped":
            err = result.get("error")
            extra = f" — {err}" if err else " (déjà fait aujourd'hui / n/a)"
            force_hint = " — utilisez --force" if result.get("stale") else ""
            console.print(f"  {key}: [yellow]SKIP[/yellow]{extra}{cov_suffix}{force_hint}")
        elif status == "dry_run":
            console.print(
                f"  {key}: [blue]DRY-RUN[/blue] ({result.get('segments', [])}){cov_suffix}"
            )
        elif status == "ok":
            console.print(f"  {key}: [green]OK[/green] ({candles} chandeliers){cov_suffix}")
        elif status == "not_implemented":
            console.print(f"  {key}: [yellow]NON IMPLÉMENTÉ[/yellow] ({result.get('error', '')})")
        else:
            console.print(f"  {key}: [red]{status}[/red]{cov_suffix}")

    if stale_count:
        console.print(
            f"\n[bold red]⚠ {stale_count} job(s) avec données périmées (STALE)[/bold red]"
        )

    return 0


def _format_coverage_suffix(result: dict) -> str:
    """Suffixe latest/lag/STALE pour le résumé fetch."""
    latest = result.get("latest")
    lag = result.get("lag_days")
    if latest is None and lag is None:
        return ""
    parts: list[str] = []
    if latest is not None:
        parts.append(f"latest={latest}")
    if lag is not None:
        parts.append(f"lag={lag}j")
    body = " ".join(parts)
    if result.get("stale"):
        return f" — {body} [red]⚠ STALE[/red]"
    return f" — {body}"


def _cmd_aggregate(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``aggregate`` : régénère le cache agrégé (générique multi-type)."""
    from myquantstore.api.client import MassiveClient
    from myquantstore.pipeline.aggregator import aggregate
    from myquantstore.pipeline.cascade import ensure_raw_dumps, print_status_snapshot
    from myquantstore.pipeline.historian import resolve_fetch_resolutions
    from myquantstore.storage.raw_dumps import raw_dumps_exist

    try:
        instruments = _resolve_instruments(settings, args.instrument, args.type)
        resolutions = resolve_fetch_resolutions(settings, getattr(args, "timeframe", "all"))
    except ValueError as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1

    for inst in instruments:
        for resolution in resolutions:
            if not raw_dumps_exist(inst, settings, resolution=resolution):
                if settings.api_key:
                    with MassiveClient(settings) as client:
                        if not args.no_cascade:
                            print_status_snapshot([inst], settings)
                        try:
                            ensure_raw_dumps(
                                inst,
                                client,
                                settings,
                                no_cascade=args.no_cascade,
                                resolution=resolution,
                            )
                        except NotImplementedError as e:
                            console.print(
                                f"  {inst.key}[{resolution}]: [yellow]NON IMPLÉMENTÉ[/yellow] ({e})"
                            )
                            continue
                        except Exception as e:
                            console.print(f"[red]Erreur cascade {inst.key}[{resolution}]:[/red] {e}")
                            return 1
                else:
                    console.print(
                        f"[yellow]Skip[/yellow] {inst.key}[{resolution}]: pas de dumps "
                        "(et pas de clé API pour cascade)."
                    )
                    continue

            df = aggregate(inst, settings, resolution=resolution)
            console.print(
                f"  {inst.key}[{resolution}]: [green]OK[/green] ({df.height} lignes agrégées)"
            )

    return 0


def _timescale_to_query_params(unit: str, nb: int) -> tuple[str, int, int]:
    """Retourne ``(resolution, k_minutes, k_days)`` pour query/chart."""
    from myquantstore.instruments import RESOLUTION_1DAY, RESOLUTION_1MIN

    if unit == "min":
        return RESOLUTION_1MIN, nb, 1
    if unit == "hour":
        return RESOLUTION_1MIN, nb * 60, 1
    if unit == "day":
        return RESOLUTION_1DAY, 1, nb
    if unit == "week":
        return RESOLUTION_1DAY, 1, nb * 7
    raise ValueError(f"timescale_unit '{unit}' non supporté")


def _cmd_query(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``query`` : interroge l'historique continu."""
    from datetime import time as time_cls

    from myquantstore.api.client import MassiveClient
    from myquantstore.instruments import RESOLUTION_1DAY
    from myquantstore.pipeline.cascade import ensure_aggregate, print_status_snapshot
    from myquantstore.query.reader import query

    try:
        instrument = _resolve_instrument_arg(settings, args.instrument, args.type)
    except ValueError as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1

    start = datetime.fromisoformat(args.start) if args.start else None
    end = datetime.fromisoformat(args.end) if args.end else None

    intraday_begin = None
    intraday_end = None
    if args.intraday_begin:
        try:
            intraday_begin = time_cls.fromisoformat(args.intraday_begin)
        except ValueError:
            console.print(f"[red]Erreur:[/red] --intraday-begin invalide : '{args.intraday_begin}'. Format: HH:MM.")
            return 1
    if args.intraday_end:
        try:
            intraday_end = time_cls.fromisoformat(args.intraday_end)
        except ValueError:
            console.print(f"[red]Erreur:[/red] --intraday-end invalide : '{args.intraday_end}'. Format: HH:MM.")
            return 1

    if (intraday_begin is None) != (intraday_end is None):
        console.print("[red]Erreur:[/red] --intraday-begin et --intraday-end doivent être fournis ensemble.")
        return 1
    if intraday_begin is not None and intraday_end is not None and intraday_begin == intraday_end:
        console.print("[red]Erreur:[/red] --intraday-begin et --intraday-end doivent être différents.")
        return 1

    try:
        resolution, k_minutes, k_days = _timescale_to_query_params(
            args.timescale_unit, args.timescale_nb
        )
    except ValueError as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1

    if not args.no_cascade:
        print_status_snapshot([instrument], settings)

    chain = None
    if not args.no_cascade and instrument.type.implemented:
        # 1day n'a pas besoin de clé Massive ; 1min si
        need_client = resolution != RESOLUTION_1DAY or bool(settings.api_key)
        if need_client:
            with MassiveClient(settings) as client:
                try:
                    chain = ensure_aggregate(
                        instrument,
                        client,
                        settings,
                        no_cascade=args.no_cascade,
                        resolution=resolution,
                    )
                except Exception as e:
                    console.print(f"[red]Erreur cascade:[/red] {e}")
                    return 1
        else:
            from myquantstore.chains import build_chain
            from myquantstore.storage.aggregate_cache import aggregate_exists

            if not aggregate_exists(instrument, settings, resolution=resolution):
                console.print(
                    f"[red]Erreur:[/red] Aucun agrégé {resolution} pour {instrument.key}. "
                    "Exécutez 'myquantstore fetch --timeframe 1day'."
                )
                return 1
            chain = build_chain(instrument)
    else:
        from myquantstore.chains import build_chain
        from myquantstore.storage.aggregate_cache import aggregate_exists

        if not aggregate_exists(instrument, settings, resolution=resolution):
            console.print(
                f"[red]Erreur:[/red] Aucun agrégé {resolution} pour {instrument.key}. "
                "Exécutez 'myquantstore aggregate' d'abord."
            )
            return 1
        if instrument.type == InstrumentType.FUTURES:
            from myquantstore.contracts.cache import ContractsCache

            cache = ContractsCache(instrument.symbol, settings)
            contracts_df = cache.get()
            chain = build_chain(
                instrument,
                contracts_df=contracts_df,
                days_before_expiry=settings.days_before_expiry,
            )
        else:
            chain = build_chain(instrument)

    try:
        df = query(
            instrument,
            settings,
            chain,
            start=start,
            end=end,
            k_minutes=k_minutes,
            k_days=k_days,
            week_aligned=(args.timescale_unit == "week"),
            resolution=resolution,
            intraday_begin=intraday_begin,
            intraday_end=intraday_end,
            adjust_rollover=args.adjust,
            normalize_tick_size=args.normalize_tick_size,
            check_ticksize_accuracy=args.check_ticksize_accuracy,
            no_split=args.no_split,
            dedup_timestamps=not args.no_dedup_timestamps,
        )
    except ValueError as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1
    except NotImplementedError as e:
        console.print(f"[yellow]Non implémenté:[/yellow] {e}")
        return 1

    if args.output:
        df.write_parquet(args.output)
        console.print(f"[green]Écrit:[/green] {args.output} ({df.height} lignes)")
    else:
        sort_col = "bucket_start" if "bucket_start" in df.columns else "session_end_date"
        # --limit : plafond d'affichage uniquement (comme display_max_rows)
        _render_df(df, settings, sort_col=sort_col, max_rows=args.limit)

    return 0


def _cmd_status(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``status`` : cache tickers global + état par instrument."""
    from myquantstore.chains import build_chain
    from myquantstore.contracts.cache import ContractsCache
    from myquantstore.corporate_actions.cache import CorporateActionsCache
    from myquantstore.storage.coverage import (
        HealthLevel,
        assess_instrument_health,
    )
    from myquantstore.storage.parquet_io import read_meta
    from myquantstore.storage.raw_dumps import list_runs, list_tickers
    from myquantstore.yahoo_actions.cache import YahooActionsCache

    # Section globale (indépendante du filtre --instrument / --type)
    _print_tickers_cache_status(settings)
    if getattr(args, "tickers", False):
        return 0

    try:
        instruments = _resolve_instruments(settings, args.instrument, args.type)
    except ValueError as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1

    today = datetime.now(UTC).date()
    any_problem = False

    for inst in instruments:
        console.print(f"\n[bold]== {inst.key} ==[/bold]")

        # Cache de listing (type-dépendant)
        if inst.type == InstrumentType.FUTURES:
            cache = ContractsCache(inst.symbol, settings)
            if cache.exists:
                last_fetched = cache.get_last_fetched()
                meta = read_meta(cache.parquet_path)
                row_count = meta.get("row_count", "?") if meta else "?"
                console.print(f"  Cache contrats : [green]présent[/green] ({row_count} contrats, last_fetched={last_fetched})")
                try:
                    contracts_df = cache.get()
                    chain = build_chain(inst, contracts_df=contracts_df, days_before_expiry=settings.days_before_expiry)
                    if len(chain) > 0:
                        active_ticker = chain.active_contract(today)
                        console.print(f"  Contrat actif aujourd'hui ({today}) : [cyan]{active_ticker}[/cyan]")
                        chain_table = chain.to_table()
                        if not chain_table.is_empty():
                            _render_df(chain_table, settings, sort_col="rollover_date")
                except Exception as e:
                    console.print(f"  [red]Erreur RolloverChain:[/red] {e}")
            else:
                console.print("  Cache contrats : [red]absent[/red]")
        elif inst.type == InstrumentType.STOCKS:
            sc = CorporateActionsCache(inst.symbol, "splits", settings)
            if sc.exists:
                last_fetched = sc.get_last_fetched()
                console.print(f"  Cache splits : [green]présent[/green] (last_fetched={last_fetched})")
            else:
                console.print("  Cache splits : [red]absent[/red]")
        else:
            console.print(f"  Cache listing : [dim]n/a ({inst.type.value})[/dim]")

        health = assess_instrument_health(inst, settings, today=today)
        if health.has_problems:
            any_problem = True

        tickers = list_tickers(inst, settings)
        for res, cov in health.coverages.items():
            if tickers:
                total_dumps = sum(len(list_runs(inst, t, settings, resolution=res)) for t in tickers)
                if total_dumps:
                    console.print(
                        f"  Dumps [{res}] : [green]présent[/green] "
                        f"({len(tickers)} ticker(s), {total_dumps} dump(s))"
                    )
                else:
                    console.print(f"  Dumps [{res}] : [red]absent[/red]")
            else:
                console.print(f"  Dumps [{res}] : [red]absent[/red]")

            if not cov.present:
                console.print(f"  Agrégé [{res}] : [red]absent[/red]")
                continue

            rows = cov.rows if cov.rows is not None else "?"
            if cov.min_date is not None and cov.max_date is not None:
                plage = f"plage={cov.min_date} à {cov.max_date}"
            else:
                plage = "plage=n/a"
            lag_part = f", lag={cov.lag_days}j" if cov.lag_days is not None else ""
            if cov.is_stale(settings):
                console.print(
                    f"  Agrégé [{res}] : [red]STALE[/red] ({rows} lignes, {plage}{lag_part}) "
                    f"[red]⚠ lag > {settings.stale_lag_days_for(res)}j[/red]"
                )
            else:
                console.print(
                    f"  Agrégé [{res}] : [green]OK[/green] ({rows} lignes, {plage}{lag_part})"
                )

        for issue in health.issues:
            if issue.code == "stale":
                continue  # déjà rendu sur la ligne Agrégé
            if issue.code == "missing_aggregate":
                continue  # déjà rendu "absent"
            color = "red" if issue.level == HealthLevel.STALE else "yellow"
            console.print(f"  [{color}]⚠ {issue.message}[/{color}]")

        if inst.type == InstrumentType.STOCKS:
            for kind in ("splits", "dividends"):
                yc = YahooActionsCache(inst.symbol, kind, settings)
                if yc.exists:
                    console.print(
                        f"  Yahoo actions {kind} : [green]présent[/green] "
                        f"(last_fetched={yc.get_last_fetched()})"
                    )
                else:
                    console.print(f"  Yahoo actions {kind} : [red]absent[/red]")

    if getattr(args, "check", False) and any_problem:
        console.print("\n[bold red]status --check : problèmes de fraîcheur détectés[/bold red]")
        return 1

    return 0


def _print_tickers_cache_status(settings: Settings) -> None:
    """Affiche l'état du cache référentiel tickers (markets / shards / types)."""
    from myquantstore.tickers.cache import KNOWN_MARKETS, TickersCache, TickerTypesCache

    console.print("\n[bold]== Cache tickers ==[/bold]")
    cache = TickersCache(settings)
    types_cache = TickerTypesCache(settings)
    ttl = settings.instrument_cache_ttl_days

    console.print(f"  Répertoire : [dim]{settings.tickers_cache_dir()}[/dim]")
    console.print(f"  TTL        : {ttl} jour(s)")

    # types.parquet
    if types_cache.exists:
        last = types_cache.get_last_fetched() or "?"
        state = "[green]frais[/green]" if types_cache.is_fresh() else "[yellow]périmé[/yellow]"
        console.print(f"  Types      : [green]présent[/green] ({state}, last_fetched={last})")
    else:
        console.print("  Types      : [red]absent[/red]")

    # Shards market × active|inactive
    present = cache.inventory()
    if not present:
        legacy = cache.legacy_all_path()
        if legacy.exists():
            console.print(
                f"  Shards     : [yellow]layout legacy[/yellow] ({legacy.name}) — "
                "relancez [cyan]myquantstore tickers refresh[/cyan]"
            )
        else:
            console.print(
                "  Shards     : [red]aucun[/red] — "
                "exécutez [cyan]myquantstore tickers refresh[/cyan]"
            )
            missing = ", ".join(KNOWN_MARKETS)
            console.print(f"  Markets connus (API) : [dim]{missing}[/dim]")
        return

    cached_markets = sorted({s.market for s in present})
    console.print(
        f"  Markets en cache : [cyan]{', '.join(cached_markets)}[/cyan] "
        f"({len(present)} shard(s))"
    )
    not_cached = [m for m in KNOWN_MARKETS if m not in cached_markets]
    if not_cached:
        console.print(f"  Markets absents  : [dim]{', '.join(not_cached)}[/dim]")

    table = Table(show_header=True, box=None, padding=(0, 1))
    table.add_column("Market", style="cyan")
    table.add_column("Shard")
    table.add_column("Lignes", justify="right")
    table.add_column("État")
    table.add_column("last_fetched")

    for s in present:
        if s.fresh:
            state = "[green]frais[/green]"
        else:
            state = "[yellow]périmé[/yellow]"
        rows = str(s.row_count) if s.row_count is not None else "?"
        last = s.last_fetched_at or "?"
        table.add_row(s.market, s.bucket, rows, state, last)

    console.print(table)


def _cmd_chart(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``chart`` : lance le serveur de visualisation interactive."""
    from datetime import time as time_cls

    from myquantstore.chart.server import ChartDefaults, run_server
    from myquantstore.instruments import RESOLUTION_1DAY, RESOLUTION_1MIN
    from myquantstore.tickers.yahoo_map import YAHOO_DAILY_TYPES

    # Résoudre l'instrument par défaut
    all_instruments = settings.all_instruments()
    if not all_instruments:
        console.print("[red]Erreur:[/red] Aucun instrument configuré.")
        return 1

    default_inst = None
    if args.instrument:
        try:
            default_inst = _resolve_instrument_arg(settings, args.instrument, args.type)
        except ValueError as e:
            console.print(f"[red]Erreur:[/red] {e}")
            return 1
    else:
        default_inst = all_instruments[0]

    timescale_unit = args.timescale_unit or settings.default_timescale_unit
    timescale_nb = args.timescale_nb or settings.default_timescale_nb
    nb_candle = args.nb_candle or settings.default_nb_candle
    port = args.port or settings.chart_port
    host = args.host or settings.chart_host
    mdns = args.mdns if args.mdns is not None else settings.chart_mdns

    if nb_candle > settings.max_visible_candles:
        console.print(
            f"[yellow]Warning:[/yellow] --nb-candle {nb_candle} > max_visible_candles "
            f"{settings.max_visible_candles}, fallback à {settings.max_visible_candles}"
        )
        nb_candle = settings.max_visible_candles

    intraday_begin = None
    intraday_end = None
    if args.intraday_begin:
        try:
            intraday_begin = time_cls.fromisoformat(args.intraday_begin)
        except ValueError:
            console.print(f"[red]Erreur:[/red] --intraday-begin invalide : '{args.intraday_begin}'. Format: HH:MM.")
            return 1
    if args.intraday_end:
        try:
            intraday_end = time_cls.fromisoformat(args.intraday_end)
        except ValueError:
            console.print(f"[red]Erreur:[/red] --intraday-end invalide : '{args.intraday_end}'. Format: HH:MM.")
            return 1
    if (intraday_begin is None) != (intraday_end is None):
        console.print("[red]Erreur:[/red] --intraday-begin et --intraday-end doivent être fournis ensemble.")
        return 1
    if intraday_begin is not None and intraday_end is not None and intraday_begin == intraday_end:
        console.print("[red]Erreur:[/red] --intraday-begin et --intraday-end doivent être différents.")
        return 1

    # Construire les chaînes pour tous les instruments avec agrégé (1min et/ou 1day)
    from myquantstore.api.client import MassiveClient
    from myquantstore.chains import build_chain
    from myquantstore.contracts.cache import ContractsCache
    from myquantstore.pipeline.cascade import ensure_aggregate
    from myquantstore.storage.aggregate_cache import aggregate_exists

    # Miniatures dashboard = 1day only : fetch/aggregate manquants au démarrage
    need_1day = [
        inst
        for inst in all_instruments
        if inst.type in YAHOO_DAILY_TYPES
        and not aggregate_exists(inst, settings, resolution=RESOLUTION_1DAY)
    ]
    if need_1day and not args.no_cascade:
        console.print(
            f"[cyan]Chart:[/cyan] agrégé 1day manquant pour {len(need_1day)} instrument(s) "
            "— fetch Yahoo…"
        )
        with MassiveClient(settings) as client:
            for inst in need_1day:
                try:
                    ensure_aggregate(
                        inst, client, settings, no_cascade=False, resolution=RESOLUTION_1DAY
                    )
                    console.print(f"  {inst.key} [1day]: [green]OK[/green]")
                except Exception as e:
                    console.print(f"  {inst.key} [1day]: [yellow]échec[/yellow] — {e}")
    elif need_1day and args.no_cascade:
        for inst in need_1day:
            console.print(
                f"[yellow]Warning:[/yellow] Pas d'agrégé 1day pour {inst.key} "
                "(--no-cascade) — miniature indisponible"
            )

    instruments_map: dict[str, Instrument] = {}
    chains_map: dict[str, InstrumentChain] = {}

    for inst in all_instruments:
        has_1min = aggregate_exists(inst, settings, resolution=RESOLUTION_1MIN)
        has_1day = aggregate_exists(inst, settings, resolution=RESOLUTION_1DAY)
        if not has_1min and not has_1day:
            console.print(
                f"[yellow]Warning:[/yellow] Aucun agrégé pour {inst.key} — non disponible dans le chart"
            )
            continue
        try:
            if inst.type == InstrumentType.FUTURES:
                cache = ContractsCache(inst.symbol, settings)
                contracts_df = cache.get()
                chain = build_chain(
                    inst, contracts_df=contracts_df, days_before_expiry=settings.days_before_expiry
                )
            else:
                chain = build_chain(inst)
            instruments_map[inst.key] = inst
            chains_map[inst.key] = chain
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Chaîne {inst.key} échouée: {e}")

    if not instruments_map:
        console.print(
            "[red]Erreur:[/red] Aucun instrument disponible. "
            "Exécutez 'myquantstore fetch' + 'myquantstore aggregate' d'abord."
        )
        return 1

    # Si instrument CLI absent des maps (pas d'agrégé), fallback premier dispo
    open_key = default_inst.key
    if open_key not in instruments_map:
        if args.instrument:
            console.print(
                f"[red]Erreur:[/red] Instrument '{default_inst.key}' n'a pas d'agrégé. "
                f"Disponibles: {list(instruments_map.keys())}"
            )
            return 1
        open_key = next(iter(instruments_map))

    defaults = ChartDefaults(
        default_product=open_key,
        timescale_unit=timescale_unit,
        timescale_nb=timescale_nb,
        nb_candle=nb_candle,
        max_visible_candles=settings.max_visible_candles,
        buffer_multiplier=settings.buffer_multiplier,
        fetch_chunk_size=settings.fetch_chunk_size,
        intraday_begin=intraday_begin,
        intraday_end=intraday_end,
        normalize_tick_size=args.normalize_tick_size,
        adjust_rollover=args.adjust,
        no_split=args.no_split,
        thumbnail_lookback_days=settings.thumbnail_lookback_days,
    )

    start_url = (
        f"http://{host}:{port}/{open_key}" if args.instrument else f"http://{host}:{port}/"
    )
    console.print(f"[green]MyQuantStore Chart[/green] — {start_url}")
    console.print(f"  Dashboard: http://{host}:{port}/")
    console.print(f"  Instruments: {list(instruments_map.keys())}")
    console.print(
        f"  Timescale: {timescale_nb}{timescale_unit} | Nb candle: {nb_candle} | "
        f"Max visible: {settings.max_visible_candles} | "
        f"Thumbnails: {settings.thumbnail_lookback_days}j"
    )
    if mdns:
        console.print("  mDNS: [green]activé[/green] (accessible sur le réseau local)")
    console.print("  Ctrl+C pour arrêter")

    try:
        run_server(settings, instruments_map, chains_map, defaults, port, host, mdns)
    except KeyboardInterrupt:
        console.print("\n[yellow]Arrêt du serveur...[/yellow]")
    return 0


def _resolve_portfolio_instruments(
    settings: Settings, symbols: list[str] | None
) -> list[Instrument]:
    """Univers portfolio : stocks config, ou liste -i (stocks)."""
    if not symbols:
        return settings.instruments_of_type(InstrumentType.STOCKS)
    out: list[Instrument] = []
    for s in symbols:
        if ":" in s:
            inst = _resolve_instrument_arg(settings, s, None)
        else:
            try:
                inst = settings.resolve_instrument(s, InstrumentType.STOCKS)
            except ValueError:
                inst = _resolve_instrument_arg(settings, s, "stocks")
        if inst.type != InstrumentType.STOCKS:
            raise ValueError(f"{inst.key}: portfolio v1 accepte uniquement stocks")
        out.append(inst)
    return out


def _cmd_portfolio(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``portfolio`` : sous-commandes MPT."""
    sub = getattr(args, "portfolio_command", None)
    if not sub:
        console.print(
            "[red]Sous-commande requise:[/red] stats|corr|cov|optimize|allocate|frontier"
        )
        console.print("  myquantstore portfolio -h")
        return 1

    from myquantstore.analytics.allocate import allocate_discrete, latest_prices_from_panel
    from myquantstore.analytics.metrics import (
        asset_stats,
        correlation_matrix,
        covariance_matrix,
    )
    from myquantstore.analytics.optimize import efficient_frontier, optimize
    from myquantstore.analytics.panel import build_price_panel
    from myquantstore.analytics.report import (
        export_frame,
        print_allocation,
        print_frontier,
        print_matrix,
        print_panel_header,
        print_portfolio,
        print_stats_table,
    )
    from myquantstore.analytics.returns import compute_returns

    try:
        instruments = _resolve_portfolio_instruments(settings, args.instrument)
    except ValueError as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1

    if not instruments:
        console.print("[red]Erreur:[/red] Aucun stock configuré.")
        return 1

    from myquantstore.analytics.risk_free import resolve_risk_free_rate

    rf_quote = resolve_risk_free_rate(settings, cli_rf=args.rf)
    rf_rate = rf_quote.rate
    kind = "log" if args.log_returns else "simple"

    try:
        panel = build_price_panel(
            instruments,
            settings,
            start=args.date_from,
            end=args.date_to,
            timescale=args.timescale,
            adjust_dividends=not args.no_div,
        )
        rets = compute_returns(panel, settings, kind=kind)
    except ValueError as e:
        console.print(f"[red]Erreur panel:[/red] {e}")
        return 1
    except Exception as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1

    print_panel_header(panel, rets)
    rf_extra = ""
    if rf_quote.source == "yahoo" and rf_quote.as_of is not None:
        rf_extra = f" ({rf_quote.yahoo_ticker} as_of={rf_quote.as_of})"
    elif rf_quote.source == "static" and rf_quote.detail.startswith("fallback"):
        rf_extra = " (fallback static)"
    console.print(
        f"  rf={rf_rate:.2%} source={rf_quote.source}{rf_extra} | returns={kind}"
    )

    export_df = None

    max_rows = settings.display_max_rows
    max_cols = settings.display_max_columns

    if sub == "stats":
        export_df = asset_stats(rets, risk_free_rate=rf_rate)
        print_stats_table(export_df, max_rows=max_rows)
    elif sub == "corr":
        export_df = correlation_matrix(rets)
        print_matrix(export_df, title="Corrélation", max_rows=max_rows, max_cols=max_cols)
    elif sub == "cov":
        export_df = covariance_matrix(rets, annualize=True)
        print_matrix(
            export_df, title="Covariance annualisée", max_rows=max_rows, max_cols=max_cols
        )
    elif sub == "optimize":
        import polars as pl

        result = optimize(
            rets,
            args.objective,
            risk_free_rate=rf_rate,
            n_samples=settings.portfolio_frontier_samples,
            seed=settings.portfolio_optim_seed,
        )
        print_portfolio(result, max_rows=max_rows)
        export_df = result.weights_frame(min_weight=0.0).with_columns(
            pl.lit(result.mean_ann).alias("port_mean_ann"),
            pl.lit(result.vol_ann).alias("port_vol_ann"),
            pl.lit(result.sharpe).alias("port_sharpe"),
            pl.lit(result.objective).alias("objective"),
        )
    elif sub == "allocate":
        import polars as pl

        value = args.value if args.value is not None else settings.portfolio_default_value
        result = optimize(
            rets,
            args.objective,
            risk_free_rate=rf_rate,
            n_samples=settings.portfolio_frontier_samples,
            seed=settings.portfolio_optim_seed,
        )
        print_portfolio(result, max_rows=max_rows)
        prices = latest_prices_from_panel(panel)
        try:
            alloc = allocate_discrete(result, prices, value)
        except ValueError as e:
            console.print(f"[red]Erreur allocate:[/red] {e}")
            return 1
        print_allocation(alloc, max_rows=max_rows)
        export_df = alloc.lots_frame().with_columns(
            pl.lit(alloc.value).alias("portfolio_value"),
            pl.lit(alloc.cash).alias("cash"),
            pl.lit(alloc.invested).alias("invested"),
            pl.lit(alloc.drift_l1).alias("drift_l1"),
            pl.lit(alloc.objective).alias("objective"),
        )
    elif sub == "frontier":
        export_df = efficient_frontier(
            rets,
            risk_free_rate=rf_rate,
            n_samples=settings.portfolio_frontier_samples,
            n_points=getattr(args, "points", 40),
            seed=settings.portfolio_optim_seed,
            method=getattr(args, "method", "qp"),
        )
        print_frontier(export_df, max_rows=max_rows)
    else:
        console.print(f"[red]Sous-commande inconnue:[/red] {sub}")
        return 1

    if args.export and export_df is not None:
        export_frame(export_df, args.export)

    return 0


def _cmd_futures_contracts(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``myquantstore futures contracts`` : liste/rafraîchit le cache contrats."""
    import polars as pl

    from myquantstore.api.client import MassiveClient
    from myquantstore.contracts.cache import ContractsCache

    symbols = [args.symbol] if args.symbol else settings.futures

    if not symbols:
        console.print("[yellow]Aucun instrument futures configuré.[/yellow]")
        return 0

    if not settings.api_key and not args.refresh:
        # Lecture seule du cache si pas de clé
        for symbol in symbols:
            cache = ContractsCache(symbol, settings)
            if cache.exists:
                df = cache.get()
                if args.active_only and "active" in df.columns:
                    df = df.filter(pl.col("active") == True)  # noqa: E712
                console.print(f"\n[bold]== futures:{symbol} : {df.height} contrat(s) ==[/bold]")
                _render_df(df, settings, sort_col="last_trade_date")
            else:
                console.print(f"[yellow]futures:{symbol}: cache absent et pas de clé API[/yellow]")
        return 0

    with MassiveClient(settings) as client:
        for symbol in symbols:
            cache = ContractsCache(symbol, settings)
            df = cache.get(client, force_refresh=args.refresh)

            if df.is_empty():
                console.print(f"[yellow]futures:{symbol}: aucun contrat[/yellow]")
                continue

            if args.active_only and "active" in df.columns:
                df = df.filter(pl.col("active") == True)  # noqa: E712

            console.print(f"\n[bold]== futures:{symbol} : {df.height} contrat(s) ==[/bold]")
            _render_df(df, settings, sort_col="last_trade_date")

    return 0


def _cmd_options_contracts(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``myquantstore options contracts`` : scaffold (NotImplementedError)."""
    console.print("[yellow]Non implémenté:[/yellow] La gestion des contrats options est un scaffold.")
    console.print("Les options requièrent une logique de chaîne par strike/call/put non encore développée.")
    return 1


def _resolve_markets_cli(
    markets: list[str] | None,
    *,
    default: tuple[str, ...] | None = None,
) -> list[str] | None:
    """Fusionne --markets. Si default=None et rien fourni → None (tous shards)."""
    from myquantstore.tickers.cache import DEFAULT_MARKETS, parse_markets_arg

    raw: list[str] = []
    if markets:
        raw.extend(markets)
    if not raw:
        return list(default) if default is not None else None
    return parse_markets_arg(raw, default=default or DEFAULT_MARKETS)


def _cmd_tickers_refresh(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``myquantstore tickers refresh`` — shards market×active."""
    from myquantstore.api.client import MassiveClient
    from myquantstore.tickers.cache import (
        DEFAULT_MARKETS,
        TickerTypesCache,
        TickersCache,
        parse_active_buckets,
        parse_markets_arg,
    )

    if not settings.api_key:
        console.print("[red]Erreur:[/red] Aucune clé API. Exécutez 'myquantstore setup-key'.")
        return 1

    raw_markets: list[str] = []
    if args.markets:
        raw_markets.extend(args.markets)
    markets = parse_markets_arg(raw_markets or None, default=DEFAULT_MARKETS)
    active_flags = parse_active_buckets(args.active)

    with MassiveClient(settings) as client:
        tcache = TickersCache(settings)
        tcache.warn_legacy_layout()
        df = tcache.refresh(
            client,
            markets=markets,
            active_flags=active_flags,
            force=args.force,
        )
        types_cache = TickerTypesCache(settings)
        types_df = types_cache.get(client, force_refresh=args.force)

    shards = tcache.list_shard_paths(markets=markets)
    console.print(
        f"[green]Tickers cache:[/green] {df.height} ligne(s) — "
        f"markets={markets} active={args.active}"
    )
    for p in shards:
        console.print(f"  [dim]→ {p}[/dim]")
    console.print(
        f"[green]Types cache:[/green]   {types_df.height} ligne(s) → {types_cache.parquet_path}"
    )
    if not df.is_empty():
        cols = [
            c
            for c in ("ticker", "name", "market", "type", "active", "primary_exchange")
            if c in df.columns
        ]
        _render_df(df.select(cols) if cols else df, settings, sort_col="ticker")
    return 0


def _cmd_tickers_types(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``myquantstore tickers types``."""
    from myquantstore.api.client import MassiveClient
    from myquantstore.tickers.cache import TickerTypesCache

    cache = TickerTypesCache(settings)
    if args.force or not cache.exists:
        if not settings.api_key:
            console.print("[red]Erreur:[/red] Aucune clé API. Exécutez 'myquantstore setup-key'.")
            return 1
        with MassiveClient(settings) as client:
            df = cache.get(client, force_refresh=args.force)
    else:
        df = cache.get(client=None, force_refresh=False)

    console.print(f"[bold]== Ticker types ({df.height}) ==[/bold]")
    _render_df(df, settings, sort_col="code")
    return 0


def _cmd_tickers_values(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``myquantstore tickers values`` — distincts des colonnes de filtre."""
    from myquantstore.tickers.search import DISTINCT_VALUE_COLUMNS, distinct_column_values

    if args.active and args.inactive:
        console.print("[red]Erreur:[/red] --active et --inactive sont mutuellement exclusifs.")
        return 1
    active: bool | None = None
    if args.active:
        active = True
    elif args.inactive:
        active = False

    markets = _resolve_markets_cli(args.markets, default=None)
    columns = tuple(args.column) if args.column else DISTINCT_VALUE_COLUMNS

    try:
        ensure_markets = markets
        if ensure_markets is None and not args.no_cascade:
            from myquantstore.tickers.cache import TickersCache

            if not TickersCache(settings).exists:
                ensure_markets = ["stocks"]
                active = True if active is None else active

        df = _ensure_tickers_cache(
            settings,
            no_cascade=args.no_cascade,
            markets=ensure_markets,
            active=active if ensure_markets else None,
        )
    except (FileNotFoundError, ValueError) as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1

    # Filtre market local si demandé (shards déjà lus)
    if markets and not df.is_empty() and "market" in df.columns:
        lowered = [m.lower() for m in markets]
        import polars as pl

        df = df.filter(pl.col("market").str.to_lowercase().is_in(lowered))
    if active is not None and not df.is_empty() and "active" in df.columns:
        import polars as pl

        df = df.filter(pl.col("active") == active)  # noqa: E712

    if df.is_empty():
        console.print("[yellow]Aucun ticker en cache pour ces filtres.[/yellow]")
        return 0

    distincts = distinct_column_values(df, columns)
    console.print(f"[bold]== Tickers values ({df.height} ligne(s) source) ==[/bold]")
    for col in columns:
        counts = distincts.get(col)
        if counts is None:
            console.print(f"\n[cyan]{col}[/cyan] : [dim]colonne absente[/dim]")
            continue
        console.print(f"\n[cyan]{col}[/cyan] ({counts.height} valeur(s) distincte(s))")
        _render_df(counts, settings, sort_col="count")
    return 0


def _ensure_tickers_cache(
    settings: Settings,
    *,
    no_cascade: bool,
    force: bool = False,
    markets: list[str] | None = None,
    active: bool | None = True,
) -> object:
    """Retourne un DataFrame tickers (cascade refresh shards si besoin)."""
    from myquantstore.api.client import MassiveClient
    from myquantstore.tickers.cache import DEFAULT_MARKETS, TickersCache

    cache = TickersCache(settings)
    mkts = markets if markets else list(DEFAULT_MARKETS)

    if no_cascade:
        return cache.read_concat(markets=markets, active=active)

    if settings.api_key:
        with MassiveClient(settings) as client:
            if markets is None and cache.exists and not force:
                # Lire tous les shards disque (frais ou non) sans fetch massif
                try:
                    return cache.read_concat(markets=None, active=active)
                except FileNotFoundError:
                    pass
            console.print("[yellow]Cache tickers — ensure shards…[/yellow]")
            return cache.ensure(
                client,
                markets=mkts,
                active=active,
                force=force,
                no_cascade=False,
            )

    return cache.read_concat(markets=markets, active=active)


def _cmd_search(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``myquantstore search``."""
    from myquantstore.tickers.cache import TickerTypesCache
    from myquantstore.tickers.search import join_ticker_types, search_tickers

    active: bool | None = None
    if args.active and args.inactive:
        console.print("[red]Erreur:[/red] --active et --inactive sont mutuellement exclusifs.")
        return 1
    if args.active:
        active = True
    elif args.inactive:
        active = False

    markets = _resolve_markets_cli(args.markets, default=None)

    try:
        # cascade: si markets précisés, assure ces shards ; sinon lit tout disque
        ensure_markets = markets if markets else None
        if ensure_markets is None and not args.no_cascade:
            # pas de market demandé → assure au moins stocks/active si rien sur disque
            from myquantstore.tickers.cache import TickersCache

            tc = TickersCache(settings)
            if not tc.exists:
                ensure_markets = ["stocks"]
                active = True if active is None else active

        df_all = _ensure_tickers_cache(
            settings,
            no_cascade=args.no_cascade,
            markets=ensure_markets,
            active=active if ensure_markets else None,
        )
    except (FileNotFoundError, ValueError) as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1

    # --limit : plafond d'affichage uniquement (comme display_max_rows)
    df = search_tickers(
        df_all,
        query=args.query,
        ticker=args.ticker,
        markets=markets,
        ticker_type=args.ticker_type,
        exchange=args.exchange,
        active=active,
    )

    # Join description des types (si cache types présent)
    types_cache = TickerTypesCache(settings)
    if types_cache.exists and not df.is_empty():
        try:
            df = join_ticker_types(df, types_cache.read())
        except FileNotFoundError:
            pass
    elif not types_cache.exists and not df.is_empty():
        console.print(
            "[dim]Cache types absent — pas de type_description "
            "(myquantstore tickers types / tickers refresh).[/dim]"
        )

    console.print(f"[bold]== Search : {df.height} résultat(s) ==[/bold]")
    cols = [
        c
        for c in (
            "ticker",
            "name",
            "market",
            "type",
            "type_description",
            "active",
            "primary_exchange",
            "currency_name",
        )
        if c in df.columns
    ]
    _render_df(
        df.select(cols) if cols and not df.is_empty() else df,
        settings,
        sort_col="ticker",
        max_rows=args.limit,
    )

    if args.output:
        out = Path(args.output)
        df.write_parquet(out)
        console.print(f"[green]Écrit :[/green] {out}")

    if not args.add:
        return 0

    return _add_search_results_to_conf(settings, df, yes=args.yes)


def _add_search_results_to_conf(settings: Settings, df: object, *, yes: bool) -> int:
    """Ajoute les résultats de search à config.toml avec garde-fous."""
    import polars as pl

    from myquantstore.config_io import add_instruments_to_config, resolve_writable_config_path
    from myquantstore.tickers.search import rows_for_config_add

    if not isinstance(df, pl.DataFrame) or df.is_empty():
        console.print("[red]Erreur:[/red] Aucun résultat à ajouter.")
        return 1

    items = rows_for_config_add(df)
    if not items:
        console.print(
            "[red]Erreur:[/red] Aucun ticker mappable vers un type MyQuantStore "
            "(crypto non supporté, market inconnu)."
        )
        return 1

    if len(items) > 1 and not yes:
        console.print(
            f"[yellow]{len(items)} tickers correspondent.[/yellow] "
            "Affinez les filtres ou passez --yes pour tout ajouter."
        )
        preview = ", ".join(f"{t.value}:{s}" for t, s in items[:20])
        console.print(f"[dim]{preview}{'…' if len(items) > 20 else ''}[/dim]")
        return 1

    path = resolve_writable_config_path()
    try:
        added = add_instruments_to_config(path, items)
    except (FileNotFoundError, ValueError) as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1

    total = sum(len(v) for v in added.values())
    if total == 0:
        console.print("[yellow]Rien à ajouter — tous déjà présents dans la conf.[/yellow]")
        return 0

    for key, syms in added.items():
        if syms:
            console.print(f"[green]Ajouté [{key}]:[/green] {', '.join(syms)}")
    console.print(f"[dim]Config : {path}[/dim]")
    return 0


def _cmd_config_add(settings: Settings, args: argparse.Namespace) -> int:
    """Commande ``myquantstore config add TICKER…``."""
    import polars as pl

    from myquantstore.config_io import add_instruments_to_config, resolve_writable_config_path
    from myquantstore.instruments import InstrumentType
    from myquantstore.tickers.search import rows_for_config_add, search_tickers, strip_api_prefix

    items: list[tuple[InstrumentType, str]] = []

    if args.type:
        # Type imposé : pas besoin du cache
        t = InstrumentType(args.type)
        for raw in args.tickers:
            items.append((t, strip_api_prefix(raw)))
    else:
        try:
            df_all = _ensure_tickers_cache(settings, no_cascade=args.no_cascade)
        except (FileNotFoundError, ValueError) as e:
            console.print(f"[red]Erreur:[/red] {e}")
            return 1

        missing: list[str] = []
        for raw in args.tickers:
            symbol = strip_api_prefix(raw)
            hit = search_tickers(df_all, ticker=symbol)
            if hit.is_empty():
                # Essai query exacte
                hit = search_tickers(df_all, query=symbol, limit=5)
                # garder égalité ticker uniquement
                if not hit.is_empty() and "ticker" in hit.columns:
                    hit = hit.filter(pl.col("ticker").str.to_uppercase() == symbol.upper())
            if hit.is_empty():
                missing.append(raw)
                continue
            mapped = rows_for_config_add(hit.head(1))
            if not mapped:
                market = hit["market"][0] if "market" in hit.columns else "?"
                console.print(
                    f"[yellow]Skip {raw}:[/yellow] market={market} non supporté pour la conf"
                )
                continue
            items.append(mapped[0])

        if missing:
            console.print(
                f"[red]Introuvable dans le cache tickers:[/red] {', '.join(missing)}. "
                "Vérifiez l'orthographe ou lancez 'myquantstore tickers refresh'."
            )
            if not items:
                return 1

    if not items:
        console.print("[yellow]Rien à ajouter.[/yellow]")
        return 1

    path = resolve_writable_config_path()
    try:
        added = add_instruments_to_config(path, items)
    except (FileNotFoundError, ValueError) as e:
        console.print(f"[red]Erreur:[/red] {e}")
        return 1

    total = sum(len(v) for v in added.values())
    if total == 0:
        console.print("[yellow]Rien à ajouter — tous déjà présents.[/yellow]")
        return 0
    for key, syms in added.items():
        if syms:
            console.print(f"[green]Ajouté [{key}]:[/green] {', '.join(syms)}")
    console.print(f"[dim]Config : {path}[/dim]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
