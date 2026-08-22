"""Jobs one-shot du schedule : fetch (OHLCV) et caches (Massive)."""

from __future__ import annotations

import shlex
from collections.abc import Callable


def run_scheduled_job(
    *,
    fetch_args: str = "",
    aggregate_args: str = "",
    skip_aggregate: bool = False,
    skip_status: bool = False,
    main_fn: Callable[[list[str] | None], int] | None = None,
) -> int:
    """Enchaîne fetch, aggregate (cache), status --check.

    Exit ≠ 0 si une étape échoue (status --check inclus).
    """
    invoke = main_fn if main_fn is not None else _cli_main()

    fetch_argv = ["fetch", *shlex.split(fetch_args)]
    rc_fetch = int(invoke(fetch_argv))
    if rc_fetch != 0:
        return rc_fetch

    if not skip_aggregate:
        agg_argv = ["aggregate", *shlex.split(aggregate_args)]
        if "--timeframe" in fetch_argv and "--timeframe" not in agg_argv:
            try:
                idx = fetch_argv.index("--timeframe")
                agg_argv.extend(["--timeframe", fetch_argv[idx + 1]])
            except (ValueError, IndexError):
                pass
        rc_agg = int(invoke(agg_argv))
        if rc_agg != 0:
            return rc_agg

    if not skip_status:
        rc_status = int(invoke(["status", "--check", "--strict-missing"]))
        if rc_status != 0:
            return rc_status

    return 0


def run_cache_refresh_job(
    *,
    main_fn: Callable[[list[str] | None], int] | None = None,
) -> int:
    """Enchaîne tickers refresh --markets all --force puis futures contracts --refresh."""
    invoke = main_fn if main_fn is not None else _cli_main()

    rc_tickers = int(invoke(["tickers", "refresh", "--markets", "all", "--force"]))
    if rc_tickers != 0:
        return rc_tickers

    rc_contracts = int(invoke(["futures", "contracts", "--refresh"]))
    if rc_contracts != 0:
        return rc_contracts

    return 0


def _cli_main() -> Callable[[list[str] | None], int]:
    from myquantstore.cli import main

    return main
