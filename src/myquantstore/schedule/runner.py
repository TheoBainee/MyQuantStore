"""Job one-shot du schedule : fetch → aggregate → status --check."""

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
    if main_fn is None:
        from myquantstore.cli import main as main_fn  # type: ignore[no-redef]

    fetch_argv = ["fetch", *shlex.split(fetch_args)]
    rc_fetch = int(main_fn(fetch_argv))
    if rc_fetch != 0:
        return rc_fetch

    if not skip_aggregate:
        agg_argv = ["aggregate", *shlex.split(aggregate_args)]
        # Align timeframe with fetch if present in fetch_args
        if "--timeframe" in fetch_argv and "--timeframe" not in agg_argv:
            try:
                idx = fetch_argv.index("--timeframe")
                agg_argv.extend(["--timeframe", fetch_argv[idx + 1]])
            except (ValueError, IndexError):
                pass
        rc_agg = int(main_fn(agg_argv))
        if rc_agg != 0:
            return rc_agg

    if not skip_status:
        rc_status = int(main_fn(["status", "--check"]))
        if rc_status != 0:
            return rc_status

    return 0
