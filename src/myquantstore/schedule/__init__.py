"""Planification OS (systemd user timer + cron) pour les jobs périodiques."""

from __future__ import annotations

from myquantstore.schedule.common import (
    DEFAULT_CRON,
    DEFAULT_ON_CALENDAR,
    JOB_CACHES,
    JOB_FETCH,
    JOB_IDS,
    JOB_SPECS,
    JobSpec,
    detect_backend,
    get_job,
    resolve_binary,
)
from myquantstore.schedule.cron import (
    cron_status,
    install_cron,
    render_cron_block,
    render_cron_line,
    uninstall_cron,
)
from myquantstore.schedule.runner import run_cache_refresh_job, run_scheduled_job
from myquantstore.schedule.systemd import (
    install_systemd,
    render_service_unit,
    render_timer_unit,
    systemd_status,
    uninstall_systemd,
)


def describe_schedule_status() -> str:
    """Résumé court pour doctor / schedule status."""
    parts: list[str] = []
    for jid in JOB_IDS:
        sd = systemd_status(job=jid)
        if sd.get("installed"):
            parts.append(
                f"{jid} systemd: enabled={sd.get('enabled')} active={sd.get('active')} "
                f"next={sd.get('next') or '?'}"
            )
        cr = cron_status(job=jid)
        if cr.get("installed"):
            parts.append(f"{jid} cron: {cr.get('line') or 'installé'}")
    if not parts:
        return "aucun (myquantstore schedule install [fetch|caches])"
    return " | ".join(parts)


__all__ = [
    "DEFAULT_CRON",
    "DEFAULT_ON_CALENDAR",
    "JOB_CACHES",
    "JOB_FETCH",
    "JOB_IDS",
    "JOB_SPECS",
    "JobSpec",
    "detect_backend",
    "describe_schedule_status",
    "get_job",
    "install_cron",
    "install_systemd",
    "render_cron_block",
    "render_cron_line",
    "render_service_unit",
    "render_timer_unit",
    "resolve_binary",
    "run_cache_refresh_job",
    "run_scheduled_job",
    "cron_status",
    "systemd_status",
    "uninstall_cron",
    "uninstall_systemd",
]
