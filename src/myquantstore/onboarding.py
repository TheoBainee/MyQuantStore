"""Bootstrap et diagnostic install (init / doctor)."""

from __future__ import annotations

import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

from myquantstore.config import (
    Settings,
    get_user_config_dir,
    get_user_config_path,
    get_user_env_path,
    load_settings,
)
from myquantstore.resources import read_resource_text


def get_user_data_root() -> Path:
    """Racine données utilisateur (défaut XDG share)."""
    return Path.home() / ".local" / "share" / "myquantstore"


def write_api_key(
    api_key: str,
    *,
    base_url: str = "https://api.massive.com",
    env_path: Path | None = None,
    overwrite: bool = False,
) -> Path:
    """Écrit ``MASSIVE_API_KEY`` dans le ``.env`` XDG.

    :raises ValueError: clé vide ou fichier existant sans overwrite.
    """
    key = api_key.strip()
    if not key:
        raise ValueError("Clé API vide")
    path = env_path or get_user_env_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        content = path.read_text(encoding="utf-8")
        for line in content.splitlines():
            if line.startswith("MASSIVE_API_KEY=") and len(line) > len("MASSIVE_API_KEY="):
                raise FileExistsError(
                    f"Une clé API existe déjà dans {path}. Utilisez --yes pour écraser."
                )
    path.write_text(
        f"MASSIVE_API_KEY={key}\nMASSIVE_BASE_URL={base_url}\n",
        encoding="utf-8",
    )
    return path


def init_workspace(
    *,
    full: bool = False,
    force: bool = False,
    api_key: str | None = None,
    base_url: str = "https://api.massive.com",
    no_key: bool = False,
) -> dict[str, object]:
    """Crée dirs XDG + config.toml (+ .env optionnel).

    :return: résumé des actions (chemins, flags created/skipped).
    """
    config_dir = get_user_config_dir()
    config_path = get_user_config_path()
    data_root = get_user_data_root()
    dirs = [
        config_dir,
        data_root / "data",
        data_root / "cache",
        data_root / "logs",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

    template = "config.full.toml" if full else "config.minimal.toml"
    config_created = False
    config_skipped = False
    if config_path.exists() and not force:
        config_skipped = True
    else:
        config_path.write_text(read_resource_text(template), encoding="utf-8")
        config_created = True

    env_path = get_user_env_path()
    env_created = False
    env_skipped = False
    if api_key and not no_key:
        write_api_key(api_key, base_url=base_url, env_path=env_path, overwrite=force)
        env_created = True
    elif env_path.exists():
        env_skipped = True

    return {
        "config_dir": config_dir,
        "config_path": config_path,
        "config_created": config_created,
        "config_skipped": config_skipped,
        "template": template,
        "data_root": data_root,
        "env_path": env_path,
        "env_created": env_created,
        "env_skipped": env_skipped,
        "full": full,
    }


@dataclass
class DoctorCheck:
    name: str
    ok: bool
    detail: str
    blocking: bool = False


@dataclass
class DoctorReport:
    checks: list[DoctorCheck] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(c.blocking and not c.ok for c in self.checks)

    def add(
        self,
        name: str,
        ok: bool,
        detail: str,
        *,
        blocking: bool = False,
    ) -> None:
        self.checks.append(DoctorCheck(name, ok, detail, blocking=blocking))


def run_doctor(*, ping_api: bool = False) -> DoctorReport:
    """Diagnostics non destructifs de l'installation."""
    report = DoctorReport()

    py_ok = sys.version_info >= (3, 11)
    report.add(
        "python",
        py_ok,
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        blocking=True,
    )

    config_path = get_user_config_path()
    settings: Settings | None = None
    try:
        settings = load_settings()
        resolved = str(config_path if config_path.exists() else "fallback repo/cwd")
        report.add("config", True, f"OK ({resolved})", blocking=True)
    except FileNotFoundError:
        report.add(
            "config",
            False,
            f"introuvable — lancez `myquantstore init` ({config_path})",
            blocking=True,
        )
    except Exception as exc:
        report.add("config", False, f"invalide: {exc}", blocking=True)

    if settings is not None:
        for label, raw in (
            ("data_dir", settings.data_dir),
            ("cache_dir", settings.cache_dir),
            ("log_dir", settings.log_dir),
        ):
            path = Path(raw).expanduser()
            try:
                path.mkdir(parents=True, exist_ok=True)
                probe = path / ".myquantstore_write_probe"
                probe.write_text("ok", encoding="utf-8")
                probe.unlink(missing_ok=True)
                report.add(label, True, str(path.resolve()), blocking=True)
            except OSError as exc:
                report.add(label, False, f"{path}: {exc}", blocking=True)

        if settings.api_key:
            masked = f"{'*' * 8}{settings.api_key[-4:]}"
            report.add("api_key", True, f"présente ({masked})", blocking=False)
        else:
            report.add(
                "api_key",
                False,
                "absente — 1min Massive impossible ; 1day Yahoo OK. "
                "`myquantstore setup-key`",
                blocking=False,
            )

        n_inst = len(settings.all_instruments())
        report.add(
            "instruments",
            n_inst > 0,
            f"{n_inst} configuré(s)",
            blocking=n_inst == 0,
        )
    else:
        for label in ("data_dir", "cache_dir", "log_dir"):
            report.add(label, False, "skip (pas de config)", blocking=True)

    binary = shutil.which("myquantstore")
    if binary:
        report.add("binary", True, binary, blocking=False)
    else:
        report.add(
            "binary",
            False,
            "myquantstore absent du PATH (pipx/venv non activé ?)",
            blocking=False,
        )

    if ping_api and settings is not None and settings.api_key:
        try:
            import httpx

            url = settings.base_url.rstrip("/") + "/"
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(url)
            report.add(
                "api_ping",
                resp.status_code < 500,
                f"{url} → HTTP {resp.status_code}",
                blocking=False,
            )
        except Exception as exc:
            report.add("api_ping", False, str(exc), blocking=False)

    # Schedule status (best-effort, non bloquant)
    try:
        from myquantstore.schedule import describe_schedule_status

        sched = describe_schedule_status()
        report.add("schedule", True, sched, blocking=False)
    except Exception as exc:
        report.add("schedule", False, f"indisponible: {exc}", blocking=False)

    return report
