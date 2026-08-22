"""Tests init / doctor / setup-key / schedule."""

from __future__ import annotations

from pathlib import Path

import pytest

from myquantstore.cli import main
from myquantstore.onboarding import init_workspace, run_doctor, write_api_key
from myquantstore.resources import read_resource_text
from myquantstore.schedule.common import (
    DEFAULT_CRON,
    DEFAULT_ON_CALENDAR,
    JOB_CACHES,
    JOB_FETCH,
    get_job,
    resolve_binary,
)
from myquantstore.schedule.cron import (
    merge_crontab,
    render_cron_block,
    strip_job_block,
    strip_myquantstore_block,
)
from myquantstore.schedule.runner import run_cache_refresh_job, run_scheduled_job
from myquantstore.schedule.systemd import render_service_unit, render_timer_unit


class TestResources:
    def test_minimal_and_full_readable(self):
        mini = read_resource_text("config.minimal.toml")
        full = read_resource_text("config.full.toml")
        assert "AAPL" in mini
        assert "[instruments]" in full
        assert "ES" in full


class TestWriteApiKey:
    def test_write_and_no_overwrite(self, tmp_path: Path):
        env = tmp_path / ".env"
        write_api_key("secret1234", env_path=env)
        assert "MASSIVE_API_KEY=secret1234" in env.read_text(encoding="utf-8")
        with pytest.raises(FileExistsError):
            write_api_key("other", env_path=env, overwrite=False)
        write_api_key("other9999", env_path=env, overwrite=True)
        assert "other9999" in env.read_text(encoding="utf-8")


class TestInit:
    def test_init_creates_minimal_config(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        xdg = tmp_path / "xdg"
        data = tmp_path / "data_root"
        monkeypatch.setattr("myquantstore.config.get_user_config_dir", lambda: xdg)
        monkeypatch.setattr("myquantstore.onboarding.get_user_config_dir", lambda: xdg)
        monkeypatch.setattr(
            "myquantstore.onboarding.get_user_config_path", lambda: xdg / "config.toml"
        )
        monkeypatch.setattr("myquantstore.onboarding.get_user_env_path", lambda: xdg / ".env")
        monkeypatch.setattr("myquantstore.onboarding.get_user_data_root", lambda: data)

        summary = init_workspace(full=False, force=False, no_key=True)
        assert summary["config_created"] is True
        cfg = (xdg / "config.toml").read_text(encoding="utf-8")
        assert "AAPL" in cfg
        assert (data / "data").is_dir()
        assert (data / "cache").is_dir()
        assert (data / "logs").is_dir()

        summary2 = init_workspace(full=False, force=False, no_key=True)
        assert summary2["config_skipped"] is True

    def test_init_cli(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys):
        xdg = tmp_path / "xdg"
        data = tmp_path / "share"
        monkeypatch.setattr("myquantstore.config.get_user_config_dir", lambda: xdg)
        monkeypatch.setattr("myquantstore.onboarding.get_user_config_dir", lambda: xdg)
        monkeypatch.setattr(
            "myquantstore.onboarding.get_user_config_path", lambda: xdg / "config.toml"
        )
        monkeypatch.setattr("myquantstore.onboarding.get_user_env_path", lambda: xdg / ".env")
        monkeypatch.setattr("myquantstore.onboarding.get_user_data_root", lambda: data)
        monkeypatch.setattr("myquantstore.cli.get_user_config_path", lambda: xdg / "config.toml")
        monkeypatch.setattr("myquantstore.cli.get_user_env_path", lambda: xdg / ".env")
        monkeypatch.setattr("sys.stdin.isatty", lambda: False)

        rc = main(["init", "--no-key"])
        assert rc == 0
        assert (xdg / "config.toml").exists()
        out = capsys.readouterr().out
        assert "init" in out.lower() or "config.toml" in out


class TestSetupKeyCli:
    def test_setup_key_noninteractive(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        xdg = tmp_path / "xdg"
        xdg.mkdir()
        monkeypatch.setattr("myquantstore.cli.get_user_env_path", lambda: xdg / ".env")
        monkeypatch.setattr(
            "myquantstore.onboarding.get_user_env_path", lambda: xdg / ".env"
        )
        rc = main(["setup-key", "--api-key", "abcd1234ef", "--yes"])
        assert rc == 0
        assert "abcd1234ef" in (xdg / ".env").read_text(encoding="utf-8")


class TestDoctor:
    def test_doctor_fails_without_config(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        xdg = tmp_path / "empty_xdg"
        xdg.mkdir()
        monkeypatch.setattr("myquantstore.config.get_user_config_dir", lambda: xdg)
        monkeypatch.setattr(
            "myquantstore.config.get_user_config_path", lambda: xdg / "config.toml"
        )
        monkeypatch.setattr(
            "myquantstore.onboarding.get_user_config_path", lambda: xdg / "config.toml"
        )
        monkeypatch.chdir(tmp_path)  # pas de config.toml cwd
        report = run_doctor()
        assert report.ok is False
        assert any(c.name == "config" and not c.ok for c in report.checks)

    def test_doctor_ok_after_init(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        xdg = tmp_path / "xdg"
        data = tmp_path / "share"
        monkeypatch.setattr("myquantstore.config.get_user_config_dir", lambda: xdg)
        monkeypatch.setattr(
            "myquantstore.config.get_user_config_path", lambda: xdg / "config.toml"
        )
        monkeypatch.setattr("myquantstore.config.get_user_env_path", lambda: xdg / ".env")
        monkeypatch.setattr("myquantstore.onboarding.get_user_config_dir", lambda: xdg)
        monkeypatch.setattr(
            "myquantstore.onboarding.get_user_config_path", lambda: xdg / "config.toml"
        )
        monkeypatch.setattr("myquantstore.onboarding.get_user_env_path", lambda: xdg / ".env")
        monkeypatch.setattr("myquantstore.onboarding.get_user_data_root", lambda: data)
        monkeypatch.chdir(tmp_path)

        init_workspace(no_key=True, force=True)
        # Pointer storage vers tmp pour doctor write probes via load_settings
        cfg = xdg / "config.toml"
        text = cfg.read_text(encoding="utf-8")
        text = text.replace(
            'data_dir = "~/.local/share/myquantstore/data"',
            f'data_dir = "{data / "data"}"',
        )
        text = text.replace(
            'cache_dir = "~/.local/share/myquantstore/cache"',
            f'cache_dir = "{data / "cache"}"',
        )
        text = text.replace(
            'log_dir = "~/.local/share/myquantstore/logs"',
            f'log_dir = "{data / "logs"}"',
        )
        cfg.write_text(text, encoding="utf-8")

        report = run_doctor()
        assert report.ok is True


class TestScheduleRender:
    def test_systemd_units_contain_binary_and_calendar(self):
        svc = render_service_unit(binary="/usr/bin/myquantstore", fetch_args="--no-cascade")
        assert "ExecStart=/usr/bin/myquantstore schedule run" in svc
        assert "--fetch-args" in svc
        assert "--no-cascade" in svc
        timer = render_timer_unit(on_calendar=DEFAULT_ON_CALENDAR)
        assert "OnCalendar=Sat *-*-* 07:00:00" in timer
        assert "Persistent=true" in timer
        assert "Unit=myquantstore-fetch.service" in timer

    def test_caches_systemd_units(self):
        spec = get_job(JOB_CACHES)
        svc = render_service_unit(job=spec, binary="/usr/bin/myquantstore")
        assert "ExecStart=/usr/bin/myquantstore schedule run caches" in svc
        assert "--fetch-args" not in svc
        timer = render_timer_unit(job=spec)
        assert "OnCalendar=Sat *-*-* 03:00:00" in timer
        assert "Unit=myquantstore-caches.service" in timer

    def test_cron_block_roundtrip(self):
        block = render_cron_block(
            schedule=DEFAULT_CRON,
            binary="/opt/myquantstore",
            fetch_args="",
        )
        assert "0 7 * * 6" in block
        assert "BEGIN MYQUANTSTORE" in block
        assert "schedule run" in block
        merged = merge_crontab("# keep\n", block)
        assert "# keep" in merged
        merged2 = merge_crontab(merged, render_cron_block(schedule="0 2 * * 0"))
        assert merged2.count("# BEGIN MYQUANTSTORE\n") == 1
        assert "0 2 * * 0" in merged2
        cleaned = strip_myquantstore_block(merged2)
        assert "# BEGIN MYQUANTSTORE\n" not in cleaned
        assert "# keep" in cleaned

    def test_cron_fetch_and_caches_coexist(self):
        fetch = render_cron_block(job=JOB_FETCH, binary="/opt/mqs")
        caches = render_cron_block(job=JOB_CACHES, binary="/opt/mqs")
        merged = merge_crontab(merge_crontab("", fetch, job=JOB_FETCH), caches, job=JOB_CACHES)
        assert "# BEGIN MYQUANTSTORE\n" in merged
        assert "# BEGIN MYQUANTSTORE-CACHES\n" in merged
        assert "schedule run caches" in merged
        assert "0 7 * * 6" in merged
        assert "0 3 * * 6" in merged
        fetch2 = render_cron_block(job=JOB_FETCH, schedule="0 8 * * 6", binary="/opt/mqs")
        rem = merge_crontab(merged, fetch2, job=JOB_FETCH)
        assert rem.count("# BEGIN MYQUANTSTORE\n") == 1
        assert "# BEGIN MYQUANTSTORE-CACHES\n" in rem
        assert "0 8 * * 6" in rem
        assert "0 3 * * 6" in rem
        only_caches = strip_job_block(rem, JOB_FETCH)
        assert "# BEGIN MYQUANTSTORE\n" not in only_caches
        assert "# BEGIN MYQUANTSTORE-CACHES\n" in only_caches

    def test_schedule_show_cli(self, capsys):
        rc = main(["schedule", "show", "--backend", "systemd"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "myquantstore-fetch.service" in out or "OnCalendar" in out

    def test_schedule_show_caches_cli(self, capsys):
        rc = main(["schedule", "show", "caches", "--backend", "systemd"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "myquantstore-caches" in out
        assert "schedule run caches" in out
        assert "03:00:00" in out

    def test_schedule_status_lists_both_jobs(self, capsys):
        rc = main(["schedule", "status"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "fetch" in out
        assert "caches" in out

    def test_resolve_binary_nonempty(self):
        assert resolve_binary()


class TestScheduleRunner:
    def test_run_fetch_aggregate_status_order(self):
        calls: list[list[str]] = []

        def fake_main(argv: list[str] | None = None) -> int:
            assert argv is not None
            calls.append(list(argv))
            return 0

        rc = run_scheduled_job(fetch_args="--type stocks", main_fn=fake_main)
        assert rc == 0
        assert calls[0][:1] == ["fetch"]
        assert "--type" in calls[0] and "stocks" in calls[0]
        assert calls[1][:1] == ["aggregate"]
        assert calls[2] == ["status", "--check"]

    def test_run_stops_on_fetch_failure(self):
        def fake_main(argv: list[str] | None = None) -> int:
            assert argv is not None
            if argv[0] == "fetch":
                return 7
            return 0

        assert run_scheduled_job(main_fn=fake_main) == 7

    def test_run_skip_aggregate(self):
        calls: list[str] = []

        def fake_main(argv: list[str] | None = None) -> int:
            assert argv is not None
            calls.append(argv[0])
            return 0

        run_scheduled_job(skip_aggregate=True, main_fn=fake_main)
        assert calls == ["fetch", "status"]

    def test_run_caches_tickers_then_contracts(self):
        calls: list[list[str]] = []

        def fake_main(argv: list[str] | None = None) -> int:
            assert argv is not None
            calls.append(list(argv))
            return 0

        assert run_cache_refresh_job(main_fn=fake_main) == 0
        assert calls[0] == ["tickers", "refresh", "--markets", "all", "--force"]
        assert calls[1] == ["futures", "contracts", "--refresh"]

    def test_run_caches_stops_on_tickers_failure(self):
        def fake_main(argv: list[str] | None = None) -> int:
            assert argv is not None
            if argv[0] == "tickers":
                return 3
            return 0

        assert run_cache_refresh_job(main_fn=fake_main) == 3
