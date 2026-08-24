"""Serveur FastAPI pour ``myquantstore serve`` (API query réseau).

Endpoints v1 :

- ``GET /v1/health`` : fraîcheur OHLCV (200 OK / 503 si ``has_problems``).
- ``GET /v1/instruments`` : instruments de la config + résolutions d'agrégat.
- ``GET /v1/query`` : équivalent ``myquantstore query`` / ``query()``.

**Contraintes v1** : pas d'auth, pas de cascade, pas de fetch Massive/Yahoo.
Agrégat ou instrument absent → 404. Le chart (``chart/server.py``) ne change pas.
"""

from __future__ import annotations

from datetime import UTC, datetime, time
from io import BytesIO
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from myquantstore.chains import InstrumentChain, build_chain
from myquantstore.config import Settings
from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.logging_setup import get_logger
from myquantstore.query.reader import parse_query_datetime, query
from myquantstore.storage.aggregate_cache import aggregate_exists, list_aggregate_resolutions
from myquantstore.storage.coverage import InstrumentHealth, assess_instrument_health

logger = get_logger("serve.server")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8741

_PARQUET_MEDIA = "application/vnd.apache.parquet"
_ARROW_MEDIA = "application/vnd.apache.arrow.stream"
_VALID_TIMESCALE_UNITS = frozenset({"min", "hour", "day", "week"})
_VALID_TYPES = frozenset(t.value for t in InstrumentType)


def create_serve_app(settings: Settings) -> FastAPI:
    """Crée l'application FastAPI de l'API query (sans cascade)."""
    app = FastAPI(
        title="MyQuantStore Serve",
        description=("API HTTP pour query() — agrégats locaux uniquement, aucun fetch réseau."),
        docs_url="/docs",
    )

    @app.get("/v1/health")
    async def get_health(
        instrument: str | None = Query(None),
        type: str | None = Query(None, alias="type"),
    ) -> JSONResponse:
        try:
            instruments = _resolve_health_instruments(settings, instrument, type)
        except ValueError as exc:
            raise _http_for_resolve(exc) from exc

        today = datetime.now(UTC).date()
        payloads = [
            _health_payload(assess_instrument_health(inst, settings, today=today))
            for inst in instruments
        ]
        has_problems = any(item["has_problems"] for item in payloads)
        body: dict[str, Any] = {
            "ok": not has_problems,
            "has_problems": has_problems,
            "instruments": payloads,
        }
        return JSONResponse(content=body, status_code=503 if has_problems else 200)

    @app.get("/v1/instruments")
    async def get_instruments() -> dict[str, Any]:
        items = []
        for inst in settings.all_instruments():
            item: dict[str, Any] = {
                "key": inst.key,
                "type": inst.type.value,
                "symbol": inst.symbol,
                "resolutions": list_aggregate_resolutions(inst, settings),
            }
            if inst.type == InstrumentType.FUTURES:
                item.update(_futures_extras(inst, settings))
            items.append(item)
        return {"instruments": items}

    @app.get("/v1/query")
    async def get_query(
        request: Request,
        instrument: str = Query(...),
        type: str | None = Query(None, alias="type"),
        start: str | None = Query(None),
        end: str | None = Query(None),
        timescale_unit: str = Query("min"),
        timescale_nb: int = Query(1),
        adjust: bool = Query(False),
        no_split: bool = Query(False),
        dedup_timestamps: bool = Query(True),
        intraday_begin: str | None = Query(None),
        intraday_end: str | None = Query(None),
        timezone: str | None = Query(
            None, description="IANA TZ pour intraday (défaut [chart] timezone)"
        ),
        normalize_tick_size: bool = Query(False),
        include_cols: str | None = Query(None),
        forward_fill: bool = Query(False),
    ) -> Response:
        try:
            inst = _resolve_instrument(settings, instrument, type)
        except ValueError as exc:
            raise _http_for_resolve(exc) from exc

        if timescale_unit not in _VALID_TIMESCALE_UNITS:
            raise HTTPException(
                status_code=400,
                detail=(f"timescale_unit '{timescale_unit}' invalide (attendu: min|hour|day|week)"),
            )
        if timescale_nb < 1:
            raise HTTPException(status_code=400, detail="timescale_nb doit être >= 1")

        start_dt = _parse_datetime(start, "start")
        end_dt = _parse_datetime(end, "end", is_end=True)
        begin_t, end_t = _parse_intraday(intraday_begin, intraday_end)
        from myquantstore.cli import _parse_include_cols

        cols = _parse_include_cols(include_cols)

        from myquantstore.cli import _timescale_to_query_params

        try:
            resolution, k_minutes, k_days = _timescale_to_query_params(timescale_unit, timescale_nb)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if not aggregate_exists(inst, settings, resolution=resolution):
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Aucun agrégé {resolution} pour {inst.key}. "
                    "Exécutez 'myquantstore fetch' / 'myquantstore aggregate'."
                ),
            )

        chain = _local_chain(inst, settings)
        if normalize_tick_size and chain is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "normalize_tick_size requiert une RolloverChain locale "
                    "(cache contrats futures). Rafraîchissez le cache hors API."
                ),
            )

        try:
            df = query(
                inst,
                settings,
                chain,
                start=start_dt,
                end=end_dt,
                k_minutes=k_minutes,
                k_days=k_days,
                week_aligned=(timescale_unit == "week"),
                resolution=resolution,
                intraday_begin=begin_t,
                intraday_end=end_t,
                timezone=timezone or settings.chart_timezone or "UTC",
                adjust_rollover=adjust,
                normalize_tick_size=normalize_tick_size,
                no_split=no_split,
                dedup_timestamps=dedup_timestamps,
                include_cols=cols,
                forward_fill=forward_fill,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotImplementedError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        accept = (request.headers.get("accept") or "").lower()
        if _ARROW_MEDIA in accept:
            buffer = BytesIO()
            df.write_ipc(buffer)
            return Response(content=buffer.getvalue(), media_type=_ARROW_MEDIA)

        buffer = BytesIO()
        df.write_parquet(buffer)
        filename = f"{inst.symbol}_{resolution}.parquet"
        return Response(
            content=buffer.getvalue(),
            media_type=_PARQUET_MEDIA,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return app


def run_server(
    settings: Settings,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
) -> None:
    """Lance uvicorn (bloquant) — bind localhost par défaut."""
    import uvicorn

    app = create_serve_app(settings)
    logger.info(f"Serveur serve démarré sur http://{host}:{port}")
    logger.info("Endpoints: /v1/health /v1/instruments /v1/query — pas de cascade")
    uvicorn.run(app, host=host, port=port, log_level="warning")


def _resolve_instrument(settings: Settings, arg: str, type_override: str | None) -> Instrument:
    """Résout symbole / ``type:symbol`` comme la CLI (sans importer cli au load)."""
    from myquantstore.cli import _resolve_instrument_arg

    if type_override is not None and type_override not in _VALID_TYPES:
        raise ValueError(f"Type '{type_override}' invalide.")
    return _resolve_instrument_arg(settings, arg, type_override)


def _resolve_health_instruments(
    settings: Settings, arg: str | None, type_override: str | None
) -> list[Instrument]:
    from myquantstore.cli import _resolve_instruments

    if type_override is not None and type_override not in _VALID_TYPES:
        raise ValueError(f"Type '{type_override}' invalide.")
    return _resolve_instruments(settings, arg, type_override)


def _http_for_resolve(exc: ValueError) -> HTTPException:
    msg = str(exc)
    lowered = msg.lower()
    if "ambigu" in lowered:
        return HTTPException(status_code=400, detail=msg)
    if "invalide" in lowered:
        return HTTPException(status_code=400, detail=msg)
    return HTTPException(status_code=404, detail=msg)


def _parse_datetime(value: str | None, name: str, *, is_end: bool = False) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        return parse_query_datetime(value, is_end=is_end)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"{name} invalide : '{value}'. Attendu YYYY-MM-DD ou ISO datetime.",
        ) from None


def _parse_intraday(begin: str | None, end: str | None) -> tuple[time | None, time | None]:
    if (begin is None) != (end is None):
        raise HTTPException(
            status_code=400,
            detail="intraday_begin et intraday_end doivent être fournis ensemble.",
        )
    if begin is None or end is None:
        return None, None
    try:
        begin_t = time.fromisoformat(begin)
    except ValueError:
        raise HTTPException(
            status_code=400, detail=f"intraday_begin invalide : '{begin}'. Format: HH:MM."
        ) from None
    try:
        end_t = time.fromisoformat(end)
    except ValueError:
        raise HTTPException(
            status_code=400, detail=f"intraday_end invalide : '{end}'. Format: HH:MM."
        ) from None
    if begin_t == end_t:
        raise HTTPException(
            status_code=400,
            detail="intraday_begin et intraday_end doivent être différents.",
        )
    return begin_t, end_t


def _local_chain(instrument: Instrument, settings: Settings) -> InstrumentChain | None:
    """Construit une chaîne **sans réseau**. Cache contrats périmé = lecture locale."""
    if instrument.type == InstrumentType.FUTURES:
        from myquantstore.contracts.cache import ContractsCache
        from myquantstore.storage.parquet_io import read_parquet

        cache = ContractsCache(instrument.symbol, settings)
        if not cache.exists:
            logger.debug(f"Pas de cache contrats local pour {instrument.key}")
            return None
        try:
            contracts_df = read_parquet(cache.parquet_path)
        except FileNotFoundError:
            return None
        try:
            return build_chain(
                instrument,
                contracts_df=contracts_df,
                days_before_expiry=settings.days_before_expiry,
            )
        except Exception as exc:
            logger.warning(f"Chaîne locale {instrument.key} échouée: {exc}")
            return None
    try:
        return build_chain(instrument)
    except Exception as exc:
        logger.warning(f"Chaîne locale {instrument.key} échouée: {exc}")
        return None


def _health_payload(health: InstrumentHealth) -> dict[str, Any]:
    coverages: dict[str, Any] = {}
    for res, cov in health.coverages.items():
        coverages[res] = {
            "present": cov.present,
            "rows": cov.rows,
            "min_date": cov.min_date.isoformat() if cov.min_date else None,
            "max_date": cov.max_date.isoformat() if cov.max_date else None,
            "lag_days": cov.lag_days,
            "stale": False,
        }
    # stale recalculé côté assess via issues ; exposer aussi le flag de couverture
    # (lag > seuil) sans réimporter Settings ici — dérivé des issues STALE.
    stale_resolutions = {i.resolution for i in health.issues if i.code == "stale" and i.resolution}
    for res, item in coverages.items():
        item["stale"] = res in stale_resolutions

    return {
        "instrument": health.instrument_key,
        "level": health.worst_level.value,
        "has_problems": health.has_problems,
        "coverages": coverages,
        "issues": [
            {
                "level": issue.level.value,
                "code": issue.code,
                "message": issue.message,
                "resolution": issue.resolution,
            }
            for issue in health.issues
        ],
    }


def _futures_extras(instrument: Instrument, settings: Settings) -> dict[str, Any]:
    """Infos futures depuis le cache contrats + agrégé local (aucun appel API)."""
    from datetime import UTC, datetime

    extras: dict[str, Any] = {
        "trade_tick_size": None,
        "tickers": [],
        "current_ticker": None,
        "last_trade_date": None,
        "days_to_maturity": None,
    }
    chain = _local_chain(instrument, settings)
    today = datetime.now(UTC).date()
    if chain is not None:
        current = chain.active_contract(today)
        extras["current_ticker"] = current
        if current:
            extras["trade_tick_size"] = chain.tick_size_for_ticker(current)
            seg = chain.segment_for_ticker(current)
            if seg is not None:
                extras["last_trade_date"] = seg.last_trade_date.isoformat()
                extras["days_to_maturity"] = (seg.last_trade_date - today).days

    tickers: set[str] = set()
    for res in list_aggregate_resolutions(instrument, settings):
        try:
            from myquantstore.storage.aggregate_cache import read_aggregate

            df = read_aggregate(instrument, settings, resolution=res)
        except FileNotFoundError:
            continue
        if "ticker" in df.columns and df.height:
            tickers.update(str(t) for t in df["ticker"].unique().to_list())
    extras["tickers"] = sorted(tickers)
    return extras
