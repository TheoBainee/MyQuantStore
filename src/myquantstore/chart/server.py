"""Serveur web FastAPI pour la visualisation interactive des chandeliers.

Backend de la commande ``myquantstore chart``. Expose :

- ``GET /`` : dashboard multi-instruments (groupes, miniatures SVG).
- ``GET /{instrument_key}`` : page HTML du chart (template unique).
- ``GET /static/{file}`` : fichiers statiques (lightweight-charts JS, apache-arrow JS).
- ``GET /api/candles`` : chandeliers OHLCV en Arrow IPC (binaire).
- ``GET /api/meta`` : métadonnées JSON (tick_size, date range).
- ``GET /api/thumbnail/{instrument_key}.svg`` : sparkline SVG (1day).

**Multi-type** : les instruments sont indexés par leur clé ``"{type}:{symbol}"``
(ex: ``futures:ES``, ``stocks:AAPL``) pour éviter les collisions de symboles
entre types. Le paramètre ``product`` des endpoints API = cette clé.

**Buffer progressif** : le frontend charge initialement
``buffer_multiplier × max_visible_candles`` chandeliers (les plus récents), puis
fetch des chunks plus anciens au fil du pan vers la gauche (lazy loading).

**Format de transfert** : Arrow IPC (binaire). Polars ``write_ipc()`` côté
serveur, ``apache-arrow`` JS côté frontend.

**License TradingView** : Lightweight Charts est sous Apache-2.0 avec
attribution requise (voir fichier ``NOTICE`` dans ce module).
"""

from __future__ import annotations

import json
from datetime import datetime, time
from io import BytesIO
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

from myquantstore.analytics.portfolio_service import (
    PORTFOLIO_PRODUCTS,
    PortfolioService,
    is_portfolio_product,
)
from myquantstore.analytics.synthetic import build_portfolio_ohlcv
from myquantstore.chains import InstrumentChain
from myquantstore.chart.overlay import list_overlays, load_overlay
from myquantstore.chart.thumbnails import (
    build_dashboard_cards,
    get_thumbnail_svg,
    render_sparkline_svg,
)
from myquantstore.config import Settings
from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.logging_setup import get_logger
from myquantstore.query.reader import query

logger = get_logger("chart.server")

_STATIC_DIR = Path(__file__).parent / "static"


def create_chart_app(
    settings: Settings,
    instruments: dict[str, Instrument],
    chains: dict[str, InstrumentChain],
    defaults: ChartDefaults,
    portfolio_service: PortfolioService | None = None,
) -> FastAPI:
    """Crée l'application FastAPI pour le serveur de visualisation.

    :param settings: Configuration globale.
    :param instruments: Dictionnaire {instrument_key: Instrument} servis.
    :param chains: Dictionnaire {instrument_key: InstrumentChain}.
    :param defaults: Paramètres par défaut injectés dans le frontend.
    :param portfolio_service: Service lazy optim paniers (optionnel).
    :return: Application FastAPI prête à lancer avec uvicorn.
    """
    app = FastAPI(title="MyQuantStore Chart", docs_url="/docs")
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    if portfolio_service is None:
        stock_insts = [i for i in instruments.values() if i.type == InstrumentType.STOCKS]
        if not stock_insts:
            stock_insts = settings.instruments_of_type(InstrumentType.STOCKS)
        portfolio_service = PortfolioService(settings, stock_insts)

    enable_portfolio = bool(
        any(i.type == InstrumentType.STOCKS for i in instruments.values())
        or settings.instruments_of_type(InstrumentType.STOCKS)
    )

    def _known_product(key: str) -> bool:
        return key in instruments or (enable_portfolio and is_portfolio_product(key))

    def _parse_before(before: str | None) -> datetime | None:
        if not before:
            return None
        try:
            parsed = datetime.fromisoformat(before.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                from datetime import UTC

                parsed = parsed.replace(tzinfo=UTC)
            return parsed
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Format 'before' invalide: {before}") from None

    def _query_portfolio_df(
        product: str,
        *,
        timescale_unit: str,
        timescale_nb: int,
        before_dt: datetime | None,
        after_dt: datetime | None = None,
    ) -> pl.DataFrame:
        assert portfolio_service is not None
        try:
            weights = portfolio_service.weights_for(product)
        except Exception as exc:
            logger.error(f"Optim portfolio {product}: {exc}")
            raise HTTPException(status_code=500, detail=f"Optim portfolio échouée: {exc}") from exc

        resolution, k_minutes, k_days = _timescale_to_params(timescale_unit, timescale_nb)
        # Combo sur barre de base, puis resample (invariant)
        base_res = "1min" if resolution == "1min" else "1day"
        try:
            return build_portfolio_ohlcv(
                weights,
                settings,
                resolution=base_res,
                start=after_dt.replace(tzinfo=None) if after_dt and after_dt.tzinfo else after_dt,
                end=before_dt.replace(tzinfo=None) if before_dt and before_dt.tzinfo else before_dt,
                k_minutes=k_minutes if base_res == "1min" else 1,
                k_days=k_days if base_res == "1day" else 1,
                week_aligned=(timescale_unit == "week"),
                adjust_dividends=defaults.adjust_rollover,
                no_split=defaults.no_split,
                rebase=100.0,
            )
        except Exception as exc:
            logger.error(f"Synthetic {product}: {exc}")
            raise HTTPException(status_code=500, detail=f"Série panier: {exc}") from exc

    @app.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        html = _render_dashboard_html(
            instruments, settings, defaults, enable_portfolio=enable_portfolio
        )
        return HTMLResponse(content=html)

    @app.get("/api/thumbnail/{instrument_key:path}")
    async def get_thumbnail(
        instrument_key: str,
        lookback_days: int | None = Query(None, ge=1, le=3650),
    ) -> Response:
        key = instrument_key.removesuffix(".svg")
        days = lookback_days if lookback_days is not None else settings.thumbnail_lookback_days
        if is_portfolio_product(key) and enable_portfolio:
            svg = _portfolio_thumbnail_svg(key, portfolio_service, settings, days)
            return Response(
                content=svg,
                media_type="image/svg+xml",
                headers={"Cache-Control": "public, max-age=60"},
            )
        if key not in instruments:
            raise HTTPException(status_code=404, detail=f"Instrument '{key}' non configuré")
        svg = get_thumbnail_svg(instruments[key], settings, lookback_days=days)
        return Response(
            content=svg,
            media_type="image/svg+xml",
            headers={"Cache-Control": "public, max-age=60"},
        )

    @app.get("/api/candles")
    async def get_candles(
        product: str = Query(..., description="Clé instrument (ex: futures:ES)"),
        timescale_unit: str = Query("min", description="Unité de l'UT: min|hour|day|week"),
        timescale_nb: int = Query(1, ge=1, description="Nombre d'unités"),
        limit: int = Query(
            settings.max_visible_candles * settings.buffer_multiplier,
            ge=1,
            description="Nombre max de chandeliers à retourner",
        ),
        before: str | None = Query(None, description="Chandeliers avant cette date (ISO 8601)"),
        after: str | None = Query(None, description="Chandeliers après cette date (ISO 8601)"),
    ) -> Response:
        if not _known_product(product):
            raise HTTPException(status_code=404, detail=f"Instrument '{product}' non configuré")

        before_dt = _parse_before(before)
        after_dt = _parse_before(after)
        resolution, k_minutes, k_days = _timescale_to_params(timescale_unit, timescale_nb)

        if is_portfolio_product(product):
            df = _query_portfolio_df(
                product,
                timescale_unit=timescale_unit,
                timescale_nb=timescale_nb,
                before_dt=before_dt,
                after_dt=after_dt,
            )
        else:
            instrument = instruments[product]
            chain = chains.get(product)
            resampled = (resolution == "1day" and k_days > 1) or (
                resolution != "1day" and k_minutes > 1
            )
            wanted = ["window_start", "open", "high", "low", "close", "volume"]
            # candle_count après resample, ou pour marquer les barres synthétiques (--forward-fill)
            if resampled or defaults.forward_fill:
                wanted.append("candle_count")
            df = _query_chart_ohlcv(
                instrument,
                settings,
                chain,
                before_dt=before_dt,
                after_dt=after_dt,
                k_minutes=k_minutes,
                k_days=k_days,
                week_aligned=(timescale_unit == "week"),
                resolution=resolution,
                defaults=defaults,
                include_cols=wanted,
            )

        if df.is_empty():
            return Response(content=b"", media_type="application/octet-stream")

        if limit is not None and limit > 0:
            df = df.tail(limit)

        chart_df = _prepare_chart_df(df)
        buffer = BytesIO()
        chart_df.write_ipc(buffer)
        logger.debug(
            f"API /candles: product={product} res={resolution} k_min={k_minutes} "
            f"k_days={k_days} limit={limit} before={before} after={after} -> {chart_df.height} candles, "
            f"{len(buffer.getvalue())} bytes"
        )
        return Response(content=buffer.getvalue(), media_type="application/octet-stream")

    @app.get("/api/meta")
    async def get_meta(product: str = Query(...)) -> dict[str, Any]:
        if not _known_product(product):
            raise HTTPException(status_code=404, detail=f"Instrument '{product}' non configuré")

        if is_portfolio_product(product):
            try:
                result = portfolio_service.get_result(product)
                return {
                    "product": product,
                    "tick_size": None,
                    "first_date": None,
                    "last_date": None,
                    "portfolio": True,
                    "objective": result.objective,
                    "mean_ann": result.mean_ann,
                    "vol_ann": result.vol_ann,
                    "sharpe": result.sharpe,
                    "n_legs": sum(1 for w in result.weights.values() if w > 1e-4),
                }
            except Exception as exc:
                raise HTTPException(status_code=500, detail=str(exc)) from exc

        instrument = instruments[product]
        chain = chains.get(product)

        tick_size: float | None = None
        if chain is not None and instrument.type == InstrumentType.FUTURES:
            from datetime import UTC
            from datetime import datetime as _dt

            active_ticker = chain.active_contract(_dt.now(UTC).date())
            if active_ticker:
                tick_size = chain.tick_size_for_ticker(active_ticker)

        from myquantstore.storage.aggregate_cache import read_aggregate

        try:
            df = read_aggregate(instrument, settings, resolution="1min")
        except FileNotFoundError:
            try:
                df = read_aggregate(instrument, settings, resolution="1day")
            except FileNotFoundError:
                return {
                    "product": product,
                    "tick_size": tick_size,
                    "first_date": None,
                    "last_date": None,
                }

        if df.is_empty():
            return {"product": product, "tick_size": tick_size, "first_date": None, "last_date": None}

        from datetime import datetime as _dt2

        ws_min = df["window_start"].min()
        ws_max = df["window_start"].max()
        first_date = ws_min.isoformat() if isinstance(ws_min, _dt2) else None
        last_date = ws_max.isoformat() if isinstance(ws_max, _dt2) else None

        return {
            "product": product,
            "tick_size": tick_size,
            "first_date": first_date,
            "last_date": last_date,
            "total_candles": df.height,
        }

    @app.get("/api/overlays")
    async def get_overlays(product: str = Query(...)) -> list[dict[str, Any]]:
        return list_overlays(settings.overlay_dir, product)

    @app.get("/api/overlay/{stem}")
    async def get_overlay(
        stem: str,
        id: str | None = Query(None, alias="id"),
    ) -> dict[str, Any]:
        try:
            return load_overlay(settings.overlay_dir, stem, id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"backtest_id inconnu: {exc}") from exc

    @app.get("/{instrument_key}", response_class=HTMLResponse)
    async def chart_page(instrument_key: str) -> HTMLResponse:
        if not _known_product(instrument_key):
            raise HTTPException(
                status_code=404, detail=f"Instrument '{instrument_key}' non configuré"
            )
        html = _render_chart_html(instrument_key, defaults)
        return HTMLResponse(content=html)

    return app


def _portfolio_thumbnail_svg(
    product: str,
    service: PortfolioService,
    settings: Settings,
    lookback_days: int,
) -> str:
    """Sparkline 1day du panier (lazy optim + synthetic)."""
    from datetime import UTC, timedelta

    try:
        weights = service.weights_for(product)
        end = datetime.now(UTC).replace(tzinfo=None)
        start = end - timedelta(days=lookback_days)
        df = build_portfolio_ohlcv(
            weights,
            settings,
            resolution="1day",
            start=start,
            end=end,
            rebase=100.0,
        )
        closes = [float(c) for c in df["close"].to_list()]
        perf = None
        if len(closes) >= 2 and closes[0] != 0:
            perf = (closes[-1] - closes[0]) / abs(closes[0]) * 100.0
        return render_sparkline_svg(tuple(closes), performance_pct=perf)
    except Exception as exc:
        logger.warning(f"Thumbnail portfolio {product}: {exc}")
        return render_sparkline_svg(())


class ChartDefaults:
    """Paramètres par défaut injectés dans le frontend (page HTML)."""

    def __init__(
        self,
        default_product: str,
        timescale_unit: str = "min",
        timescale_nb: int = 1,
        nb_candle: int = 50000,
        max_visible_candles: int = 50000,
        buffer_multiplier: int = 3,
        fetch_chunk_size: int = 50000,
        intraday_begin: time | None = None,
        intraday_end: time | None = None,
        normalize_tick_size: bool = False,
        adjust_rollover: bool = False,
        no_split: bool = False,
        forward_fill: bool = False,
        thumbnail_lookback_days: int = 90,
        candle_up: str = "#26a69a",
        candle_down: str = "#ef5350",
        tx_buy: str = "#2196F3",
        tx_sell: str = "#FF9800",
        order_buy: str = "#2196F3",
        order_sell: str = "#FF9800",
        timezone: str = "UTC",
    ) -> None:
        self.default_product = default_product
        self.timescale_unit = timescale_unit
        self.timescale_nb = timescale_nb
        self.nb_candle = nb_candle
        self.max_visible_candles = max_visible_candles
        self.buffer_multiplier = buffer_multiplier
        self.fetch_chunk_size = fetch_chunk_size
        self.intraday_begin = intraday_begin
        self.intraday_end = intraday_end
        self.normalize_tick_size = normalize_tick_size
        self.adjust_rollover = adjust_rollover
        self.no_split = no_split
        self.forward_fill = forward_fill
        self.thumbnail_lookback_days = thumbnail_lookback_days
        self.candle_up = candle_up
        self.candle_down = candle_down
        self.tx_buy = tx_buy
        self.tx_sell = tx_sell
        self.order_buy = order_buy
        self.order_sell = order_sell
        self.timezone = timezone or "UTC"


def _timescale_to_params(unit: str, nb: int) -> tuple[str, int, int]:
    """Convertit (unit, nb) → (resolution, k_minutes, k_days)."""
    if unit == "min":
        return "1min", nb, 1
    if unit == "hour":
        return "1min", nb * 60, 1
    if unit == "day":
        return "1day", 1, nb
    if unit == "week":
        return "1day", 1, nb * 7
    raise HTTPException(
        status_code=400,
        detail=f"timescale_unit '{unit}' non implémenté. Unités: min, hour, day, week.",
    )



def _query_chart_ohlcv(
    instrument,
    settings,
    chain,
    *,
    before_dt,
    after_dt=None,
    k_minutes,
    k_days,
    week_aligned,
    resolution,
    defaults,
    include_cols,
):
    """query() pour le chart, via include_cols (retire les colonnes optionnelles absentes)."""
    wanted = list(include_cols)
    kwargs = {
        "start": after_dt,
        "end": before_dt,
        "k_minutes": k_minutes,
        "k_days": k_days,
        "week_aligned": week_aligned,
        "resolution": resolution,
        "intraday_begin": defaults.intraday_begin if resolution != "1day" else None,
        "intraday_end": defaults.intraday_end if resolution != "1day" else None,
        "timezone": defaults.timezone if resolution != "1day" else "UTC",
        "normalize_tick_size": defaults.normalize_tick_size if resolution != "1day" else False,
        "adjust_rollover": defaults.adjust_rollover,
        "no_split": defaults.no_split,
        "forward_fill": defaults.forward_fill,
        "limit": None,
    }
    try:
        return query(instrument, settings, chain, include_cols=wanted, **kwargs)
    except ValueError as exc:
        msg = str(exc)
        if "include_cols" not in msg:
            raise
        # "Colonnes inconnues pour include_cols: volume (disponibles: ...)"
        missing_part = msg.split("include_cols:", 1)[-1].split("(disponibles", 1)[0]
        missing = {c.strip() for c in missing_part.split(",") if c.strip()}
        reduced = [c for c in wanted if c not in missing]
        if not reduced or reduced == wanted:
            raise
        return query(instrument, settings, chain, include_cols=reduced, **kwargs)


def _prepare_chart_df(df: pl.DataFrame) -> pl.DataFrame:
    """Filtre et caste les colonnes pour produire un Arrow IPC compatible apache-arrow JS.

    Le frontend n'a besoin que de : time, OHLC, volume, candle_count.
    On élimine les colonnes ``Categorical`` (non supportées par apache-arrow JS)
    et on caste les timestamps en ``ms`` + le volume en ``Int32``.
    """
    select_exprs: list[pl.Expr] = [
        pl.col("window_start").cast(pl.Datetime("ms")).alias("time"),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
    ]

    if "volume" in df.columns:
        # Float64 : volumes daily Yahoo peuvent dépasser Int32 ; JS gère bien f64.
        select_exprs.append(pl.col("volume").cast(pl.Float64))
    if "candle_count" in df.columns:
        select_exprs.append(pl.col("candle_count").cast(pl.Int32))

    # query() déduplique déjà les timestamps de roll (défaut).
    return df.select(select_exprs).sort("time")


def _render_dashboard_html(
    instruments: dict[str, Instrument],
    settings: Settings,
    defaults: ChartDefaults,
    *,
    enable_portfolio: bool = False,
) -> str:
    """Génère la page dashboard avec cartes injectées en JSON."""
    template_path = _STATIC_DIR / "dashboard.html"
    html = template_path.read_text(encoding="utf-8")
    lookback = defaults.thumbnail_lookback_days
    cards = build_dashboard_cards(instruments, settings, lookback_days=lookback)
    portfolio_btns = []
    rf_label = ""
    if enable_portfolio:
        portfolio_btns = [
            {
                "key": key,
                "label": "Max Sharpe" if obj == "max-sharpe" else "Min Vol",
                "objective": obj,
            }
            for key, obj in PORTFOLIO_PRODUCTS.items()
        ]
        from myquantstore.analytics.risk_free import resolve_risk_free_rate

        try:
            rf_q = resolve_risk_free_rate(settings)
            if rf_q.source == "yahoo":
                rf_label = f"rf={rf_q.rate:.2%} ({rf_q.detail or 'yahoo'})"
            else:
                rf_label = f"rf={rf_q.rate:.2%} ({rf_q.source})"
        except Exception:
            rf_label = f"rf={settings.portfolio_risk_free_rate:.2%} (static)"
    payload = {
        "lookback_days": lookback,
        "cards": cards,
        "portfolio": portfolio_btns,
        "rf_label": rf_label,
    }
    # JSON sûr dans <script type="application/json">
    json_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    json_str = json_str.replace("<", "\\u003c")
    return html.replace("__DASHBOARD_JSON__", json_str)


def _render_chart_html(instrument_key: str, defaults: ChartDefaults) -> str:
    """Génère la page HTML du chart en injectant les paramètres par défaut."""
    template_path = _STATIC_DIR / "chart.html"
    html = template_path.read_text(encoding="utf-8")

    intraday_begin_str = f'"{defaults.intraday_begin.isoformat()}"' if defaults.intraday_begin else "null"
    intraday_end_str = f'"{defaults.intraday_end.isoformat()}"' if defaults.intraday_end else "null"

    replacements = {
        "__PRODUCT__": instrument_key,
        "__TIMESCALE_UNIT__": defaults.timescale_unit,
        "__TIMESCALE_NB__": str(defaults.timescale_nb),
        "__NB_CANDLE__": str(defaults.nb_candle),
        "__MAX_VISIBLE_CANDLES__": str(defaults.max_visible_candles),
        "__BUFFER_MULTIPLIER__": str(defaults.buffer_multiplier),
        "__FETCH_CHUNK_SIZE__": str(defaults.fetch_chunk_size),
        "__INTRADAY_BEGIN__": intraday_begin_str,
        "__INTRADAY_END__": intraday_end_str,
        "__NORMALIZE_TICK_SIZE__": str(defaults.normalize_tick_size).lower(),
        "__CANDLE_UP__": defaults.candle_up,
        "__CANDLE_DOWN__": defaults.candle_down,
        "__TX_BUY__": defaults.tx_buy,
        "__TX_SELL__": defaults.tx_sell,
        "__ORDER_BUY__": defaults.order_buy,
        "__ORDER_SELL__": defaults.order_sell,
        "__TIMEZONE__": defaults.timezone,
    }

    for key, value in replacements.items():
        html = html.replace(key, value)

    current_ts = f"{defaults.timescale_unit}:{defaults.timescale_nb}"
    # Markers alignés sur chart.html (__SEL_1min__, __SEL_1h__, __SEL_1d__, …)
    ts_markers = {
        "min:1": "__SEL_1min__",
        "min:2": "__SEL_2min__",
        "min:5": "__SEL_5min__",
        "min:10": "__SEL_10min__",
        "min:15": "__SEL_15min__",
        "min:30": "__SEL_30min__",
        "hour:1": "__SEL_1h__",
        "hour:2": "__SEL_2h__",
        "hour:4": "__SEL_4h__",
        "day:1": "__SEL_1d__",
        "day:2": "__SEL_2d__",
        "week:1": "__SEL_1w__",
    }
    for opt, marker in ts_markers.items():
        html = html.replace(marker, "selected" if opt == current_ts else "")

    return html


def run_server(
    settings: Settings,
    instruments: dict[str, Instrument],
    chains: dict[str, InstrumentChain],
    defaults: ChartDefaults,
    port: int,
    host: str,
    mdns: bool = False,
) -> None:
    """Lance le serveur uvicorn (bloquant)."""
    import uvicorn

    stock_insts = [i for i in instruments.values() if i.type == InstrumentType.STOCKS]
    if not stock_insts:
        stock_insts = settings.instruments_of_type(InstrumentType.STOCKS)
    psvc = PortfolioService(settings, stock_insts)
    app = create_chart_app(
        settings, instruments, chains, defaults, portfolio_service=psvc
    )

    mdns_service = None
    if mdns:
        from myquantstore.chart.mdns import register_mdns

        mdns_service = register_mdns(host, port)
        logger.info("Service mDNS enregistré: accessible via le réseau local")

    logger.info(f"Serveur chart démarré sur http://{host}:{port}")
    logger.info(f"Instruments servis: {list(instruments.keys())}")
    logger.info(f"Instrument par défaut: {defaults.default_product}")
    logger.info(
        f"Timescale: {defaults.timescale_nb}{defaults.timescale_unit} | "
        f"Max visible: {defaults.max_visible_candles} | "
        f"Buffer: {defaults.buffer_multiplier}x | "
        f"Thumbnails: {defaults.thumbnail_lookback_days}j"
    )

    try:
        uvicorn.run(app, host=host, port=port, log_level="warning")
    finally:
        if mdns_service:
            mdns_service.close()
            logger.info("Service mDNS désenregistré")
