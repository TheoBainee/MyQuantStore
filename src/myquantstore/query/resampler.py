"""Rééchantillonnage et filtrage intraday des candles OHLCV.

Ce module fournit deux fonctions utilisées par la commande ``query`` :

- :func:`filter_intraday` : filtre les candles par heure du jour (supporte
  le wrap-around, ex: 20:00-04:00 pour les sessions overnight).
- :func:`resample_ohlcv` : rééchantillonne des candles 1min en candles k-min
  (ex: 7min, 15min, 60min) avec une grille **ancrée au début de la session**
  pour garantir la cohérence entre jours.

**Problème de cohérence** : ``group_by_dynamic`` de Polars ancre la grille à
l'epoch (1970-01-01), pas au début de la session. Résultat : les buckets
sont décalés différemment chaque jour (ex: 22:03/22:10 le lundi, 22:01/22:08
le mardi). La solution est de calculer manuellement l'ancre (anchor) par
session, puis de bucketer relativement à cette ancre.

**Algorithme de bucketing** :

1. **Anchor** : calculé par session (groupé par ``session_end_date``) :
   - **Avec intraday** : ``anchor = session_end_date + intraday_begin`` (ou
     ``(session_end_date - 1) + intraday_begin`` pour le wrap-around, car
     la session commence la veille).
   - **Sans intraday** : ``anchor = min(window_start)`` par session (le
     premier candle de la session).

2. **Bucket** : pour chaque candle, ``bucket_id = floor((window_start - anchor) / k)``
   et le timestamp du bucket = ``anchor + bucket_id * k`` (réécrit dans
   ``window_start``).

3. **Agrégation** : ``group_by([session_end_date, window_start])`` avec
   ``open=first, high=max, low=min, close=last, volume=sum, transactions=sum,
   dollar_volume=sum``. La colonne ``candle_count`` compte le nombre de
   candles 1min agrégés dans chaque bucket.

4. **Drop des partiels de fin** : un bucket est partiel si ``window_start + k
   > session_end``. On drop ces buckets pour garantir que tous les buckets
   font exactement k minutes.

**Gaps intra-session** : si des candles 1min manquent dans un bucket (pas de
trades), le bucket est **conservé** avec ``candle_count < k``. C'est un
comportement naturel du ``group_by`` — on n'invente pas de données.
"""

from __future__ import annotations

from datetime import date, time, timedelta

import polars as pl

from myquantstore.logging_setup import get_logger

logger = get_logger("resampler")


def _ws_time_zone(df: pl.DataFrame) -> str | None:
    dtype = df.schema.get("window_start")
    return getattr(dtype, "time_zone", None) if dtype is not None else None


def _local_time_expr(timezone: str, ws_tz: str | None) -> pl.Expr:
    """Heure locale de ``window_start`` dans ``timezone`` (stockage = UTC)."""
    expr = pl.col("window_start")
    if ws_tz:
        expr = expr.dt.convert_time_zone(timezone)
    else:
        expr = expr.dt.replace_time_zone("UTC").dt.convert_time_zone(timezone)
    return expr.dt.time()


def _wall_clock_on_date_naive_utc(
    date_expr: pl.Expr,
    clock: time,
    timezone: str,
    *,
    day_offset: int = 0,
) -> pl.Expr:
    """Date calendaire + HH:MM en ``timezone`` → datetime naive UTC (ns)."""
    base = date_expr.cast(pl.Datetime("ns"))
    if day_offset:
        base = base + pl.duration(days=day_offset)
    local_naive = base + pl.duration(
        hours=clock.hour,
        minutes=clock.minute,
        seconds=clock.second or 0,
    )
    return (
        local_naive.dt.replace_time_zone(timezone)
        .dt.convert_time_zone("UTC")
        .dt.replace_time_zone(None)
    )


def _as_naive_utc_window(df: pl.DataFrame) -> tuple[pl.DataFrame, str | None]:
    """``window_start`` → naive UTC ; retourne (df, tz_origine ou None)."""
    ws_tz = _ws_time_zone(df)
    if not ws_tz:
        return df, None
    out = df.with_columns(
        pl.col("window_start")
        .dt.convert_time_zone("UTC")
        .dt.replace_time_zone(None)
        .alias("window_start")
    )
    return out, ws_tz


def _restore_window_tz(df: pl.DataFrame, ws_tz: str | None) -> pl.DataFrame:
    if not ws_tz or "window_start" not in df.columns:
        return df
    return df.with_columns(
        pl.col("window_start").dt.replace_time_zone("UTC").dt.convert_time_zone(ws_tz)
    )


def filter_intraday(
    df: pl.DataFrame,
    intraday_begin: time,
    intraday_end: time,
    *,
    timezone: str = "UTC",
) -> pl.DataFrame:
    """Filtre les candles par heure du jour (time-of-day) dans ``timezone``.

    Deux modes selon l'ordre des bornes :

    - **Normal** (``begin < end``, ex: 09:30-16:00) : garde les candles dont
      l'heure **locale** est dans ``[begin, end]`` (inclusif aux deux bornes).

    - **Wrap-around** (``begin > end``, ex: 20:00-04:00) : garde les candles
      dont l'heure locale est ``>= begin`` **ou** ``<= end``. Utile pour les
      sessions overnight qui spannent minuit.

    ``window_start`` est interprété en UTC (naive = UTC). ``intraday_begin/end``
    sont des heures murales dans ``timezone`` (IANA, défaut ``UTC``).

    :param df: DataFrame Polars avec colonne ``window_start`` (Datetime).
    :param intraday_begin: Heure de début (ex: ``time(9, 30)``).
    :param intraday_end: Heure de fin (ex: ``time(16, 0)``).
    :param timezone: Fuseau IANA pour interpréter begin/end (ex: ``America/Chicago``).
    :raises ValueError: Si ``intraday_begin == intraday_end``.
    :return: DataFrame filtré (mêmes colonnes, moins de lignes).
    """
    if intraday_begin == intraday_end:
        raise ValueError(
            f"intraday_begin ({intraday_begin}) ne peut pas être égal à "
            f"intraday_end ({intraday_end})."
        )

    tz = timezone or "UTC"
    local_t = _local_time_expr(tz, _ws_time_zone(df))

    if intraday_begin < intraday_end:
        mask = (local_t >= intraday_begin) & (local_t <= intraday_end)
        mode = "normal"
    else:
        mask = (local_t >= intraday_begin) | (local_t <= intraday_end)
        mode = "wrap-around"

    logger.debug(
        f"Filtrage intraday ({mode}): {intraday_begin} - {intraday_end} tz={tz}"
    )
    return df.filter(mask)


def resample_ohlcv(
    df: pl.DataFrame,
    k_minutes: int,
    intraday_begin: time | None = None,
    intraday_end: time | None = None,
    *,
    timezone: str = "UTC",
) -> pl.DataFrame:
    """Rééchantillonne des candles 1min en candles k-min.

    La grille est **ancrée au début de chaque session** pour garantir la
    cohérence entre jours : le bucket N démarre à ``anchor + N * k``, identique
    pour chaque session. Voir la docstring du module pour l'algorithme complet.

    :param df: DataFrame Polars de candles 1min avec colonnes ``window_start``,
        ``session_end_date``, ``open``, ``high``, ``low``, ``close``, ``volume``,
        ``transactions``, ``dollar_volume``.
    :param k_minutes: Taille du bucket en minutes (ex: 7 pour 7min).
    :param intraday_begin: Heure de début intraday (si le filtrage intraday a
        été appliqué avant). Utilisé pour calculer l'ancre.
    :param intraday_end: Heure de fin intraday. Utilisé pour calculer la fin
        de session (et dropper les partiels).
    :param timezone: Fuseau IANA des heures intraday (défaut ``UTC``).
    :raises ValueError: Si ``k_minutes < 1``.
    :return: DataFrame Polars de candles k-min avec une colonne supplémentaire
        ``candle_count`` (nombre de candles 1min agrégés par bucket).
    """
    if k_minutes < 1:
        raise ValueError(f"k_minutes doit être >= 1 (reçu: {k_minutes})")

    # k=1 → pas de resampling, retourne tel quel (mais ajoute candle_count=1)
    if k_minutes == 1:
        logger.debug("k=1 : pas de resampling (noop)")
        if "candle_count" not in df.columns:
            return df.with_columns(pl.lit(1).cast(pl.Int32).alias("candle_count"))
        return df

    tz = timezone or "UTC"
    logger.info(f"Resampling 1min -> {k_minutes}min (tz={tz})")

    df, orig_ws_tz = _as_naive_utc_window(df)

    # --- 1. Calculer l'ancre (anchor) par session ---
    # L'ancre est le point de départ de la grille de bucketing (naive UTC).
    if intraday_begin is not None and intraday_end is not None:
        # Mode intraday : ancre = session_end_date @ begin (TZ) → UTC
        # Wrap-around (begin > end) : session commence la veille
        sed = pl.col("session_end_date")
        if intraday_begin > intraday_end:
            anchor_expr = _wall_clock_on_date_naive_utc(
                sed, intraday_begin, tz, day_offset=-1
            )
            session_end_expr = _wall_clock_on_date_naive_utc(sed, intraday_end, tz)
        else:
            anchor_expr = _wall_clock_on_date_naive_utc(sed, intraday_begin, tz)
            session_end_expr = _wall_clock_on_date_naive_utc(sed, intraday_end, tz)

        # Joindre l'ancre et la fin de session par session_end_date
        anchors = df.select("session_end_date").unique().with_columns(
            anchor_expr.alias("anchor"),
            session_end_expr.alias("session_end"),
        )
        df = df.join(anchors, on="session_end_date")
    else:
        # Mode sans intraday : ancre = min(window_start) par session
        # Fin de session = max(window_start) + 1min
        anchors = df.group_by("session_end_date").agg(
            pl.col("window_start").min().alias("anchor"),
            (pl.col("window_start").max() + pl.duration(minutes=1)).alias("session_end"),
        )
        df = df.join(anchors, on="session_end_date")

    # --- 2. Calculer bucket_id puis réécrire window_start = début du bucket ---
    df = df.with_columns(
        ((pl.col("window_start") - pl.col("anchor")).dt.total_minutes() // k_minutes)
        .cast(pl.Int64)
        .alias("bucket_id")
    )
    df = df.with_columns(
        (pl.col("anchor") + pl.duration(minutes=k_minutes) * pl.col("bucket_id")).alias("window_start")
    )

    # --- 3. Agréger par (session_end_date, window_start) ---
    agg_exprs = [
        pl.col("open").first(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("close").last(),
        pl.len().alias("candle_count"),
    ]
    # Colonnes optionnelles (somme si présentes)
    for col in ("volume", "transactions", "dollar_volume"):
        if col in df.columns:
            agg_exprs.append(pl.col(col).sum())
    # Ticker (first si présent)
    if "ticker" in df.columns:
        agg_exprs.append(pl.col("ticker").first())
    # settlement_price (first si présent)
    if "settlement_price" in df.columns:
        agg_exprs.append(pl.col("settlement_price").first())

    agg = df.group_by(["session_end_date", "window_start"]).agg(agg_exprs)

    # Joindre session_end pour filtrer les partiels
    session_ends = df.select(["session_end_date", "session_end"]).unique()
    agg = agg.join(session_ends, on="session_end_date")

    # --- 4. Drop des partiels de fin de session ---
    # Un bucket est partiel si window_start + k > session_end
    before_drop = agg.height
    agg = agg.filter(pl.col("window_start") + pl.duration(minutes=k_minutes) <= pl.col("session_end"))
    dropped = before_drop - agg.height
    if dropped > 0:
        logger.info(f"Drop de {dropped} bucket(s) partiel(s) de fin de session")

    # Nettoyer les colonnes temporaires
    agg = agg.drop(["session_end"])

    # Cast candle_count en Int32 (cohérent avec les autres colonnes entières)
    agg = agg.with_columns(pl.col("candle_count").cast(pl.Int32))

    # Trier par window_start (chronologique)
    agg = agg.sort("window_start")

    logger.info(
        f"Resampling terminé: {agg.height} buckets {k_minutes}min "
        f"(depuis {df.height} candles 1min, {dropped} partiels droppés)"
    )
    return _restore_window_tz(agg, orig_ws_tz)


def resample_extraday(
    df: pl.DataFrame,
    k_days: int,
    *,
    week_aligned: bool = False,
) -> pl.DataFrame:
    """Rééchantillonne des barres daily en multi-day / multi-week.

    **Bucketing**
    - ``week_aligned=False`` (``--timescale-unit day``) : fenêtres de ``k_days``
      jours calendaires ancrées sur la 1ʳᵉ date de la série.
    - ``week_aligned=True`` (``--timescale-unit week``) : semaines ISO (lundi→dimanche),
      groupées par paquets de ``k_days // 7`` semaines (``k_days`` multiple de 7).

    **Partiels** — on ne exige **pas** ``candle_count >= k_days`` (une semaine
    boursière n'a que ~5 séances, jamais 7 barres daily). Règle :

    - tous les buckets historiques (``bucket_id < max``) sont **conservés**
      dès qu'ils ont ≥1 barre (jours fériés / half-weeks OK) ;
    - le **dernier** bucket n'est gardé que si sa fenêtre calendaire est
      entièrement couverte par ``last_session_date`` (semaine/période close).

    :param df: Barres daily avec ``window_start`` / ``session_end_date``.
    :param k_days: Taille calendaire en jours (1 = noop + candle_count).
    :param week_aligned: Ancrage lundi (UT week).
    """
    if k_days < 1:
        raise ValueError(f"k_days doit être >= 1 (reçu: {k_days})")

    if df.is_empty():
        return df

    if k_days == 1:
        if "candle_count" not in df.columns:
            return df.with_columns(pl.lit(1).cast(pl.Int32).alias("candle_count"))
        return df

    if week_aligned and k_days % 7 != 0:
        raise ValueError(
            f"week_aligned requiert k_days multiple de 7 (reçu: {k_days})"
        )

    label = f"{k_days // 7}week" if week_aligned else f"{k_days}day"
    logger.info(f"Resampling extraday 1day -> {label}")

    work = df.sort("window_start")
    if "session_end_date" not in work.columns:
        work = work.with_columns(pl.col("window_start").dt.date().alias("session_end_date"))

    last_date = work["session_end_date"].max()
    if not isinstance(last_date, date):
        last_date = last_date  # type: ignore[assignment]

    if week_aligned:
        # Lundi de la semaine ISO (Polars weekday: Mon=1 … Sun=7)
        n_weeks = k_days // 7
        work = work.with_columns(
            (
                pl.col("session_end_date")
                - pl.duration(days=pl.col("session_end_date").dt.weekday() - 1)
            ).alias("_period_start")
        )
        anchor: date = work["_period_start"].min()  # type: ignore[assignment]
        work = work.with_columns(
            (
                (
                    pl.col("_period_start").cast(pl.Datetime("ns"))
                    - pl.lit(anchor).cast(pl.Datetime("ns"))
                ).dt.total_days()
                // (7 * n_weeks)
            )
            .cast(pl.Int64)
            .alias("bucket_id")
        )
    else:
        anchor = work["session_end_date"].min()  # type: ignore[assignment]
        work = work.with_columns(
            (
                (
                    pl.col("session_end_date").cast(pl.Datetime("ns"))
                    - pl.lit(anchor).cast(pl.Datetime("ns"))
                ).dt.total_days()
                // k_days
            )
            .cast(pl.Int64)
            .alias("bucket_id")
        )

    agg_exprs = [
        pl.col("open").first(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("close").last(),
        pl.col("session_end_date").min().alias("session_end_date"),
        pl.col("window_start").min().alias("window_start"),
        pl.len().alias("candle_count"),
    ]
    for col in ("volume", "transactions", "dollar_volume"):
        if col in work.columns:
            agg_exprs.append(pl.col(col).sum())
    if "ticker" in work.columns:
        agg_exprs.append(pl.col("ticker").first())

    agg = work.group_by("bucket_id").agg(agg_exprs)
    before = agg.height
    max_id = int(agg["bucket_id"].max())

    # Historique : garder dès 1 barre (week-ends / fériés → candle_count < k_days).
    # Dernier bucket : uniquement si sa fenêtre calendaire est close.
    last_cal_end = anchor + timedelta(days=(max_id + 1) * k_days - 1)
    if last_date < last_cal_end:
        agg = agg.filter(pl.col("bucket_id") < max_id)
    dropped = before - agg.height
    if dropped > 0:
        logger.info(f"Drop de {dropped} bucket(s) extraday partiel(s)")

    agg = agg.with_columns(pl.col("candle_count").cast(pl.Int32)).sort("window_start")
    if "bucket_id" in agg.columns:
        agg = agg.drop("bucket_id")

    logger.info(f"Resampling extraday terminé: {agg.height} buckets {label}")
    return agg
