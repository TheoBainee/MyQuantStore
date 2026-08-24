"""Tests du module query/resampler.py.

Couvre :
- Cohérence du bucketing entre sessions (même grille ancrée).
- Drop des partiels de fin de session.
- Conservation des gaps intra-session (candle_count < k).
- Agrégation OHLCV (open=first, close=last, high=max, low=min, volume=sum).
- k=1 (noop) et k invalide (ValueError).
- Filtrage intraday normal (begin < end) et wrap-around (begin > end).
- Cohérence + drop partiels avec intraday.
"""

from __future__ import annotations

from datetime import date, datetime, time

import polars as pl
import pytest

from myquantstore.query.resampler import filter_intraday, forward_fill_ohlcv, resample_ohlcv


def _make_session_df(
    session_end_date: date,
    window_starts: list[datetime],
    opens: list[float] | None = None,
    volumes: list[int] | None = None,
) -> pl.DataFrame:
    """Crée un DataFrame de candles 1min pour une session donnée.

    :param session_end_date: Date de fin de session (clé de regroupement).
    :param window_starts: Liste des timestamps de début de chandelier.
    :param opens: Prix open (si None, utilise l'index du chandelier).
    :param volumes: Volumes (si None, utilise 100 pour tous).
    """
    n = len(window_starts)
    if opens is None:
        opens = [float(i) for i in range(n)]
    if volumes is None:
        volumes = [100] * n
    return pl.DataFrame(
        {
            "window_start": window_starts,
            "ticker": ["TEST"] * n,
            "open": opens,
            "high": [o + 1.0 for o in opens],
            "low": [o - 1.0 for o in opens],
            "close": [o + 0.5 for o in opens],
            "volume": volumes,
            "dollar_volume": [o * 100 for o in opens],
            "transactions": [10] * n,
            "session_end_date": [session_end_date] * n,
        }
    )


class TestResampleCoherence:
    """Cohérence du bucketing entre sessions (grille ancrée)."""

    def test_resample_coherence(self):
        """2 sessions k=7 → mêmes window_start (relatifs à l'ancre de session)."""
        # Session A: 10 candles à partir de 09:30
        starts_a = [datetime(2026, 7, 10, 9, 30 + i) for i in range(10)]
        df_a = _make_session_df(date(2026, 7, 10), starts_a)
        # Session B: 10 candles à partir de 09:30 (jour suivant)
        starts_b = [datetime(2026, 7, 11, 9, 30 + i) for i in range(10)]
        df_b = _make_session_df(date(2026, 7, 11), starts_b)

        df = pl.concat([df_a, df_b])
        result = resample_ohlcv(df, k_minutes=7)

        # Les window_start doivent être identiques (au jour près) pour chaque session
        buckets_a = (
            result.filter(pl.col("session_end_date") == date(2026, 7, 10))
            .sort("window_start")
            .select(pl.col("window_start").dt.time())
            .to_series()
            .to_list()
        )
        buckets_b = (
            result.filter(pl.col("session_end_date") == date(2026, 7, 11))
            .sort("window_start")
            .select(pl.col("window_start").dt.time())
            .to_series()
            .to_list()
        )
        assert buckets_a == buckets_b, (
            f"Les buckets doivent être cohérents entre sessions: A={buckets_a}, B={buckets_b}"
        )
        # 10 candles k=7 → 1 bucket complet (7 candles) + 1 partiel (3 candles) droppé
        # → 1 bucket attendu (09:30-09:36)
        assert len(buckets_a) == 1
        assert buckets_a[0] == time(9, 30)


class TestResampleDropEndPartial:
    """Drop des buckets partiels de fin de session."""

    def test_resample_drop_end_partial(self):
        """Session 10min k=7 → 1 bucket (09:30-09:36), 09:37 droppé (partiel)."""
        starts = [datetime(2026, 7, 10, 9, 30 + i) for i in range(10)]
        df = _make_session_df(date(2026, 7, 10), starts)

        result = resample_ohlcv(df, k_minutes=7)

        # 10 candles k=7 → 1 bucket complet (7) + 1 partiel (3) droppé
        assert result.height == 1
        bucket_time = result["window_start"][0].time()
        assert bucket_time == time(9, 30)
        assert result["candle_count"][0] == 7


class TestResampleKeepGapPartials:
    """Conservation des gaps intra-session (candles manquants)."""

    def test_resample_keep_gap_partials(self):
        """Bucket avec 5/7 candles (gap) → conservé, candle_count=5."""
        # 7 candles pour le 1er bucket (09:30-09:36), puis on saute 09:37 et 09:38,
        # puis 5 candles à partir de 09:39 (09:39-09:43) qui tombent dans le bucket 09:37-09:43
        # Le bucket 09:37-09:43 n'a que 5 candles (09:39-09:43) → gap, mais conservé
        starts = [datetime(2026, 7, 10, 9, 30 + i) for i in range(7)]  # 09:30-09:36
        starts += [datetime(2026, 7, 10, 9, 39 + i) for i in range(5)]  # 09:39-09:43
        df = _make_session_df(date(2026, 7, 10), starts)

        result = resample_ohlcv(df, k_minutes=7)

        # 2 buckets attendus : 09:30 (7 candles) et 09:37 (5 candles, gap)
        # Le bucket 09:37 + 7 = 09:44 → check session_end : max(window_start)+1 = 09:44
        # 09:44 <= 09:44 → conservé
        assert result.height == 2
        result_sorted = result.sort("window_start")
        assert result_sorted["window_start"][0].time() == time(9, 30)
        assert result_sorted["candle_count"][0] == 7
        assert result_sorted["window_start"][1].time() == time(9, 37)
        assert result_sorted["candle_count"][1] == 5


class TestOHLCVAggregation:
    """Vérification des règles d'agrégation OHLCV."""

    def test_ohlcv_aggregation(self):
        """open=first, close=last, high=max, low=min, volume=sum."""
        # 7 candles dans un seul bucket (k=7)
        opens = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0]
        starts = [datetime(2026, 7, 10, 9, 30 + i) for i in range(7)]
        df = _make_session_df(
            date(2026, 7, 10), starts, opens=opens, volumes=[10, 20, 30, 40, 50, 60, 70]
        )

        result = resample_ohlcv(df, k_minutes=7)

        assert result.height == 1
        row = result.row(0, named=True)
        assert row["open"] == 100.0  # first
        assert row["close"] == 106.5  # last close (106.0 + 0.5)
        assert row["high"] == 107.0  # max (106.0 + 1.0)
        assert row["low"] == 99.0  # min (100.0 - 1.0)
        assert row["volume"] == 280  # 10+20+30+40+50+60+70
        assert row["candle_count"] == 7


class TestK1Noop:
    """k=1 ne fait pas de resampling."""

    def test_k1_noop(self):
        """k=1 retourne les mêmes données avec candle_count=1 ajouté."""
        starts = [datetime(2026, 7, 10, 9, 30 + i) for i in range(5)]
        df = _make_session_df(date(2026, 7, 10), starts)

        result = resample_ohlcv(df, k_minutes=1)

        # Même nombre de lignes
        assert result.height == df.height
        # candle_count = 1 pour toutes les lignes
        assert result["candle_count"].to_list() == [1] * df.height
        # Les prix open sont identiques
        assert result["open"].to_list() == df["open"].to_list()


class TestInvalidK:
    """Validation de k_minutes."""

    @pytest.mark.parametrize("k", [0, -1, -10])
    def test_invalid_k(self, k: int):
        """k < 1 → ValueError."""
        df = _make_session_df(date(2026, 7, 10), [datetime(2026, 7, 10, 9, 30)])
        with pytest.raises(ValueError, match="k_minutes doit être >= 1"):
            resample_ohlcv(df, k_minutes=k)


class TestFilterIntradayNormal:
    """Filtrage intraday mode normal (begin < end)."""

    def test_intraday_normal(self):
        """begin=09:30 < end=16:00 → garde [09:30, 16:00]."""
        starts = [
            datetime(2026, 7, 10, 8, 0),  # avant → exclu
            datetime(2026, 7, 10, 9, 30),  # borne début → inclus
            datetime(2026, 7, 10, 12, 0),  # milieu → inclus
            datetime(2026, 7, 10, 16, 0),  # borne fin → inclus
            datetime(2026, 7, 10, 17, 0),  # après → exclu
        ]
        df = _make_session_df(date(2026, 7, 10), starts)

        result = filter_intraday(df, time(9, 30), time(16, 0))

        assert result.height == 3
        kept_times = result["window_start"].dt.time().to_list()
        assert time(9, 30) in kept_times
        assert time(12, 0) in kept_times
        assert time(16, 0) in kept_times
        assert time(8, 0) not in kept_times
        assert time(17, 0) not in kept_times


class TestFilterIntradayWraparound:
    """Filtrage intraday mode wrap-around (begin > end, spanne minuit)."""

    def test_intraday_wraparound(self):
        """begin=20:00 > end=04:00 → garde >= 20:00 OR <= 04:00."""
        starts = [
            datetime(2026, 7, 10, 3, 0),  # <= 04:00 → inclus
            datetime(2026, 7, 10, 4, 0),  # borne fin → inclus
            datetime(2026, 7, 10, 12, 0),  # milieu → exclu
            datetime(2026, 7, 10, 20, 0),  # borne début → inclus
            datetime(2026, 7, 10, 23, 0),  # >= 20:00 → inclus
        ]
        df = _make_session_df(date(2026, 7, 10), starts)

        result = filter_intraday(df, time(20, 0), time(4, 0))

        assert result.height == 4
        kept_times = result["window_start"].dt.time().to_list()
        assert time(3, 0) in kept_times
        assert time(4, 0) in kept_times
        assert time(20, 0) in kept_times
        assert time(23, 0) in kept_times
        assert time(12, 0) not in kept_times


class TestFilterIntradayEquals:
    """Validation begin != end."""

    def test_intraday_begin_equals_end(self):
        """begin == end → ValueError."""
        df = _make_session_df(date(2026, 7, 10), [datetime(2026, 7, 10, 9, 30)])
        with pytest.raises(ValueError, match="ne peut pas être égal"):
            filter_intraday(df, time(9, 30), time(9, 30))


class TestFilterIntradayTimezone:
    """intraday_begin/end interprétés dans une timezone IANA."""

    def test_chicago_summer_cdt(self):
        """14:30 UTC = 09:30 CDT ; 21:00 UTC = 16:00 CDT (2024-07)."""
        starts = [
            datetime(2024, 7, 15, 14, 0),  # 09:00 CDT → exclu
            datetime(2024, 7, 15, 14, 30),  # 09:30 CDT → inclus
            datetime(2024, 7, 15, 21, 0),  # 16:00 CDT → inclus
            datetime(2024, 7, 15, 21, 30),  # 16:30 CDT → exclu
        ]
        df = _make_session_df(date(2024, 7, 15), starts)
        result = filter_intraday(df, time(9, 30), time(16, 0), timezone="America/Chicago")
        kept = result["window_start"].to_list()
        assert len(kept) == 2
        assert datetime(2024, 7, 15, 14, 30) in kept
        assert datetime(2024, 7, 15, 21, 0) in kept


class TestIntradayCoherence:
    """Cohérence du bucketing avec filtrage intraday."""

    def test_intraday_coherence(self):
        """2 sessions intraday k=7 → mêmes window_start (09:30, 09:37)."""
        # Session A: 09:30-09:39 (10 candles)
        starts_a = [datetime(2026, 7, 10, 9, 30 + i) for i in range(10)]
        df_a = _make_session_df(date(2026, 7, 10), starts_a)
        # Session B: 09:30-09:39 (10 candles)
        starts_b = [datetime(2026, 7, 11, 9, 30 + i) for i in range(10)]
        df_b = _make_session_df(date(2026, 7, 11), starts_b)

        df = pl.concat([df_a, df_b])
        df = filter_intraday(df, time(9, 30), time(16, 0))
        result = resample_ohlcv(
            df, k_minutes=7, intraday_begin=time(9, 30), intraday_end=time(16, 0)
        )

        buckets_a = (
            result.filter(pl.col("session_end_date") == date(2026, 7, 10))
            .sort("window_start")
            .select(pl.col("window_start").dt.time())
            .to_series()
            .to_list()
        )
        buckets_b = (
            result.filter(pl.col("session_end_date") == date(2026, 7, 11))
            .sort("window_start")
            .select(pl.col("window_start").dt.time())
            .to_series()
            .to_list()
        )
        assert buckets_a == buckets_b
        # 10 candles k=7 → 2 buckets : 09:30 (7 candles) + 09:37 (3 candles, gap)
        # 09:37+7=09:44 <= 16:00 (session_end) → conservé (pas un partiel de fin de session)
        assert len(buckets_a) == 2
        assert buckets_a[0] == time(9, 30)
        assert buckets_a[1] == time(9, 37)


class TestIntradayDropPartial:
    """Drop des partiels induits par la fenêtre intraday."""

    def test_intraday_drop_partial(self):
        """Intraday 09:30-09:39 (9min), k=7 → 1 bucket (09:30-09:36), 09:37 droppé."""
        starts = [datetime(2026, 7, 10, 9, 30 + i) for i in range(10)]
        df = _make_session_df(date(2026, 7, 10), starts)

        df = filter_intraday(df, time(9, 30), time(9, 39))
        result = resample_ohlcv(
            df, k_minutes=7, intraday_begin=time(9, 30), intraday_end=time(9, 39)
        )

        # 10 candles k=7 → 1 bucket (09:30-09:36), 09:37 droppé (09:37+7=09:44 > 09:39)
        assert result.height == 1
        assert result["window_start"][0].time() == time(9, 30)
        assert result["candle_count"][0] == 7


class TestWraparoundCoherence:
    """Cohérence du bucketing en mode wrap-around (overnight)."""

    def test_wraparound_coherence(self):
        """Wrap-around 20:00-04:00 k=7 → buckets cohérents entre sessions."""
        # Session A: 20:00 la veille + 03:55-03:59 le jour J
        sd_a = date(2026, 7, 10)
        starts_a = [datetime(2026, 7, 9, 20, m) for m in range(10)]  # 20:00-20:09 veille
        starts_a += [datetime(2026, 7, 10, 3, 55 + i) for i in range(5)]  # 03:55-03:59
        df_a = _make_session_df(sd_a, starts_a)

        # Session B: identique (jour suivant)
        sd_b = date(2026, 7, 11)
        starts_b = [datetime(2026, 7, 10, 20, m) for m in range(10)]  # 20:00-20:09 veille
        starts_b += [datetime(2026, 7, 11, 3, 55 + i) for i in range(5)]  # 03:55-03:59
        df_b = _make_session_df(sd_b, starts_b)

        df = pl.concat([df_a, df_b])
        df = filter_intraday(df, time(20, 0), time(4, 0))
        result = resample_ohlcv(
            df, k_minutes=7, intraday_begin=time(20, 0), intraday_end=time(4, 0)
        )

        # Les buckets doivent être cohérents entre les 2 sessions
        buckets_a = (
            result.filter(pl.col("session_end_date") == sd_a)
            .sort("window_start")
            .select(pl.col("window_start").dt.time())
            .to_series()
            .to_list()
        )
        buckets_b = (
            result.filter(pl.col("session_end_date") == sd_b)
            .sort("window_start")
            .select(pl.col("window_start").dt.time())
            .to_series()
            .to_list()
        )
        assert buckets_a == buckets_b, (
            f"Buckets incohérents en wrap-around: A={buckets_a}, B={buckets_b}"
        )
        # 20:00-20:09 (10 candles) → 1 bucket (20:00-20:06), 20:07 droppé (20:07+7=20:14 > 04:00)
        # 03:55-03:59 (5 candles) → 1 bucket (03:53-03:59) car 03:53 = 20:00 + (7h53 // 7 * 7)
        # Vérifier qu'on a au moins 1 bucket à 20:00
        assert time(20, 0) in buckets_a


class TestForwardFillIntraday:
    """Réinsertion des barres manquantes intra-session (opt-in)."""

    def test_fills_gap_with_last_close(self):
        """09:30, 09:32 (trou 09:31) → 3 barres, 09:31 = last close, volume 0."""
        starts = [
            datetime(2026, 7, 10, 9, 30),
            datetime(2026, 7, 10, 9, 32),
        ]
        df = _make_session_df(
            date(2026, 7, 10),
            starts,
            opens=[100.0, 110.0],
            volumes=[50, 80],
        )
        out = forward_fill_ohlcv(df, is_extraday=False, k_minutes=1)
        assert out.height == 3
        row = out.filter(pl.col("window_start") == datetime(2026, 7, 10, 9, 31)).row(0, named=True)
        last_close = 100.0 + 0.5  # _make_session_df : close = open + 0.5
        assert row["open"] == last_close
        assert row["high"] == last_close
        assert row["low"] == last_close
        assert row["close"] == last_close
        assert row["volume"] == 0
        assert row["transactions"] == 0
        assert row["dollar_volume"] == 0
        assert row["candle_count"] == 0
        assert row["ticker"] == "TEST"

    def test_does_not_fill_across_sessions(self):
        """Deux sessions (ven / lun) : pas de barres overnight / week-end."""
        df_a = _make_session_df(
            date(2026, 7, 10),
            [datetime(2026, 7, 10, 16, 0)],
            opens=[100.0],
        )
        df_b = _make_session_df(
            date(2026, 7, 13),
            [datetime(2026, 7, 13, 9, 30)],
            opens=[110.0],
        )
        df = pl.concat([df_a, df_b])
        out = forward_fill_ohlcv(df, is_extraday=False, k_minutes=1)
        assert out.height == 2

    def test_preserves_real_bars(self):
        starts = [datetime(2026, 7, 10, 9, 30 + i) for i in range(3)]
        df = _make_session_df(date(2026, 7, 10), starts, opens=[1.0, 2.0, 3.0])
        out = forward_fill_ohlcv(df, is_extraday=False, k_minutes=1)
        assert out.height == 3
        assert out["open"].to_list() == [1.0, 2.0, 3.0]
        assert out["volume"].to_list() == [100, 100, 100]


class TestForwardFillExtraday:
    """Réinsertion des jours ouvrés manquants (opt-in)."""

    def test_fills_weekday_holiday_not_weekend(self):
        """Lun + mer : fill mardi, pas samedi/dimanche jusqu'au mercredi."""
        rows = []
        for d, price in ((date(2024, 1, 1), 100.0), (date(2024, 1, 3), 102.0)):
            # 2024-01-01 = lundi, 2024-01-03 = mercredi
            rows.append(
                {
                    "window_start": datetime(d.year, d.month, d.day),
                    "session_end_date": d,
                    "ticker": "AAPL",
                    "open": price,
                    "high": price + 1,
                    "low": price - 1,
                    "close": price + 0.5,
                    "volume": 1000,
                    "transactions": 10,
                    "dollar_volume": 100.0,
                }
            )
        df = pl.DataFrame(rows).with_columns(pl.col("window_start").cast(pl.Datetime("ns")))
        out = forward_fill_ohlcv(df, is_extraday=True, k_days=1)
        dates = [t.date() for t in out["window_start"].to_list()]
        assert date(2024, 1, 1) in dates
        assert date(2024, 1, 2) in dates  # mardi
        assert date(2024, 1, 3) in dates
        assert date(2024, 1, 6) not in dates  # samedi hors plage
        tue = out.filter(pl.col("window_start").dt.date() == date(2024, 1, 2)).row(0, named=True)
        assert tue["close"] == 100.5
        assert tue["open"] == 100.5
        assert tue["volume"] == 0
        assert tue["candle_count"] == 0

    def test_does_not_invent_weekend(self):
        """Vendredi + lundi : pas de samedi / dimanche."""
        rows = []
        for d, price in ((date(2024, 1, 5), 10.0), (date(2024, 1, 8), 12.0)):
            # ven 5 jan 2024, lun 8 jan 2024
            rows.append(
                {
                    "window_start": datetime(d.year, d.month, d.day),
                    "session_end_date": d,
                    "ticker": "AAPL",
                    "open": price,
                    "high": price + 1,
                    "low": price - 1,
                    "close": price + 0.5,
                    "volume": 100,
                }
            )
        df = pl.DataFrame(rows).with_columns(pl.col("window_start").cast(pl.Datetime("ns")))
        out = forward_fill_ohlcv(df, is_extraday=True, k_days=1)
        dates = {t.date() for t in out["window_start"].to_list()}
        assert date(2024, 1, 6) not in dates
        assert date(2024, 1, 7) not in dates
        assert dates == {date(2024, 1, 5), date(2024, 1, 8)}
