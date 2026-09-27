"""Tests du module contracts/rollover.py (RolloverChain)."""

from __future__ import annotations

from datetime import date

import polars as pl

from myquantstore.contracts.rollover import RolloverChain


class TestRolloverChainConstruction:
    """Tests de la construction de la RolloverChain."""

    def test_build_chain(self, sample_contracts_df):
        """La chaîne est construite avec 3 segments pour 3 contrats."""
        chain = RolloverChain("ES", sample_contracts_df, days_before_expiry=7)

        assert len(chain) == 3
        assert chain.segments[0].ticker == "ESH5"
        assert chain.segments[1].ticker == "ESM5"
        assert chain.segments[2].ticker == "ESU5"

    def test_rollover_date_calculation(self, sample_chain):
        """rollover_date = last_trade_date - days_before_expiry."""
        # ESH5 : last_trade_date = 2025-03-14, rollover = 2025-03-14 - 7 = 2025-03-07
        seg = sample_chain.segment_for_ticker("ESH5")
        assert seg.rollover_date == date(2025, 3, 7)

        # ESM5 : last_trade_date = 2025-06-13, rollover = 2025-06-13 - 7 = 2025-06-06
        seg = sample_chain.segment_for_ticker("ESM5")
        assert seg.rollover_date == date(2025, 6, 6)

    def test_active_from_chaining(self, sample_chain):
        """active_from du segment N+1 = jour ouvré suivant le rollover_date du segment N."""
        # Premier segment : active_from = first_trade_date
        assert sample_chain.segments[0].active_from == date(2024, 12, 16)

        # Deuxième segment : rollover du premier = ven. 2025-03-07 → lun. 2025-03-10
        assert sample_chain.segments[0].active_until == date(2025, 3, 10)
        assert sample_chain.segments[1].active_from == date(2025, 3, 10)

        # Troisième segment : rollover du deuxième = ven. 2025-06-06 → lun. 2025-06-09
        assert sample_chain.segments[1].active_until == date(2025, 6, 9)
        assert sample_chain.segments[2].active_from == date(2025, 6, 9)

    def test_empty_contracts(self):
        """Une chaîne avec des contrats vides a 0 segment."""
        chain = RolloverChain("ES", pl.DataFrame(), days_before_expiry=7)
        assert len(chain) == 0

    def test_filter_combo_contracts(self):
        """Les contrats de type 'combo' sont ignorés."""
        df = pl.DataFrame(
            {
                "ticker": ["ESH5", "ES_SPREAD"],
                "first_trade_date": [date(2024, 12, 16), date(2024, 12, 16)],
                "last_trade_date": [date(2025, 3, 14), date(2025, 3, 14)],
                "settlement_date": [date(2025, 3, 14), date(2025, 3, 14)],
                "trade_tick_size": [0.25, 0.25],
                "type": ["single", "combo"],
                "product_code": ["ES", "ES"],
            }
        )
        chain = RolloverChain("ES", df, days_before_expiry=7)
        assert len(chain) == 1  # seulement le contrat "single"
        assert chain.segments[0].ticker == "ESH5"


class TestActiveContract:
    """Tests de active_contract()."""

    def test_active_contract_in_first_segment(self, sample_chain):
        """Date dans le premier segment → ESH5."""
        assert sample_chain.active_contract(date(2025, 1, 15)) == "ESH5"

    def test_active_contract_in_second_segment(self, sample_chain):
        """Date dans le deuxième segment → ESM5."""
        assert sample_chain.active_contract(date(2025, 4, 15)) == "ESM5"

    def test_active_contract_at_rollover_boundary(self, sample_chain):
        """Le rollover_date est le dernier jour du contrat courant.

        Rollover de ESM5 = ven. 2025-06-06 : ESM5 est encore actif ce jour-là,
        ESU5 prend le relais au jour ouvré suivant (lun. 2025-06-09).
        """
        assert sample_chain.active_contract(date(2025, 6, 6)) == "ESM5"
        assert sample_chain.active_contract(date(2025, 6, 9)) == "ESU5"

    def test_active_contract_before_first_segment(self, sample_chain):
        """Date avant le premier segment → premier contrat."""
        assert sample_chain.active_contract(date(2024, 1, 1)) == "ESH5"

    def test_active_contract_after_last_segment(self, sample_chain):
        """Date après le dernier segment → dernier contrat."""
        assert sample_chain.active_contract(date(2026, 1, 1)) == "ESU5"

    def test_empty_chain_returns_none(self):
        """Une chaîne vide retourne None."""
        chain = RolloverChain("ES", pl.DataFrame(), days_before_expiry=7)
        assert chain.active_contract(date(2025, 1, 1)) is None


class TestTickSize:
    """Tests de tick_size_for_ticker()."""

    def test_tick_size_for_known_ticker(self, sample_chain):
        """tick_size_for_ticker retourne la bonne valeur."""
        assert sample_chain.tick_size_for_ticker("ESH5") == 0.25
        assert sample_chain.tick_size_for_ticker("ESM5") == 0.25

    def test_tick_size_for_unknown_ticker(self, sample_chain):
        """tick_size_for_ticker retourne 0.0 pour un ticker inconnu."""
        assert sample_chain.tick_size_for_ticker("UNKNOWN") == 0.0


class TestContinuousSegments:
    """Tests de continuous_segments()."""

    def test_segments_covering_period(self, sample_chain):
        """continuous_segments retourne les segments chevauchant la période."""
        segments = sample_chain.continuous_segments(
            date(2025, 3, 1),  # chevauche ESH5 et ESM5
            date(2025, 4, 1),
        )
        tickers = [s.ticker for s in segments]
        assert "ESH5" in tickers
        assert "ESM5" in tickers

    def test_segments_single_period(self, sample_chain):
        """continuous_segments avec une période courte retourne 1 segment."""
        segments = sample_chain.continuous_segments(
            date(2025, 1, 1),
            date(2025, 1, 15),
        )
        assert len(segments) == 1
        assert segments[0].ticker == "ESH5"


class TestToTable:
    """Tests de to_table()."""

    def test_to_table_returns_dataframe(self, sample_chain):
        """to_table retourne un DataFrame avec les bonnes colonnes."""
        table = sample_chain.to_table()

        assert isinstance(table, pl.DataFrame)
        assert table.height == 3
        assert "ticker" in table.columns
        assert "first_trade_date" in table.columns
        assert "last_trade_date" in table.columns
        assert "rollover_date" in table.columns
        assert "active_from" in table.columns
        assert "active_until" in table.columns
        assert "trade_tick_size" in table.columns

    def test_to_table_empty_chain(self):
        """to_table sur une chaîne vide retourne un DataFrame vide."""
        chain = RolloverChain("ES", pl.DataFrame(), days_before_expiry=7)
        table = chain.to_table()
        assert table.is_empty()


class TestRolloverExample:
    """Exemple de la documentation : contrat expirant le vendredi 18/09/2026."""

    def test_friday_18_example(self):
        """Expiration ven. 18 → rollover_date ven. 11 (encore l'ancien), nouveau dès lun. 14."""
        df = pl.DataFrame(
            {
                "ticker": ["YMU6", "YMZ6"],
                "first_trade_date": [date(2026, 1, 1), date(2026, 5, 1)],
                "last_trade_date": [date(2026, 9, 18), date(2026, 12, 18)],
                "settlement_date": [date(2026, 9, 18), date(2026, 12, 18)],
                "trade_tick_size": [1.0, 1.0],
                "type": ["single", "single"],
            }
        )

        chain = RolloverChain("YM", df, days_before_expiry=7)

        seg_u = chain.segment_for_ticker("YMU6")
        assert seg_u.rollover_date == date(2026, 9, 11)
        assert seg_u.active_until == date(2026, 9, 14)
        assert chain.segment_for_ticker("YMZ6").active_from == date(2026, 9, 14)

        assert chain.active_contract(date(2026, 9, 10)) == "YMU6"
        assert chain.active_contract(date(2026, 9, 11)) == "YMU6"
        assert chain.active_contract(date(2026, 9, 13)) == "YMU6"
        assert chain.active_contract(date(2026, 9, 14)) == "YMZ6"

    def test_midweek_expiry_switches_next_day(self):
        """Expiration un mercredi → rollover_date mercredi, bascule le jeudi."""
        df = pl.DataFrame(
            {
                "ticker": ["CLV6", "CLX6"],
                "first_trade_date": [date(2026, 1, 1), date(2026, 2, 1)],
                "last_trade_date": [date(2026, 9, 23), date(2026, 10, 20)],
                "settlement_date": [date(2026, 9, 23), date(2026, 10, 20)],
                "trade_tick_size": [0.01, 0.01],
                "type": ["single", "single"],
            }
        )

        chain = RolloverChain("CL", df, days_before_expiry=7)

        seg = chain.segment_for_ticker("CLV6")
        assert seg.rollover_date == date(2026, 9, 16)  # mercredi
        assert seg.active_until == date(2026, 9, 17)  # jeudi
        assert chain.active_contract(date(2026, 9, 16)) == "CLV6"
        assert chain.active_contract(date(2026, 9, 17)) == "CLX6"
