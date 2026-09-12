"""Tests du parseur d'overlays backtest v2 (``chart/overlay.py``).

Couvre : validation du contrat v2, fusion shared/params, aplatissement en
chemins pointés, canonicalisation de l'UT, détection des params saillants,
génération des labels, facettes, cache mtime et chargement d'un overlay.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from myquantstore.chart.overlay import (
    OverlayFormatError,
    build_catalog,
    clear_catalog_cache,
    load_overlay,
    scan_report,
    timeframe_label,
)


def _write(root: Path, stem: str, meta: dict[str, Any]) -> Path:
    back = root / "Backtests"
    back.mkdir(parents=True, exist_ok=True)
    path = back / f"{stem}.meta.json"
    path.write_text(json.dumps(meta), encoding="utf-8")
    return path


def _meta(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "mqs_overlay": 2,
        "backtest_type": "cth_factor",
        "instrument": "futures:YM",
        "timeframe": {"unit": "min", "nb": 1},
        "shared": {"ticksize": 1.0, "cth_open": 242, "session": {"tz": "America/Chicago"}},
        "backtests": {
            "short_20_35": {"params": {"is_short": True, "entry_factor": 20, "factor": 35}},
            "long_50_45": {"params": {"is_short": False, "entry_factor": 50, "factor": 45}},
        },
    }
    base.update(overrides)
    return base


def _rows(root: Path, product: str = "futures:YM") -> list[dict[str, Any]]:
    return build_catalog(root, product)["overlays"]


class TestTimeframe:
    @pytest.mark.parametrize(
        ("unit", "nb", "minutes", "label"),
        [
            ("min", 1, 1, "1min"),
            ("min", 15, 15, "15min"),
            ("hour", 4, 240, "4h"),
            ("day", 1, 1440, "1d"),
            ("week", 1, 10080, "1w"),
        ],
    )
    def test_canonicalisation(self, tmp_path, unit, nb, minutes, label):
        _write(tmp_path, "s", _meta(timeframe={"unit": unit, "nb": nb}))
        tf = _rows(tmp_path)[0]["timeframe"]
        assert tf == {"unit": unit, "nb": nb, "minutes": minutes}
        assert timeframe_label(tf) == label

    @pytest.mark.parametrize(
        "timeframe",
        [
            {"unit": "seconde", "nb": 1},
            {"unit": "min", "nb": 0},
            {"unit": "min", "nb": -5},
            {"unit": "min", "nb": True},
            {"unit": "min"},
            {"nb": 1},
            "1min",
            60,
        ],
    )
    def test_timeframe_invalide_rejette_le_fichier(self, tmp_path, timeframe):
        _write(tmp_path, "s", _meta(timeframe=timeframe))
        catalog = build_catalog(tmp_path, "futures:YM")
        assert catalog["overlays"] == []
        assert len(catalog["skipped"]) == 1
        assert "timeframe" in catalog["skipped"][0]["reason"]

    def test_timeframe_manquant_rejette(self, tmp_path):
        meta = _meta()
        del meta["timeframe"]
        _write(tmp_path, "s", meta)
        skipped = build_catalog(tmp_path, "futures:YM")["skipped"]
        assert "timeframe manquant" in skipped[0]["reason"]

    def test_override_par_entree(self, tmp_path):
        """Un sweep qui balaie aussi l'UT tient dans un seul fichier."""
        meta = _meta()
        meta["backtests"]["long_50_45"]["timeframe"] = {"unit": "hour", "nb": 1}
        _write(tmp_path, "s", meta)
        by_id = {row["id"]: row for row in _rows(tmp_path)}
        assert by_id["short_20_35"]["timeframe"]["minutes"] == 1
        assert by_id["long_50_45"]["timeframe"]["minutes"] == 60


class TestValidation:
    @pytest.mark.parametrize(
        ("meta", "fragment"),
        [
            ({"backtest_type": "x", "instrument": "futures:YM"}, "mqs_overlay"),
            (_meta(mqs_overlay=1), "mqs_overlay"),
            (_meta(mqs_overlay="2"), "mqs_overlay"),
            (_meta(backtest_type=""), "backtest_type"),
            (_meta(backtest_type=None), "backtest_type"),
            (_meta(instrument=""), "instrument"),
            (_meta(backtests={}), "backtests"),
            (_meta(backtests="nope"), "backtests"),
            (_meta(shared=[1, 2]), "shared"),
            (_meta(label_params="is_short"), "label_params"),
            (_meta(label_params=[1, 2]), "label_params"),
        ],
    )
    def test_contrat_v2(self, tmp_path, meta, fragment):
        _write(tmp_path, "s", meta)
        catalog = build_catalog(tmp_path, "futures:YM")
        assert catalog["overlays"] == []
        assert fragment in catalog["skipped"][0]["reason"]

    def test_json_invalide_est_liste_pas_fatal(self, tmp_path):
        back = tmp_path / "Backtests"
        back.mkdir(parents=True)
        (back / "casse.meta.json").write_text("{ pas du json", encoding="utf-8")
        _write(tmp_path, "bon", _meta())
        catalog = build_catalog(tmp_path, "futures:YM")
        assert len(catalog["overlays"]) == 2  # le fichier valide passe quand même
        assert len(catalog["skipped"]) == 1
        assert "JSON invalide" in catalog["skipped"][0]["reason"]

    def test_format_legacy_rejette_avec_raison_explicite(self, tmp_path):
        """Le producteur migre vers v2 : le legacy doit être signalé, pas ignoré en silence."""
        legacy = {"short_20_35_0": {"instrument": "futures:YM", "is_short": True}}
        _write(tmp_path, "legacy", legacy)
        catalog = build_catalog(tmp_path, "futures:YM")
        assert catalog["overlays"] == []
        assert catalog["skipped"] == [
            {"file": "legacy.meta.json", "reason": "mqs_overlay absent ou != 2 (trouvé None)"}
        ]

    def test_params_non_objet_rejette(self, tmp_path):
        _write(tmp_path, "s", _meta(backtests={"a": {"params": [1]}}))
        assert "params" in build_catalog(tmp_path, "futures:YM")["skipped"][0]["reason"]

    def test_entree_sans_params_acceptee(self, tmp_path):
        _write(tmp_path, "s", _meta(backtests={"nu": {}, "vide": None}))
        rows = _rows(tmp_path)
        assert {row["id"] for row in rows} == {"nu", "vide"}
        # Seuls les params partagés subsistent.
        assert rows[0]["params"]["ticksize"] == 1.0

    def test_dossier_absent_renvoie_catalogue_vide(self, tmp_path):
        catalog = build_catalog(tmp_path / "nulle-part", "futures:YM")
        assert catalog == {
            "overlays": [],
            "facets": {"types": [], "timeframes": [], "param_keys": {}},
            "skipped": [],
        }

    def test_overlay_dir_vide_renvoie_catalogue_vide(self, tmp_path):
        assert build_catalog("", "futures:YM")["overlays"] == []
        assert build_catalog(None, "futures:YM")["overlays"] == []


class TestParams:
    def test_shared_fusionne_et_entree_gagne(self, tmp_path):
        meta = _meta(shared={"ticksize": 1.0, "factor": 999})
        _write(tmp_path, "s", meta)
        by_id = {row["id"]: row for row in _rows(tmp_path)}
        assert by_id["short_20_35"]["params"]["ticksize"] == 1.0  # hérité
        assert by_id["short_20_35"]["params"]["factor"] == 35  # surchargé par l'entrée

    def test_aplatissement_en_chemins_pointes(self, tmp_path):
        _write(tmp_path, "s", _meta())
        params = _rows(tmp_path)[0]["params"]
        assert params["session.tz"] == "America/Chicago"
        assert "session" not in params

    def test_aplatissement_recursif(self, tmp_path):
        meta = _meta(shared={"a": {"b": {"c": 3}}})
        _write(tmp_path, "s", meta)
        assert _rows(tmp_path)[0]["params"]["a.b.c"] == 3

    def test_listes_stringifiees(self, tmp_path):
        meta = _meta(shared={"windows": [5, 10]})
        _write(tmp_path, "s", meta)
        assert _rows(tmp_path)[0]["params"]["windows"] == "[5, 10]"


class TestSalienceEtLabels:
    def test_params_invariants_exclus(self, tmp_path):
        _write(tmp_path, "s", _meta())
        row = _rows(tmp_path)[0]
        assert set(row["salient"]) == {"is_short", "entry_factor", "factor"}
        # Invariants du groupe : consultables au tooltip, absents du label.
        assert "ticksize" not in row["salient"]
        assert "session.tz" not in row["salient"]
        assert "ticksize" not in row["label"]

    def test_salience_par_type_pas_globale(self, tmp_path):
        """Chaque optimisation a ses propres discriminants."""
        _write(tmp_path, "cth", _meta())
        _write(
            tmp_path,
            "rsi",
            _meta(
                backtest_type="rsi",
                shared={"ticksize": 1.0},
                backtests={
                    "rsi_14": {"params": {"length": 14, "threshold": 70}},
                    "rsi_21": {"params": {"length": 21, "threshold": 70}},
                },
            ),
        )
        by_type: dict[str, list[dict[str, Any]]] = {}
        for row in _rows(tmp_path):
            by_type.setdefault(row["backtest_type"], []).append(row)
        # threshold est constant dans le groupe rsi → non saillant.
        assert by_type["rsi"][0]["salient"] == ["length"]
        assert set(by_type["cth_factor"][0]["salient"]) == {"is_short", "entry_factor", "factor"}

    def test_cle_absente_ailleurs_est_saillante(self, tmp_path):
        meta = _meta(
            backtests={
                "a": {"params": {"entry_factor": 1}},
                "b": {"params": {"entry_factor": 1, "bonus": 7}},
            }
        )
        _write(tmp_path, "s", meta)
        rows = {row["id"]: row for row in _rows(tmp_path)}
        assert "bonus" in rows["b"]["salient"]
        assert "entry_factor" not in rows["b"]["salient"]  # constant partout

    def test_label_structure_type_ut_params(self, tmp_path):
        _write(tmp_path, "s", _meta())
        by_id = {row["id"]: row for row in _rows(tmp_path)}
        label = by_id["short_20_35"]["label"]
        assert label.startswith("cth_factor · 1min · ")
        assert "entry_factor=20" in label
        assert "factor=35" in label

    def test_booleens_en_nom_nu(self, tmp_path):
        _write(tmp_path, "s", _meta())
        by_id = {row["id"]: row for row in _rows(tmp_path)}
        assert " · short · " in by_id["short_20_35"]["label"]
        assert " · ¬short · " in by_id["long_50_45"]["label"]

    def test_label_params_override(self, tmp_path):
        _write(tmp_path, "s", _meta(label_params=["factor", "ticksize"]))
        row = {r["id"]: r for r in _rows(tmp_path)}["short_20_35"]
        assert row["salient"] == ["factor", "ticksize"]
        assert row["label"] == "cth_factor · 1min · factor=35 · ticksize=1"
        assert "entry_factor" not in row["label"]

    def test_label_params_inconnu_ignore(self, tmp_path):
        _write(tmp_path, "s", _meta(label_params=["factor", "jamais_vu"]))
        row = {r["id"]: r for r in _rows(tmp_path)}["short_20_35"]
        assert row["salient"] == ["factor"]

    def test_repli_sur_id_si_aucun_saillant(self, tmp_path):
        _write(tmp_path, "s", _meta(backtests={"solo": {"params": {"entry_factor": 1}}}))
        row = _rows(tmp_path)[0]
        assert row["salient"] == []
        assert row["label"] == "cth_factor · 1min · solo"

    def test_floats_sans_zeros_inutiles(self, tmp_path):
        meta = _meta(
            backtests={
                "a": {"params": {"seuil": 1.0}},
                "b": {"params": {"seuil": 0.25}},
            }
        )
        _write(tmp_path, "s", meta)
        labels = {row["id"]: row["label"] for row in _rows(tmp_path)}
        assert labels["a"].endswith("seuil=1")
        assert labels["b"].endswith("seuil=0.25")


class TestCatalogue:
    def test_une_ligne_par_backtest_avec_cle_stable(self, tmp_path):
        _write(tmp_path, "YM_242", _meta())
        rows = _rows(tmp_path)
        assert len(rows) == 2
        assert {row["key"] for row in rows} == {
            "YM_242|short_20_35",
            "YM_242|long_50_45",
        }

    def test_filtre_par_instrument(self, tmp_path):
        _write(tmp_path, "ym", _meta())
        _write(tmp_path, "es", _meta(instrument="futures:ES"))
        assert {row["instrument"] for row in _rows(tmp_path, "futures:YM")} == {"futures:YM"}
        assert len(_rows(tmp_path, "futures:ES")) == 2
        assert _rows(tmp_path, "futures:NQ") == []

    def test_instrument_override_par_entree(self, tmp_path):
        meta = _meta()
        meta["backtests"]["long_50_45"]["instrument"] = "futures:ES"
        _write(tmp_path, "s", meta)
        assert len(_rows(tmp_path, "futures:YM")) == 1
        assert len(_rows(tmp_path, "futures:ES")) == 1

    def test_tri_deterministe(self, tmp_path):
        _write(tmp_path, "b", _meta(backtest_type="zeta"))
        _write(tmp_path, "a", _meta(backtest_type="alpha", timeframe={"unit": "hour", "nb": 1}))
        rows = _rows(tmp_path)
        assert [row["backtest_type"] for row in rows] == ["alpha", "alpha", "zeta", "zeta"]

    def test_facettes(self, tmp_path):
        _write(tmp_path, "cth", _meta())
        _write(
            tmp_path,
            "rsi",
            _meta(
                backtest_type="rsi",
                timeframe={"unit": "min", "nb": 15},
                shared={},
                backtests={"r": {"params": {"length": 14, "actif": True}}},
            ),
        )
        facets = build_catalog(tmp_path, "futures:YM")["facets"]
        assert facets["types"] == [
            {"name": "cth_factor", "count": 2},
            {"name": "rsi", "count": 1},
        ]
        assert [tf["minutes"] for tf in facets["timeframes"]] == [1, 15]
        assert facets["param_keys"]["entry_factor"]["kind"] == "number"
        assert facets["param_keys"]["entry_factor"]["values"] == [20, 50]
        assert facets["param_keys"]["actif"]["kind"] == "bool"
        assert facets["param_keys"]["session.tz"]["kind"] == "string"
        assert facets["param_keys"]["entry_factor"]["truncated"] is False

    def test_facettes_valeurs_plafonnees(self, tmp_path):
        meta = _meta(
            shared={},
            backtests={f"b{i}": {"params": {"n": i}} for i in range(80)},
        )
        _write(tmp_path, "s", meta)
        facet = build_catalog(tmp_path, "futures:YM")["facets"]["param_keys"]["n"]
        assert len(facet["values"]) == 50
        assert facet["truncated"] is True


class TestCache:
    def test_reutilise_sans_relecture(self, tmp_path, monkeypatch):
        _write(tmp_path, "s", _meta())
        assert len(_rows(tmp_path)) == 2

        # Un parsing supplémentaire ferait échouer le test : le cache doit servir.
        import myquantstore.chart.overlay as overlay_mod

        def _boom(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("le cache aurait dû éviter ce parsing")

        monkeypatch.setattr(overlay_mod, "_parse_meta", _boom)
        assert len(_rows(tmp_path)) == 2

    def test_invalide_sur_modification(self, tmp_path):
        path = _write(tmp_path, "s", _meta())
        assert len(_rows(tmp_path)) == 2

        meta = _meta()
        meta["backtests"]["troisieme"] = {"params": {"is_short": True, "entry_factor": 99}}
        path.write_text(json.dumps(meta), encoding="utf-8")
        # mtime_ns + taille changent → empreinte différente → rescan.
        assert len(_rows(tmp_path)) == 3

    def test_invalide_sur_ajout_de_fichier(self, tmp_path):
        _write(tmp_path, "s", _meta())
        assert len(_rows(tmp_path)) == 2
        _write(tmp_path, "autre", _meta(backtest_type="rsi"))
        assert len(_rows(tmp_path)) == 4

    def test_clear_catalog_cache(self, tmp_path):
        _write(tmp_path, "s", _meta())
        assert len(_rows(tmp_path)) == 2
        clear_catalog_cache()
        assert len(_rows(tmp_path)) == 2


class TestLoadOverlay:
    def test_payload_enrichi(self, tmp_path):
        _write(tmp_path, "s", _meta())
        payload = load_overlay(tmp_path, "s", "short_20_35")
        assert payload["id"] == "short_20_35"
        assert payload["key"] == "s|short_20_35"
        assert payload["backtest_type"] == "cth_factor"
        assert payload["timeframe"]["minutes"] == 1
        assert payload["params"]["entry_factor"] == 20
        assert set(payload["ids"]) == {"short_20_35", "long_50_45"}
        assert payload["transactions"] == []
        assert payload["orders"] == []

    def test_premier_id_par_defaut(self, tmp_path):
        _write(tmp_path, "s", _meta())
        assert load_overlay(tmp_path, "s")["id"] == "short_20_35"

    def test_id_inconnu(self, tmp_path):
        _write(tmp_path, "s", _meta())
        with pytest.raises(KeyError):
            load_overlay(tmp_path, "s", "jamais_vu")

    def test_stem_inconnu(self, tmp_path):
        _write(tmp_path, "s", _meta())
        with pytest.raises(FileNotFoundError):
            load_overlay(tmp_path, "absent")

    @pytest.mark.parametrize("stem", ["../secret", "a/b", "a b", "..", "wow$"])
    def test_stem_invalide_rejette(self, tmp_path, stem):
        _write(tmp_path, "s", _meta())
        with pytest.raises(ValueError):
            load_overlay(tmp_path, stem)

    def test_format_invalide_leve_overlay_format_error(self, tmp_path):
        _write(tmp_path, "s", _meta(mqs_overlay=1))
        with pytest.raises(OverlayFormatError):
            load_overlay(tmp_path, "s")

    def test_sans_dossier(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_overlay(tmp_path / "nulle-part", "s")


class TestScanReport:
    def test_regroupe_par_produit(self, tmp_path):
        _write(tmp_path, "ym", _meta())
        _write(tmp_path, "es", _meta(instrument="futures:ES"))
        _write(tmp_path, "legacy", {"a": {"instrument": "futures:YM"}})
        report = scan_report(tmp_path)
        assert report["exists"] is True
        assert [block["product"] for block in report["products"]] == ["futures:ES", "futures:YM"]
        assert len(report["products"][1]["overlays"]) == 2
        assert len(report["skipped"]) == 1

    def test_dossier_absent(self, tmp_path):
        report = scan_report(tmp_path / "nulle-part")
        assert report["exists"] is False
        assert report["products"] == []
