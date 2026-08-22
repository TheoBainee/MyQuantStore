# TODO — `myquantstore serve` (API query réseau)

Statut : **fait (v1)**. Reprise d'une discussion (2026-08) : interfaçage backtest
weekend NQ/ES + consommation depuis un autre langage / une autre machine.

Implémenté : `src/myquantstore/serve/`, CLI `myquantstore serve`, tests
`tests/test_serve.py`. Chart et `schedule` inchangés. Hors v1 : voir §4.

Le backtest hebdo **n'attend pas** cette API. Il utilise un snapshot Parquet
(voir §0). `serve` est un produit à part, pour le query ad-hoc sur le réseau.

---

## 0. Contexte — ce qui est déjà en place (ne pas casser)

### Dual-track / query

- Agrégat = fusion de dumps, clé `(window_start, ticker)`. Au roll 1min, deux
  contrats peuvent partager le même timestamp (volontaire).
- `query()` est la couche conso : resample, splits/div, Panama `--adjust`,
  **dédup timestamps ON par défaut** (`dedup_timestamps=True` ; le contrat le
  plus récent de la `RolloverChain` gagne). `--no-dedup-timestamps` conserve
  les deux. Après `--adjust` et le bilan tick size, avant normalize/resample.
- Le chart s'appuie sur ce défaut (plus de `unique` dans `_prepare_chart_df`).
- Package : `__init__.py` n'exporte que `__version__`. Pas de SDK public.
- `schedule run` / `schedule run fetch` = fetch → aggregate → `status --check`.
  `schedule run caches` = tickers + contrats (indépendant). Pas de daemon.
  `schedule install` **écrase** `~/.config/systemd/user/myquantstore-fetch.service`
  (le job caches a ses propres units `myquantstore-caches.*`).

### Snapshot backtest (hors myquantstore — déjà la recette)

Feed du backtest Python / même machine : **pas** `import query()`, **pas**
l'API chart, **pas** `query` dans `schedule run`.

```
sam. 07:00  myquantstore schedule run
     └── OnSuccess (drop-in systemd, survit à schedule install)
            query NQ|ES --timescale-unit min --no-cascade --output …
            backtest lit le Parquet
```

- Script : `~/bin/mqs-snapshot.sh` →
  `~/.local/share/mybacktest/snapshots/YYYY-MM-DD/{nq,es}_1min.parquet`
- Unit : `~/.config/systemd/user/mqs-snapshot.service` (oneshot, `After=` fetch)
- Drop-in : `~/.config/systemd/user/myquantstore-fetch.service.d/snapshot.conf`
  → `[Unit] OnSuccess=mqs-snapshot.service`
- Raw 1min, pas de `--adjust`. Dédup roll = défaut `query`. `--no-cascade`.
- Si `status --check` exit 1 → pas d'`OnSuccess`.
- Cron : wrapper `schedule run && mqs-snapshot.sh`.
- Ne pas écrire dans `data/aggregate/` (écrasé chaque samedi).

---

## 1. Objectif de `serve`

Exposer **`query()`** en HTTP pour un client quelconque (autre langage, autre
machine, notebook) **sans** partager `data_dir` ni importer le package.

Ce n'est **pas** :

- le serveur chart (`/api/candles` = lazy UI, Arrow, `tail`, buffer, flags chart) ;
- un remplacement du snapshot hebdo (le backtest reste sur fichiers) ;
- un daemon livré par `schedule` (contrainte : pas de long-running dans le schedule).

---

## 2. CLI

```
myquantstore serve [--host] [--port]
```

- uvicorn oneshot. `--host` / `--port` absents → `[serve].host` / `[serve].port`
  (défauts 127.0.0.1:8741). LAN explicite via `--host` ou `host` en conf.
- bind **localhost** par défaut.
- Pas d'unité systemd installée par défaut.
- Pas d'auth en v1 (LAN only).
- **Jamais** de cascade / fetch depuis l'API : agrégat absent → 404.

Réutiliser le mapping CLI → `query()` déjà dans `cli.py`
(`_timescale_to_query_params`, `_cmd_query`) plutôt que de le dupliquer.
Le chart (`chart/server.py`) **ne change pas**.

---

## 3. Endpoints v1

| Méthode | Path | Rôle |
|---|---|---|
| `GET` | `/v1/health` | `assess_instrument_health` (tous, ou `?instrument=NQ` / `?instrument=futures:NQ`). HTTP 200 si OK, **503** si `has_problems`. Corps JSON : lag, STALE, issues. |
| `GET` | `/v1/instruments` | Config + résolutions. Futures : `trade_tick_size`, `tickers`, `current_ticker`, `last_trade_date`, `days_to_maturity` (cache local). |
| `GET` | `/v1/query` | Équivalent `myquantstore query` / `query()`. |

### `/v1/query` — paramètres (query string)

Alignés sur `query()` / la CLI :

| Param | Défaut | Notes |
|---|---|---|
| `instrument` | requis | Symbole ou `type:symbol` |
| `type` | None | Si symbole ambigu |
| `start` / `end` | None | `YYYY-MM-DD` ou ISO datetime |
| `timescale_unit` | `min` | `min` \| `hour` \| `day` \| `week` |
| `timescale_nb` | `1` | |
| `adjust` | false | Futures Panama / stocks dividends |
| `no_split` | false | Stocks bruts |
| `dedup_timestamps` | **true** | false = deux tickers au roll |
| `intraday_begin` / `intraday_end` | None | `HH:MM`, les deux ou aucun |
| `normalize_tick_size` | false | Incompatible avec `adjust` → 400. HTTP : `true`/`false`. |
| `include_cols` | None | CSV de colonnes. Toute colonne absente → 400. |

Pas de `limit` « head oldest » (piège actuel de `query()`). Si un plafond est
utile plus tard : `tail` des N plus récentes, documenté comme tel — ou rien en v1.

### `/v1/query` — réponse

- Défaut : **Parquet** (`Content-Type: application/vnd.apache.parquet`) =
  mêmes bytes que `query --output`.
- Optionnel : `Accept: application/vnd.apache.arrow.stream` (IPC).
- 400 validation (params incompatibles, timescale invalide).
- 404 agrégat / instrument absent.
- 503 seulement sur `/v1/health`, pas un refus silencieux de `/v1/query`
  (un client peut vouloir relire des données STALE). Documenter que le
  consommateur sérieux appelle health d'abord.

---

## 4. Hors v1 (ne pas faire au premier passage)

- Auth, TLS, rate-limit.
- Cascade / fetch / `--force` via l'API.
- Mélanger les routes avec `/api/candles` du chart.
- Timer / unit systemd `serve` livré par `schedule install`.
- CSV.
- Snapshot / `export` daté (le script `mqs-snapshot.sh` suffit ; une CLI
  `myquantstore export` n'est qu'un sucre éventuel, autre ticket).
- Multiplier / specs contrats (reste côté moteur de backtest).
- Écriture atomique des agrégats (autre amélioration, utile si lecture
  concurrente pendant `aggregate` ; le snapshot tourne *après* schedule).

---

## 5. Fichiers probables à l'implémentation

- `src/myquantstore/serve/` (app FastAPI séparée de `chart/server.py`)
- Branche CLI dans `cli.py` (`serve`)
- Tests respx-free : `TestClient` + agrégat seedé (comme `test_chart_server.py`)
- Doc : ce fichier → section README + `TECHNICAL_DESIGN` quand c'est fait
- **Ne pas** documenter `--adjust` comme stub (déjà implémenté)

---

## 6. Critères de done

1. `myquantstore serve` démarre ; `GET /v1/query?instrument=ES` renvoie un
   Parquet relisible par `pl.read_parquet` / pyarrow / un client non-Python.
2. `dedup_timestamps=true` par défaut ; `false` renvoie 2 lignes au même
   `window_start` si l'agrégat a le recouvrement de roll.
3. Aucun appel réseau Massive/Yahoo depuis `serve`.
4. Chart et `schedule` inchangés.
5. Tests : health 200/503, query 200/400/404, Parquet round-trip NQ/ES 1min.
