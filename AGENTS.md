Tu es un expert Python senior. Maintiens et développe MyQuantStore, outil professionnel d'historisation périodique des données OHLCV multi-instruments.

### Objectifs principaux
- Récupérer et historiser les chandeliers OHLCV multi-instruments (futures, stocks, forex, indices ; options = scaffold).
- **Deux familles de timeframes / sources** (dual-source) :
  - **Intraday** : Massive.com, barre de base **1min** → resample query 2m/5m/1h/4h…
  - **Extraday** : Yahoo Finance (API chart `query1/2.finance.yahoo.com` via `curl_cffi`, pas yfinance/`fc.yahoo.com`), barre de base **1day** multi-type → resample 2d/1w…
- Utiliser **Polars** en priorité (Pandas uniquement si vraiment nécessaire).
- Tout le stockage se fait en **fichiers Parquet** (layout multi-type × multi-résolution).
- Caches : contrats futures + splits/dividends Massive (1min) ; `cache/yahoo_actions/` pour daily **stocks** only.
- Cascade type-aware **et par résolution** (query day → fetch 1day only).
- Fetch défaut : `--timeframe all` (1min + 1day Yahoo multi-type) ; `1min` | `1day` pour cibler.
- Mapping Yahoo : `tickers/yahoo_map.py` — stocks (`.`→`-`, skip `.WS`/`.U`), forex `=X`, indices `^`, futures continu `=F` ; overrides TOML.
- **Futures dual-track** : 1min = contrats Massive + rollover maison ; 1day = série continue Yahoo (`ES=F`) par root. Ne jamais croiser les deux pour reconstruire un agrégat.

### Configuration
- Système clair : pydantic-settings + tomllib (XDG ~/.config/myquantstore/ prioritaire, fallback repo).
- Fichiers :
  - ~/.config/myquantstore/.env (API key, jamais commité)
  - ~/.config/myquantstore/config.toml (instruments par type, fetch, storage, futures/stocks, logging, chart...)
- Paramètres clés configurables :
  - Instruments par type (futures = ["NQ", "ES", ...], stocks, forex, indices)
  - timeframe = "1min"
  - overlap_buffer_days
  - history_months par type (défaut 24, 60 pour indices)
  - days_before_expiry (futures rollover)
  - logging level (DEBUG par défaut)
  - data_dir, cache_dir, etc.
  - health : stale_lag_days_1min / stale_lag_days_1day / cross_resolution_lag_days

### Logique d'historisation
1. **Premier run** : récupérer depuis (today - history_months.<type>).
2. **Runs suivants** : depuis (dernière date agrégée - overlap_buffer_days).
3. Extension arrière automatique si history_months est augmenté.
4. À chaque exécution :
   - Sauvegarder un **dump pseudo-brut** (1 fichier par ticker + run_ts).
   - Mettre à jour l'agrégé.
5. **Définition "dump pseudo-brut"** :
   - Ce ne sont **pas** les réponses JSON brutes de l'API.
   - Ce sont les données API après normalisation minimale au format interne canonique (conversion timestamps ns/ms → Datetime[ns], normalisation champs, ajout colonnes d'identité symbol/instrument_type/product_code/run_id, casts volume→Int32 etc.).
   - Choix volontaire pour praticité et performance.
   - **Contrainte absolue (même en alpha)** : il doit toujours être possible de reconstruire l'agrégat complet à partir des dumps existants (read_all_runs + concat + dédup sur (window_start, ticker) + casts).

### Dumps & Stockage (multi-résolution)
- Layout raw : `data/raw/{type}/{symbol}/{ticker}/{resolution}/{run_ts}.parquet` (+ `.meta.json`)
- Layout aggregate : `data/aggregate/{type}/{symbol}/{resolution}.parquet` (+ `.meta.json`)
- Résolutions de stockage (barres de base) : `1min` (Massive), `1day` (Yahoo multi-type)
- Meta sidecar : inclut `resolution`, `source` (`massive` | `yahoo`)
- Pour futures 1min : ticker = contrat (ESM5 etc.) ; pour futures 1day : ticker = root (ES, série `=F`)
- Pour stocks/forex/indices : ticker = symbole
- Agrégation **par résolution** (pas de logique rollover dedans) : concat dumps de la résolution, dédup sur **`(window_start, ticker)`** keep=last, Categorical + Int32 casts.
- **Invariant** : l'agrégat d'une résolution se reconstruit uniquement depuis les dumps de **cette** résolution.
- Futures 1min : au jour de roll, l'agrégat **peut** avoir deux lignes au même `window_start` (deux `ticker`). Volontaire — clé naturelle = (timestamp, contrat).
- `query()` déduplique **par défaut** (`dedup_timestamps=True`) : une barre par timestamp, contrat le plus récent de la chaîne. `--no-dedup-timestamps` conserve les deux. Le chart utilise ce défaut (pas de `unique` côté chart).
- **Pas de resample 1min → day** en production (extraday = Yahoo only).

### Gestion des contrats et rollovers (futures)
- Cache /futures/v1/contracts intelligent (TTL, snapshots échelonnés pour contrats expirés).
- Rollover : days_before_expiry (défaut 7) → rollover_date = last_trade_date - N jours.
- Ex : contrat expire vendredi 19 → dernier jour conservé = vendredi 12.
- RolloverChain + RolloverSegment pour active_contract, continuous_segments, tick_size.
- Fetch 1min : `window_start.gte/lte` en `YYYY-MM-DD` inclusifs sur `active_from` / `active_until` (même date de roll) → recouvrement possible des deux contrats.
- Pour query : gaps naturels conservés ; timestamps dupliqués au roll **dédupliqués par défaut** (contrat le plus récent). `--no-dedup-timestamps` pour garder les deux.

### Corporate actions (stocks)
- **Massive 1min** : fetch `adjusted=false` → prix bruts ; cache `corporate_actions/`.
- **Yahoo 1day stocks** : le chart livre des OHLC **déjà split-adjusted** → désajustement à l'ingest
  (`reverse_split_adjustment`) pour stocker des bruts ; cache `yahoo_actions/`.
  Les events d'un fetch **incrémental** ne doivent **jamais** écraser ce cache
  (historique complet uniquement : period=max au 1er run, sinon refresh TTL dédié).
- **Yahoo 1day forex/indices/futures** : pas de corporate actions ; dump OHLC chart tel quel.
  Futures `=F` = continu Yahoo (souvent déjà back-adjusted côté Yahoo) stocké tel quel.
- Ajustement split appliqué **à la query** stocks (les deux résolutions) ; `--no-split` = bruts.
- Dividendes : facteurs calculés sur l'espace split-adjusted Yahoo ; `--adjust` à la query.
- Premier run Yahoo : toujours `period=max` (tous types) ; `history_months` = Massive only.

### Pipeline & Architecture
- Fetchers multi-type (FuturesFetcher, StocksFetcher, V2SingleSymbolFetcher, YahooDailyFetcher, OptionsFetcher scaffold).
- Cascade type-aware dans pipeline/cascade.py.
- Agrégateur générique (polars unique + casts).
- Query : reader + resampler + adjust (split).
- CLI complète + chart serveur (dashboard `/` multi-type, miniatures SVG 1day, charts `/{type}:{symbol}`).
- **`myquantstore serve`** : API HTTP `query()` (`/v1/health`, `/v1/instruments`, `/v1/query`) — Parquet / Arrow, pas de cascade, pas d'auth v1. Spec : `docs/TODO_SERVE.md`.
- **Portfolio MPT** (`analytics/`, CLI `portfolio`) : panel stocks 1day total-return, corr/cov, optim long-only equal|min-vol|max-sharpe, allocate (lots), frontier (Polars + numpy). Chart lazy `portfolio:max-sharpe`/`min-vol` (combo base puis resample, rebase 100).

### Onboarding & schedule (UX install)
- Templates embarqués : `src/myquantstore/resources/` (`config.minimal.toml`, `config.full.toml`, `env.example`) — accessibles hors clone via `importlib.resources`.
- **`myquantstore init`** : crée XDG config + `~/.local/share/myquantstore/{data,cache,logs}`, copie config minimale (défaut) ou `--full`, clé optionnelle (`-k` / prompt).
- **`myquantstore doctor`** : checks Python/config/dirs/clé/binary/schedule ; exit 1 si bloquant.
- **`setup-key --api-key KEY --yes`** : non-interactif.
- **`myquantstore schedule`** (backends `systemd` user timer + `cron`) :
  - `schedule run` = **fetch → aggregate → status --check** (aggregate pour régénérer le cache parquet consommé en externe).
  - Défaut horaires : samedi 01:00 (`OnCalendar=Sat *-*-* 01:00:00` / cron `0 1 * * 6`).
  - `install|uninstall|status|show` ; `--backend auto|systemd|cron` ; `--fetch-args`.
  - Templates manuels : `contrib/systemd/`, `contrib/cron/`.
  - Pas de daemon Python long-running.

### Logging & Observabilité
- DEBUG par défaut :
  - Tous les appels API (endpoint + params, clé masquée).
  - Skips grâce au cache.
  - Pagination : extrait des résultats (avec window_start) à chaque page.
- Retry Tenacity (429 avec Retry-After, 5xx backoff).
- **Fraîcheur OHLCV** (`storage/coverage.py`, config `[health]`) :
  - lag calendaire = `(today - max(window_start)).days` ; STALE si > `stale_lag_days_1min` (défaut 3) / `stale_lag_days_1day` (5).
  - Warn si `|lag_1min - lag_1day| > cross_resolution_lag_days` (défaut 7).
  - `status` affiche lag + STALE ; `status --check` exit 1 si problème (cron / `schedule run`).
  - Résumé `fetch` : `latest=` / `lag=` / `⚠ STALE` (warn only — pas de soft-skip ; utiliser `--force`).

### Tests & Qualité
- Tests pytest + respx (mocks API).
- Commentaires clairs et explicatifs.
- Structure propre (src/ layout, type hints stricts, ruff + mypy).
- **Avant tout gros backfill (2 ans+)** : tester obligatoirement la récupération fonctionnelle de l'historique complet d'un contrat entier (utiliser/améliorer scripts/test_single_contract.py) pour valider workflow, perf, pagination, etc.

### Contraintes techniques
- uv recommandé pour deps + env.
- Python >= 3.11.
- Polars + pyarrow prioritaires.
- Pas de pandas.
- Code maintenable, prêt pour review.

### Documentation
- https://massive.com/docs/llms.txt
- README.md, docs/TECHNICAL_DESIGN.md, docs/MULTI_TYPE.md, docs/PORTFOLIO.md
- **`myquantstore serve` est implémenté** (API query réseau, pas le chart) : `docs/TODO_SERVE.md` (hors v1 encore ouvert). Backtest hebdo = snapshot Parquet, pas cette API.
- **`--adjust` est implémenté** (futures back-adjusted rollover + stocks dividends après splits). Ne pas le documenter comme stub/NotImplemented.
- Maintenir AGENTS.md à jour (ce fichier est la source de vérité pour les consignes de dev).

### Notes alpha
- Version 0.1 alpha.
- Pas de garantie de rétrocompatibilité des formats de stockage ou layouts.
- La seule contrainte forte : pouvoir reconstruire les agrégats depuis les dumps pseudo-bruts existants.
- Pas de dump JSON brut supplémentaire (les dumps Parquet normalisés suffisent).

### Consignes pour les agents IA
- Lors de tout commit git, le message de commit **doit obligatoirement mentionner l'agent/modèle utilisé** (ex: "grok-build-0.1" ou "grok build 0.1").
- Avant de committer, s'assurer que AGENTS.md est à jour avec les nouvelles consignes pour les agents futurs.
- Toujours utiliser des messages de commit clairs décrivant les changements + référence à l'agent.

Commence/maintiens par : arborescence propre, pyproject.toml (uv/hatch), config (pydantic+toml), implémentation pipeline + fetchers + storage, tests.
