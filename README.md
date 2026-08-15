# MyQuantStore

Historisation périodique des données OHLCV multi-instruments via l'API REST de [Massive.com](https://massive.com).

MyQuantStore supporte les **5 types d'instruments** de Massive : **futures**, **stocks**, **forex**, **indices** et **options**. À ce jour, **futures**, **stocks**, **forex** et **indices** sont pleinement implémentés ; **options** est scaffoldé (`NotImplementedError`).

## Dual-source (intraday + extraday)

Deux familles de timeframes / sources, **sans se croiser** pour reconstruire un agrégat :

| Famille | Source | Barre stockée | Resample à la query |
|---|---|---|---|
| **Intraday** | Massive.com REST | **1min** | 2m, 5m, 1h, 4h… |
| **Extraday** | Yahoo Finance chart (`curl_cffi`, pas yfinance) | **1day** multi-type | 2d, 1w… |

- **Futures dual-track** : 1min = contrats Massive + rollover maison ; 1day = série continue Yahoo (`ES=F`) par root. Ne jamais croiser les deux.
- **Stocks** : 1min Massive en prix bruts (`adjusted=false`) ; 1day Yahoo désajusté splits à l'ingest puis re-ajusté à la query (toggle `--no-split` / `--adjust` dividends).
- Fetch défaut : `--timeframe all` (1min + 1day) ; cibler avec `1min` ou `1day`.
- Mapping Yahoo : `tickers/yahoo_map.py` (stocks `.`→`-`, forex `=X`, indices `^`, futures `=F`).

## Fonctionnalités

- **Multi-type** : futures (rollover + contrats), stocks (splits/dividends), forex, indices, options — dispatch automatique par type d'instrument.
- Récupération et historisation **chaque semaine** des chandeliers OHLCV 1 minute.
- Stockage en **fichiers Parquet** via **Polars** (types `Categorical` optimisés), layout multi-type × multi-résolution : `data/{raw,aggregate}/{type}/{symbol}/…/{resolution}/…` (`1min` Massive, `1day` Yahoo multi-type).
- **Dumps pseudo-bruts** : les réponses API sont normalisées au format interne canonique (timestamps, champs, colonnes d'identité) avant écriture dans `data/raw/` — suffisants pour reconstruire intégralement les agrégats (pas de dump JSON brut).
- **Ajustement split** pour stocks : stockage en prix **bruts** (`adjusted=false`) + ajustement à la query (toggle `--no-split`, splits ON par défaut via le cache `/stocks/v1/splits`).
- Mise en cache intelligente : contrats futures (`/futures/v1/contracts`) et corporate actions stocks (`/stocks/v1/splits`), TTL commun configurable.
- Gestion automatique du **rollover** des contrats futures (switch J-7 avant expiration) via la `RolloverChain`.
- **Cascade automatique** des dépendances (type-aware) : `query` déclenche `aggregate` → `fetch` → `contracts`/`splits` si nécessaire.
- Normalisation des prix en **multiples entiers de tick size** (`Int32`) via `--normalize-tick-size` (futures).
- Test de qualité des données via `--check-ticksize-accuracy` (bilan par ticker, futures).
- Sidecar `.meta.json` systématique sur tous les fichiers Parquet (métadonnées, traçabilité).
- Retry automatique (Tenacity) sur 429/5xx avec `Retry-After` et exponential backoff.
- Logging DEBUG détaillé (appels API, skips cache, extraits pagination).

## Prérequis

- Python ≥ 3.11
- [pipx](https://pipx.pypa.io/) ou [uv](https://docs.astral.sh/uv/) pour le binaire global (recommandé)
- Clé API [Massive.com](https://massive.com) pour le track **1min** (le **1day** Yahoo fonctionne sans)

## Quickstart (5 minutes)

**Sans cloner** (binaire isolé, install figée) :

```bash
pipx install git+https://github.com/TheoBainee/MyQuantStore.git
# équivalent uv :
# uv tool install git+https://github.com/TheoBainee/MyQuantStore.git

myquantstore init
myquantstore doctor
myquantstore fetch --dry-run
myquantstore fetch
myquantstore schedule install     # samedi 01h00 — systemd user ou cron (auto)
myquantstore status
```

**Depuis un clone** :

```bash
git clone https://github.com/TheoBainee/MyQuantStore.git
cd MyQuantStore
pipx install .                    # install normale (pas --editable)
myquantstore init && myquantstore doctor
```

- `init` crée `~/.config/myquantstore/config.toml` (profil **minimal** par défaut).
- `init --full` copie l'exemple multi-type (futures/forex/stocks/indices) — **backfill beaucoup plus lourd**.
- Clé non interactive : `myquantstore setup-key --api-key YOUR_KEY --yes` ou `init -k YOUR_KEY`.
- Mise à jour plus tard : `pipx reinstall myquantstore` ou `pipx install --force git+https://github.com/TheoBainee/MyQuantStore.git`.

## Installation détaillée

### Usage quotidien (binaire global)

Install **non-editable** : le code est copié dans l'environnement pipx/uv ; les changements locaux au dépôt ne sont **pas** pris en compte tant qu'on ne réinstalle pas.

```bash
# Depuis GitHub
pipx install git+https://github.com/TheoBainee/MyQuantStore.git
# ou: uv tool install git+https://github.com/TheoBainee/MyQuantStore.git

# Depuis un clone local
cd MyQuantStore && pipx install .

myquantstore --help

# Upgrade
pipx reinstall myquantstore
# ou: pipx install --force git+https://github.com/TheoBainee/MyQuantStore.git
```

### Développement (contribuer / tester)

L’install **editable** (`-e`) est réservée aux contributeurs : le binaire pointe vers les sources du clone.

```bash
git clone https://github.com/TheoBainee/MyQuantStore.git
cd MyQuantStore
uv venv .venv
source .venv/bin/activate        # Linux / macOS
uv pip install -e ".[dev]"
myquantstore --help
```

> **Sans uv** : `python -m venv .venv` puis `pip install -e ".[dev]"`.  
> Alternative pipx en dev uniquement : `pipx install --editable .` (recharger via réinstall si besoin).

## Configuration

La config suit le [XDG Base Directory](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html) :

| Fichier | Emplacement principal | Fallback dev |
|---|---|---|
| Secrets | `~/.config/myquantstore/.env` | `./.env` |
| Config métier | `~/.config/myquantstore/config.toml` | `./config.toml` |
| Données / cache / logs | `~/.local/share/myquantstore/{data,cache,logs}` | configurable |

```bash
myquantstore init                 # recommandé
# ou manuel: cp config.toml.example ~/.config/myquantstore/config.toml
myquantstore setup-key            # ou setup-key -k KEY --yes
myquantstore config
myquantstore config --paths
```

> **Migration depuis MassiVibe** : renommez `~/.config/massivibe` → `myquantstore` et
> `~/.local/share/massivibe` → `myquantstore` (Parquet + config compatibles).

Voir `docs/TECHNICAL_DESIGN.md` et `docs/MULTI_TYPE.md` pour le détail des paramètres.

## Automatisation (schedule)

Le job périodique exécute dans l'ordre :

1. **`fetch`** — dumps + mise à jour agrégat côté historian
2. **`aggregate`** — régénère le cache Parquet `data/aggregate/` (pour brancher des traitements externes directement sur l'agrégat). Futures 1min : clé `(window_start, ticker)` — au roll, deux contrats peuvent partager le même timestamp. `query` déduplique par défaut (`--no-dedup-timestamps` pour garder les deux ; voir `docs/TECHNICAL_DESIGN.md` §8.6).
3. **`status --check`** — exit 1 si données STALE / problème de fraîcheur (idéal monitoring)

```bash
myquantstore schedule install                    # auto: systemd user si dispo, sinon cron
myquantstore schedule install --backend systemd  # samedi 01:00 (OnCalendar)
myquantstore schedule install --backend cron --when '0 1 * * 6'
myquantstore schedule install --fetch-args '--no-cascade'
myquantstore schedule status
myquantstore schedule run                        # exécution manuelle du job
myquantstore schedule show                       # preview units / crontab
myquantstore schedule uninstall
```

- **systemd user** : units dans `~/.config/systemd/user/myquantstore-fetch.{service,timer}`.
  Si la machine est souvent hors session graphique : `loginctl enable-linger $USER`.
  `Persistent=true` rattrape un run manqué (machine éteinte).
- **cron** : bloc marqué `# BEGIN MYQUANTSTORE` … `# END MYQUANTSTORE` ; logs dans
  `~/.local/share/myquantstore/logs/schedule.log`.
- Templates manuels : `contrib/systemd/`, `contrib/cron/`.
- Monitoring seul : `myquantstore status --check` (exit 1 si STALE).

> Faites un **premier `fetch` manuel** après `init` avant de vous reposer uniquement sur le timer
> (le 1er run peut backfiller plusieurs mois d'historique).

## Usage

### Workflow complet

```bash
myquantstore init
myquantstore doctor
myquantstore config

# Dry-run pour valider les ranges
myquantstore fetch --dry-run

# Backfill complet
myquantstore fetch

# Status + monitoring (exit 1 si STALE)
myquantstore status
myquantstore status --check

# Interroger l'historique (futures)
myquantstore query ES --start 2026-01-01 --end 2026-07-11 --output es_history.parquet

# 7. Interroger un stock (ajustement split appliqué par défaut)
myquantstore query AAPL --start 2024-01-01 --output aapl.parquet
# Prix bruts (non ajustés splits)
myquantstore query AAPL --no-split --output aapl_raw.parquet

# 8. Vérifier la qualité des données futures (tick size)
myquantstore query ES --check-ticksize-accuracy

# 9. Normaliser les prix futures en Int32 (multiples de tick)
myquantstore query ES --normalize-tick-size --output es_int.parquet

# 10. Rééchantillonner en candles k-min (ex: 7min) avec filtrage intraday
myquantstore query NQ --timescale-unit min --timescale-nb 7 --intraday-begin 09:30 --intraday-end 16:00

# 11. Lister/rafraîchir le cache contrats futures
myquantstore futures contracts --symbol ES --refresh

# 12. Référentiel tickers + recherche + ajout conf
myquantstore tickers refresh                              # → tickers/stocks/active.parquet
myquantstore tickers refresh --markets stocks fx --active all
myquantstore search apple --markets stocks --limit 50
myquantstore search --ticker MSFT --add                   # 1 match → config.toml
myquantstore config add TSLA NVDA                         # lookup type via cache
```

### Commandes CLI

| Commande | Description |
|---|---|
| `myquantstore init [--minimal\|--full] [-k KEY]` | Bootstrap XDG (config + dirs + clé optionnelle) |
| `myquantstore doctor [--ping]` | Diagnostic install / config / chemins (exit 1 si bloquant) |
| `myquantstore setup-key [-k KEY] [-y]` | Configure la clé API dans `~/.config/myquantstore/.env` |
| `myquantstore schedule {install\|run\|status\|show\|uninstall}` | Job périodique fetch→aggregate→status (systemd/cron) |
| `myquantstore config` | Affiche la configuration résolue (clé masquée) + chemin du fichier |
| `myquantstore status [--instrument ES] [--type futures] [--check]` | État par instrument ; `--check` exit 1 si STALE |
| `myquantstore fetch [--instrument ES] [--type futures] [--force] [--dry-run] [--no-cascade]` | Historise les chandeliers OHLCV (multi-type, cascade auto) |
| `myquantstore aggregate [--instrument ES] [--type futures] [--no-cascade]` | Régénère le cache agrégé (générique) |
| `myquantstore query <instrument> [--type] [--start] [--end] [--timescale-unit min\|hour] [--timescale-nb K] [--intraday-begin HH:MM] [--intraday-end HH:MM] [--adjust] [--no-split] [--normalize-tick-size] [--check-ticksize-accuracy] [--output] [--limit] [--no-cascade]` | Interroge l'historique continu |
| `myquantstore chart [instrument] [--type] [--port] [--host] [--mdns] [--timescale-unit] [--timescale-nb] [--nb-candle] [--intraday-begin] [--intraday-end] [--normalize-tick-size] [--no-split] [--adjust] [--no-cascade]` | Serveur de visualisation interactive |
| `myquantstore serve [--host] [--port]` | API HTTP `query()` (Parquet / Arrow, localhost, pas de cascade) |
| `myquantstore portfolio {stats\|corr\|cov\|optimize\|allocate\|frontier} [-i …] [--value] [--objective equal\|min-vol\|max-sharpe] [--export]` | MPT stocks 1day + lots ; chart `portfolio:*` (voir [docs/PORTFOLIO.md](docs/PORTFOLIO.md)) |
| `myquantstore futures contracts [--symbol ES] [--refresh] [--active-only]` | Liste/rafraîchit le cache contrats futures |
| `myquantstore options contracts` | Scaffold options (`NotImplementedError`) |
| `myquantstore tickers refresh [--markets stocks fx] [--active true\|false\|all] [--force]` | Fetch/cache shards `tickers/{market}/{active\|inactive}.parquet` + types |
| `myquantstore tickers types [--force]` | Liste/rafraîchit le cache des ticker types |
| `myquantstore search [QUERY] [--markets] [--limit N] [--add] [--yes]` | Recherche locale ; `--limit` override `display_max_rows` ; `--add` → conf |
| `myquantstore config add TICKER… [--type stocks]` | Ajoute des tickers à la conf (lookup type via cache) |

> **Référencement des instruments** : par **symbole nu** (`ES`, `AAPL`, `EURUSD`, `NDX`) — le type est résolu depuis la config. En cas d'ambiguïté (symbole présent dans plusieurs types), utiliser `--type`. On peut aussi passer la clé complète `type:symbol` (ex: `futures:ES`, `stocks:AAPL`).

### Cascade automatique (type-aware)

Les commandes `fetch`, `aggregate` et `query` vérifient leurs prérequis et les déclenchent en cascade si manquants. La chaîne dépend du type :

```
futures : contracts (/futures/v1/contracts) → fetch → aggregate → query
stocks  : splits (/stocks/v1/splits)        → fetch → aggregate → query
forex/indices :                              fetch → aggregate → query
options : NotImplemented
```

Utiliser `--no-cascade` pour désactiver l'auto-cascade (erreur explicite si prérequis manquant — utile pour cron/CI).

### Resampling et filtrage intraday (`query --timescale-unit` / `--timescale-nb` / `--intraday-begin` / `--intraday-end`)

La commande `query` supporte le **rééchantillonnage à la volée** des candles 1min en candles k-min, ainsi que le **filtrage par heure du jour** (intraday). Ces transformations sont faites à la lecture (aucun stockage) — l'agrégé reste en 1min.

**`--timescale-unit min|hour` + `--timescale-nb K`** : rééchantillonne les candles 1min en buckets de K unités (ex: `--timescale-unit min --timescale-nb 7` pour 7min, `--timescale-unit hour --timescale-nb 2` pour 2h). La grille est **ancrée au début de chaque session** pour garantir la cohérence entre jours : le bucket N démarre à `anchor + N * K`, identique pour chaque session. Les buckets partiels de fin de session sont supprimés. Une colonne `candle_count` indique le nombre de candles 1min agrégés dans chaque bucket (utile pour détecter les gaps intra-session).

**`--intraday-begin HH:MM` / `--intraday-end HH:MM`** : filtre les candles par heure du jour. Deux modes :
- **Normal** (`begin < end`, ex: `09:30`-`16:00`) : garde les candles dans `[begin, end]`.
- **Wrap-around** (`begin > end`, ex: `20:00`-`04:00`) : garde les candles `>= begin` OU `<= end` (utile pour les sessions overnight qui spannent minuit).

Les deux doivent être fournis ensemble et doivent être différents.

```bash
# Candles 7min, session RTH uniquement (09:30-16:00)
myquantstore query NQ --timescale-unit min --timescale-nb 7 --intraday-begin 09:30 --intraday-end 16:00

# Candles 15min, session overnight (wrap-around 20:00-04:00)
myquantstore query NQ --timescale-unit min --timescale-nb 15 --intraday-begin 20:00 --intraday-end 04:00

# Filtrage intraday sans resampling (candles 1min filtrés)
myquantstore query NQ --intraday-begin 09:30 --intraday-end 16:00
```

> **Note sur les types** : les colonnes `volume` et `transactions` sont stockées en `Int32` dans le Parquet agrégé (et non `Int64` comme retourné par l'API). Ce cast est fait une fois au moment de l'agrégation (`myquantstore aggregate`) et persisté dans le Parquet. Si vous avez un cache agrégé antérieur à cette version, relancez `myquantstore aggregate --instrument <symbol>` pour bénéficier du cast.

### Visualisation interactive (`myquantstore chart`)

La commande `myquantstore chart` lance un serveur web FastAPI qui sert un graphique candlestick interactif basé sur [TradingView Lightweight Charts™](https://tradingview.github.io/lightweight-charts/) (HTML5 Canvas). Le graphique supporte le zoom/pan fluide sur des centaines de milliers de chandeliers.

```bash
# Dashboard multi-instruments (http://host:port/)
myquantstore chart

# Ouvre directement le chart d'un instrument (bouton maison → dashboard)
myquantstore chart NQ

# Avec timescale 7min et filtrage intraday
myquantstore chart NQ --timescale-unit min --timescale-nb 7 --intraday-begin 09:30 --intraday-end 16:00

# Accessible sur le réseau local (mDNS)
myquantstore chart --mdns --host 0.0.0.0
```

**Fonctionnalités** :
- **Dashboard `/`** : cartes groupées par type (futures / stocks / forex / indices), collapse, tri (ticker / nom / performance), miniatures SVG sparkline 1day (`thumbnail_lookback_days`, défaut 90j). Au démarrage, fetch Yahoo 1day auto si agrégé manquant.
- **Candlestick + volume** : pane principal (candles) + pane secondaire (volume histogram). Bouton maison → dashboard.
- **Zoom/pan** : roulette de la souris = zoom axe temps, drag = pan horizontal. Cap de zoom configurable (`max_visible_candles` dans la config).
- **Buffer progressif** : chargement initial de `buffer_multiplier × max_visible_candles` candles, puis fetch progressif au fur et à mesure du pan vers la gauche (lazy loading horizontal via `before` param). Le fetch se déclenche uniquement quand moins de 250 candles restent avant le bord gauche de la vue ; un flag `noMoreData` coupe les requêtes quand l'historique est épuisé (évite les boucles sur buckets partiels).
- **Sélecteur d'UT** : dropdown dans la toolbar (1min → 4h, 1day/2day/1week).
- **Multi-instrument** : `localhost:8050/futures:ES`, `localhost:8050/stocks:AAPL`, etc. Un seul serveur sert tous les instruments configurés (indexés par clé `type:symbol`).
- **Format de transfert** : Arrow IPC (binaire, ~3x plus compact que JSON).
- **mDNS** : `--mdns` pour la découverte réseau local (accessible depuis tablette/autre poste).

**License TradingView** : Lightweight Charts est sous Apache-2.0 avec attribution requise. Le logo TradingView est affiché sur le chart (`attributionLogo: true`), ce qui satisfait l'obligation de licence.

**Améliorations futures** (documentées, non implémentées) :
- ~~Dual-source extraday Yahoo étendu (forex/indices/futures daily)~~ done
- ~~Page d'accueil dashboard à `/`~~ done
- Récupérer les chandeliers 1 seconde (plan payant)
- Import d'éléments externes : backtest / indicateurs / objets custom
- Backend alternatif FinPlot (desktop only)
- Streaming temps réel (websockets, plans payants)

### API query réseau (`myquantstore serve`)

Expose `query()` en HTTP pour un client quelconque (autre langage, autre machine, notebook) **sans** partager `data_dir` ni importer le package. Ce n'est **pas** le serveur chart et **pas** un remplacement du snapshot hebdo.

```bash
myquantstore serve                          # bind [serve].host:[serve].port (défaut 127.0.0.1:8741)
myquantstore serve --host 0.0.0.0 --port 8741   # override CLI

curl -o es.parquet 'http://127.0.0.1:8741/v1/query?instrument=ES'
curl 'http://127.0.0.1:8741/v1/health?instrument=futures:ES'
curl 'http://127.0.0.1:8741/v1/instruments'
```

- `GET /v1/query` : mêmes params que `query` (`instrument` requis, `type`, `start`/`end`, `timescale_unit`/`timescale_nb`, `adjust`, `no_split`, `dedup_timestamps` défaut true, `intraday_*`, `normalize_tick_size`). Réponse Parquet ; `Accept: application/vnd.apache.arrow.stream` → Arrow IPC.
- `GET /v1/health` : 200 OK / **503** si STALE ou agrégé manquant. Un client sérieux appelle health d'abord ; `/v1/query` sert quand même les données STALE.
- Aucune cascade / fetch réseau (agrégat absent → 404). Pas d'auth en v1 (bind localhost).
- Détail : [docs/TODO_SERVE.md](docs/TODO_SERVE.md), [docs/TECHNICAL_DESIGN.md](docs/TECHNICAL_DESIGN.md) §12ter.

## Structure du projet

```
MyQuantStore/
├─ config.toml.example          # Modèle FULL (ou: myquantstore init)
├─ .env.example                 # Modèle secrets
├─ contrib/systemd|cron/        # Templates schedule manuels
├─ docs/
│  ├─ TECHNICAL_DESIGN.md       # Documentation technique
│  ├─ MULTI_TYPE.md             # Architecture multi-type
│  ├─ PORTFOLIO.md              # MPT / portfolio CLI + chart lazy
│  └─ TODO_SERVE.md             # Spec `myquantstore serve` (API query)
├─ src/myquantstore/
│  ├─ cli.py                    # CLI argparse (multi-type + portfolio)
│  ├─ config.py                 # pydantic-settings + tomllib (XDG)
│  ├─ onboarding.py             # init / doctor
│  ├─ schedule/                 # systemd + cron + schedule run
│  ├─ resources/                # templates config embarqués
│  ├─ instruments.py / chains.py / logging_setup.py
│  ├─ api/                      # httpx + tenacity (Massive) + yahoo (curl_cffi)
│  │  ├─ aggs_futures.py, aggs_v2.py, contracts.py
│  │  ├─ corporate_actions.py   # splits + dividends Massive
│  │  ├─ tickers.py, yahoo.py, client.py
│  ├─ contracts/                # Cache contrats + RolloverChain
│  ├─ corporate_actions/        # Cache splits/dividends Massive (1min)
│  ├─ yahoo_actions/            # Cache splits/dividends Yahoo (1day)
│  ├─ tickers/                  # Référentiel + search + yahoo_map
│  ├─ storage/                  # Parquet + meta + coverage
│  ├─ pipeline/                 # historian, aggregator, cascade
│  │  └─ fetchers/              # futures, stocks, v2_single, yahoo_daily, options
│  ├─ query/                    # reader, resampler, adjust (split/div/rollover)
│  ├─ analytics/                # MPT portfolio (panel, optim, allocate, synthetic)
│  ├─ chart/                    # FastAPI + Lightweight Charts + dashboard
│  ├─ serve/                    # FastAPI API query réseau (pas le chart)
│  └─ py.typed
└─ tests/                       # pytest + respx
```

Config utilisateur (hors dépôt) :

```
~/.config/myquantstore/config.toml
~/.config/myquantstore/.env
~/.local/share/myquantstore/{data,cache,logs}/
```

## Tests

```bash
python -m pytest tests/ -v
```

## Autocompletion (optionnel)

L'autocompletion des sous-commandes et options est supportée via
[argcomplete](https://kislyuk.github.io/argcomplete/) (inclus dans les
dépendances de dev). Une fois l'environnement activé :

```bash
# Bash — ajouter dans ~/.bashrc :
eval "$(register-python-argcomplete myquantstore)"

# ZSH — ajouter dans ~/.zshrc :
autoload bashcompinit
bashcompinit
eval "$(register-python-argcomplete myquantstore)"

# Fish shell :
register-python-argcomplete --shell fish myquantstore | source
```

Après quoi `myquantstore fe<Tab>` complète automatiquement en `myquantstore fetch`.

## Documentation

- `docs/TECHNICAL_DESIGN.md` — documentation technique complète (architecture, configuration, API, rollover, cascade, etc.).
- `docs/MULTI_TYPE.md` — architecture multi-type (5 types d'instruments, endpoints par type, sémantique `--adjust`/`--no-split`, layout de stockage, statut d'implémentation).
- `docs/PORTFOLIO.md` — analyse MPT (`portfolio stats|corr|optimize|allocate|frontier`) et chart lazy `portfolio:*`.

## Confidentialité et sécurité

> **Rappel MassiVe Terms of Service** : le code source de ce projet est libre (MIT), mais les Market Data récupérées via l'API Massive.com sont soumises aux [Market Data Terms](https://massive.com/legal/market-data-terms-of-service) et ne peuvent être redistribuées. Ce dépôt ne sert qu'à partager l'outil de collecte, pas les données elles-mêmes.

## Licence

Le code de MyQuantStore est sous licence **MIT** (voir [LICENSE](./LICENSE)).

La librairie [TradingView Lightweight Charts](https://www.tradingview.com/lightweight-charts/) utilisée par la commande `myquantstore chart` est sous licence **Apache 2.0** (voir [src/myquantstore/chart/NOTICE](./src/myquantstore/chart/NOTICE) et [LICENSE-2.0.txt](./src/myquantstore/chart/LICENSE-2.0.txt)).
lète (architecture, configuration, API, rollover, cascade, etc.).
- `docs/MULTI_TYPE.md` — architecture multi-type (5 types d'instruments, endpoints par type, sémantique `--adjust`/`--no-split`, layout de stockage, statut d'implémentation).
- `docs/PORTFOLIO.md` — analyse MPT (`portfolio stats|corr|optimize|allocate|frontier`) et chart lazy `portfolio:*`.

## Confidentialité et sécurité

> **Rappel MassiVe Terms of Service** : le code source de ce projet est libre (MIT), mais les Market Data récupérées via l'API Massive.com sont soumises aux [Market Data Terms](https://massive.com/legal/market-data-terms-of-service) et ne peuvent être redistribuées. Ce dépôt ne sert qu'à partager l'outil de collecte, pas les données elles-mêmes.

## Licence

Le code de MyQuantStore est sous licence **MIT** (voir [LICENSE](./LICENSE)).

La librairie [TradingView Lightweight Charts](https://www.tradingview.com/lightweight-charts/) utilisée par la commande `myquantstore chart` est sous licence **Apache 2.0** (voir [src/myquantstore/chart/NOTICE](./src/myquantstore/chart/NOTICE) et [LICENSE-2.0.txt](./src/myquantstore/chart/LICENSE-2.0.txt)).
