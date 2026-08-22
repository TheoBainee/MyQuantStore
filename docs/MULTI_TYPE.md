# Architecture multi-type — MyQuantStore

Massive.com expose **5 types d'instruments** financiers, chacun avec un endpoint
REST et un schéma de réponse distincts. MyQuantStore les modélise via un système
**multi-type** avec dispatch automatique par type.

## 1. Les 5 types d'instruments

| Type | Endpoint OHLCV | Timestamp | Contrats / expiration | Implémenté |
|---|---|---|---|---|
| `futures` | `/futures/v1/aggs/{ticker}` | nanosecondes | OUI (rollover) | ✅ |
| `stocks` | `/v2/aggs/ticker/{t}/range/{m}/{ts}/{from}/{to}` | millisecondes | NON (splits/dividends) | ✅ |
| `forex` | `/v2/aggs/ticker/C:{t}/range/...` | millisecondes | NON | ✅ |
| `indices` | `/v2/aggs/ticker/I:{t}/range/...` | millisecondes | NON (pas de volume) | ✅ |
| `options` | `/v2/aggs/ticker/O:{t}/range/...` | millisecondes | OUI (strike/call/put) | 🚧 Scaffold |

### Différences clés (API)

- **Futures** utilise un endpoint dédié (`/futures/v1/aggs`) avec un seul paramètre
  `resolution="1min"` et des filtres `window_start.gte/lte`. Schéma : champs longs
  (`open, high, low, close, volume, transactions, dollar_volume, settlement_price,
  session_end_date, ticker`).
- **Forex / stocks / indices / options** partagent l'endpoint v2
  (`/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`) avec
  timestamps en **millisecondes** et **champs courts** (`o, h, l, c, v, n, t, vw`).
  Barre Massive hardcodée `"1min"` → `(multiplier=1, timespan="minute")`.
- **Préfixe de ticker** : `forex` → `C:`, `indices` → `I:`, `options` → `O:`.
  Ajouté automatiquement par `Instrument.api_ticker` (le symbole nu est utilisé en
  config/CLI/storage).
- **`session_end_date`** n'existe que pour futures. Pour les autres types, MyQuantStore
  synthétise `session_end_date = window_start.date()` afin que le resampler
  (ancré par session) reste générique.
- **`volume`** est absent pour les indices (l'agrégateur et le resampler gèrent
  l'absence via des `if col in df.columns`).
- **`adjusted` (v2)** : pour stocks, MyQuantStore fetch avec `adjusted=false` (prix
  bruts) afin de permettre le toggle `--no-split` au runtime (voir §4).

## 2. Modèle d'instrument

`myquantstore/instruments.py` :

```python
class InstrumentType(StrEnum):  # futures | forex | stocks | indices | options
    ...

@dataclass(frozen=True)
class Instrument:
    type: InstrumentType
    symbol: str          # symbole nu : "ES", "AAPL", "EURUSD", "NDX"

    @property
    def key(self) -> str:          # "futures:ES" — anti-collision de stockage
    @property
    def api_ticker(self) -> str:   # "C:EURUSD" (préfixe auto pour v2)
    @property
    def path_segment(self) -> str: # "futures" (segment de chemin)
```

Les instruments sont déclarés en config par listes compactes par type (symboles
nus) :

```toml
[instruments]
futures = ["NQ", "ES", "RTY", "YM"]
forex   = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD"]
stocks  = []
indices = ["SPX", "NDX", "DJI", "RUT"]
options = []
```

`Settings.resolve_instrument(symbol, type=None)` résout un symbole depuis la
config (lève si absent ou ambigu sans `--type`).

## 3. Chaînes d'instruments (InstrumentChain)

`myquantstore/chains.py` définit le protocole `InstrumentChain` — abstraction commune
pour `query` et `chart` :

- `active_contract(d)`, `segment_for_ticker(t)`, `continuous_segments(start, end)`,
  `tick_size_for_ticker(t)`, `to_table()`.

Implémentations :

- **`RolloverChain`** (`contracts/rollover.py`) — futures : chaîne de contrats
  expirants, `rollover_date = last_trade_date - days_before_expiry`.
- **`SingleSymbolChain`** — forex/stocks/indices : un seul segment, `tick_size = 0.0`
  (normalisation no-op).
- **`OptionsChain`** — scaffold : toutes méthodes lèvent `NotImplementedError`.

`build_chain(instrument, contracts_df=None, **kwargs)` fabrique la bonne chaîne
selon le type. `query()` accepte `chain: InstrumentChain | None` — requis
uniquement pour `--normalize-tick-size` et `--check-ticksize-accuracy` (futures).

## 4. Ajustements des prix

MyQuantStore stocke les prix **bruts** et applique les ajustements **à la query**
(permet les toggles runtime) :

| Flag | Futures | Stocks | Forex/Indices |
|---|---|---|---|
| `--no-split` | no-op | désactive l'ajustement split (ON par défaut) | no-op |
| `--adjust` | back-adjusted rollover (Panama ratio sur closes au bascule) | ajustement dividend (après splits) | no-op |

**Ajustement split (stocks)** — `query/adjust.py:apply_split_adjustment` :
pour chaque chandelier à la date D, multiplier les prix par le
`historical_adjustment_factor` du premier split **postérieur** à D.
- Track **1min** : cache Massive `/stocks/v1/splits`.
- Track **1day** : cache Yahoo `yahoo_actions` (les OHLC Yahoo sont d'abord
  désajustés à l'ingest via `reverse_split_adjustment` pour stocker des bruts).
Activé par défaut ; `--no-split` le désactive (prix bruts).

**Ajustement dividend (stocks)** — `--adjust` → `apply_dividend_adjustment` :
même logique join-asof sur `ex_dividend_date` + `historical_adjustment_factor`
(Massive 1min ou Yahoo 1day). Cascade pré-fetch peupple le cache dividends.

**Ajustement rollover (futures)** — `--adjust` → `apply_rollover_adjustment` :
série back-adjusted vers le contrat le plus récent (ratio des closes au
`rollover_date`, facteurs cumulés arrière). Incompatible avec
`--normalize-tick-size`.

## 5. Fetchers multi-type

`myquantstore/pipeline/fetchers/` :

- `base.InstrumentFetcher` (ABC) — `fetch(instrument, settings, client, force, dry_run)`.
- `FuturesFetcher` — RolloverChain + `/futures/v1/aggs/{ticker}` (range par segment).
- `StocksFetcher` — `/v2/aggs/ticker/{t}/range/...` (`adjusted=false`) + cache splits/dividends.
- `V2SingleSymbolFetcher` — forex / indices via `/v2/aggs/ticker/{api_ticker}/range/...`
  (préfixes `C:` / `I:`, pas de corporate actions ; volume optionnel pour indices).
- `YahooDailyFetcher` — track `1day` multi-type (chart Yahoo via `curl_cffi`).
- `OptionsFetcher` — scaffold (`NotImplementedError`).
- `get_fetcher(instrument)` — factory Massive 1min (options lève `NotImplementedError`).

`pipeline/historian.py:run_fetch` orchestre la boucle sur une liste d'instruments
et délègue au fetcher adapté. Le retour est un dict homogène
`{instrument_key: {status, candles, ...}}`.

## 6. Cascade (type-aware)

`pipeline/cascade.py` — la chaîne de dépendances diffère par type :

```
futures : contracts (/futures/v1/contracts) → fetch → aggregate → query
stocks  : splits + dividends                → fetch → aggregate → query
forex/indices :                              fetch → aggregate → query
options : NotImplemented
```

`ensure_pre_fetch(instrument, ...)` rafraîchit le cache de listing adapté au type.
`ensure_aggregate(...)` retourne la chaîne d'instrument construite
(`RolloverChain` / `SingleSymbolChain`).

## 7. Stockage (layout multi-type × multi-résolution)

```
{data_dir}/
├─ raw/
│  └─ {type}/              # futures, stocks, ...
│     └─ {symbol}/         # ES, AAPL
│        └─ {ticker}/      # ESM5 (contrat futures) ou = symbol (stocks)
│           └─ {resolution}/   # 1min (Massive) | 1day (Yahoo multi-type)
│              └─ {run_ts}.parquet (+ .meta.json, immuable)
└─ aggregate/
   └─ {type}/
      └─ {symbol}/
         ├─ 1min.parquet (+ .meta.json)   # Massive
         └─ 1day.parquet (+ .meta.json)   # Yahoo multi-type

{cache_dir}/
├─ contracts/              # cache contrats futures (inchangé)
│  └─ {product}.parquet
├─ corporate_actions/      # cache splits/dividends stocks (Massive, track 1min)
│  └─ {ticker}/
│     └─ splits.parquet
├─ yahoo_actions/          # splits/dividends Yahoo (track 1day stocks only)
│  └─ {ticker}/
└─ tickers/                # référentiel /v3/reference/tickers (shards)
   ├─ types.parquet
   ├─ stocks/active.parquet
   ├─ stocks/inactive.parquet   # si refresh --active all|false
   ├─ fx/active.parquet
   └─ …
```

**Dual-source (design)** :

| Famille | Source | Résolution stockée | Construit à la query |
|---|---|---|---|
| Intraday | Massive | `1min` | 2m, 5m, 1h, 4h… |
| Extraday | Yahoo (chart curl_cffi) | `1day` multi-type | 2d, 1w… |

- Helpers : `instruments.timeframe_family`, `base_resolution_for_timeframe`, `DEFAULT_RESOLUTION`.

### Recherche d'instruments (`myquantstore search` / `config add`)

1. `myquantstore tickers refresh [--markets stocks fx] [--active true|false|all]`  
   écrit un shard par `(market, active|inactive)`. Défaut : `stocks/active`.  
   TTL = `[instrument_cache] ttl_days` **par shard**.
2. `myquantstore search [query] [--markets …] [--limit N]` filtre en local (concat des shards).  
   `--limit` plafonne data **et** affichage (override `display_max_rows`).
3. `search --add` ou `config add TICKER…` écrit dans `config.toml` via `tomlkit`,
   map `market` → liste conf : `stocks|otc→stocks`, `fx→forex`, `indices→indices`
   (crypto skip). Préfixes `C:`/`I:`/`O:` retirés.

Schéma canonique des dumps (normalisé depuis l'API) : `window_start` (Datetime[ns]),
`ticker`, `open/high/low/close`, `volume`?, `transactions`?, `dollar_volume`?,
`vwap`?, `session_end_date` (synthétisé pour non-futures), `settlement_price`?
(futures), + stamp `symbol`, `instrument_type`, `run_id` (à la lecture).

L'agrégateur (`pipeline/aggregator.py`) est **générique** : concat des dumps,
dédup sur `(window_start, ticker)`, cast `Categorical` (`run_id, ticker, symbol,
instrument_type, product_code`) + `Int32` (`volume, transactions`). Aucune
logique de rollover : le stitch 1min (quel contrat sur quelle fenêtre) se fait
au **fetch**. `query()` n'applique le rollover que pour `--adjust` (Panama).

Au jour de roll, l'agrégat 1min **peut** contenir deux lignes au même
`window_start` (deux `ticker`) — clé naturelle `(timestamp, contrat)`, pas un
bug. `query()` déduplique **par défaut** (contrat le plus récent de la
chaîne). `--no-dedup-timestamps` conserve les deux. Le chart utilise ce défaut.

## 8. Configuration

```toml
[instruments]          # listes compactes par type (symboles nus)
futures = ["NQ", "ES", "RTY", "YM"]
forex   = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD"]
stocks  = []
indices = ["SPX", "NDX", "DJI", "RUT"]
options = []

[futures]              # spécifique futures
days_before_expiry = 7
contracts_page_limit = 1000
contracts_snapshot_interval_months = 1

[stocks]               # spécifique stocks
splits_page_limit = 5000
dividends_page_limit = 5000

[instrument_cache]     # TTL commun à tous les caches (contrats, splits, ...)
ttl_days = 30

[fetch]                # générique (commun aux aggs de tous les types)
timeframe = "1min"
overlap_buffer_days = 1
requests_per_minute = 6
page_limit = 50000
max_retries = 6

[fetch.history_months] # par type (défaut: 24, indices: 60)
futures = 24
forex = 24
stocks = 24
indices = 60
options = 24

[storage]
data_dir = "~/.local/share/myquantstore/data"
cache_dir = "~/.local/share/myquantstore/cache"
log_dir = "~/.local/share/myquantstore/logs"
```

## 9. CLI multi-type

- `--instrument <symbol>` (ou clé `type:symbol`) + `--type <type>` optionnel pour
  lever l'ambiguïté. Si omis, opère sur **tous** les instruments configurés.
- `myquantstore futures contracts [--symbol ES]` — cache contrats futures.
- `myquantstore options contracts` — scaffold (`NotImplementedError`).
- `query`/`chart` : `--no-split` (stocks, bruts), `--adjust` (Panama futures 1min /
  dividends stocks ; no-op warn sur futures 1day Yahoo `=F`),
  `--normalize-tick-size` / `--check-ticksize-accuracy` (futures, requièrent la chaîne).
  `--display-rows` (alias `--limit`) = plafond d'affichage CLI uniquement.

## 10. Statut d'implémentation

| Type | Fetch | Aggregate | Query | Chart | Cache listing | Rollover/splits |
|---|---|---|---|---|---|---|
| futures | ✅ | ✅ | ✅ | ✅ | ✅ contrats | ✅ RolloverChain |
| stocks | ✅ | ✅ | ✅ (split adjust) | ✅ | ✅ splits | ✅ split adjust (`--no-split`) |
| forex | ✅ | ✅ | ✅ | ✅ | n/a | n/a (`SingleSymbolChain`) |
| indices | ✅ | ✅ | ✅ (sans volume) | ✅ | n/a | n/a (`SingleSymbolChain`) |
| options | 🚧 NotImplemented | 🚧 | 🚧 | 🚧 | 🚧 NotImplemented | 🚧 OptionsChain NotImplemented |

**Roadmap** :
- ~~Implémentation de `--adjust` (rollover back-adjusted futures + dividend stocks).~~ done
- ~~`fetch_dividends` / cache dividends (stocks).~~ done (Massive + Yahoo)
- Options : `OptionsFetcher` + `OptionsChain` + cache contrats.
- Commande `myquantstore instruments` (vue d'ensemble multi-type).
- Portfolio multi-asset (ETFs, indices, futures root) au-delà des stocks 1day.
