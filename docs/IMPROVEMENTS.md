# Propositions d'amélioration

Audit 2026-08-22. Les **correctifs évidents** (bugs, docs périmées, fuites de clé)
sont déjà appliqués. Ce fichier ne liste que ce qui reste à décider / implémenter.

## Priorité haute (fiabilité fetch / cron)

### 1. Skip partiel futures + `has_run_today`

`has_run_today` est vrai dès qu'**un** ticker du produit a un dump daté
d'aujourd'hui. Si `fetch_aggs_futures` lève au milieu de la boucle de segments,
les dumps déjà écrits restent ; le run suivant **sans `--force`** skippe tout
le produit. Les contrats manquants ne sont pas retentés avant le lendemain.

Pistes : sentinel de run complet, skip **par segment**, ou ne pas skipper si
la chaîne attendue n'est pas couverte / si l'agrégé est STALE.

Fichiers : `storage/raw_dumps.py`, `pipeline/fetchers/futures.py`.

### 2. Résumé fetch plus strict

`fetch` retourne désormais 1 sur `error` / `not_implemented`. À trancher :

- `no_candles` / `no_range` : warn vs fail
- `skipped` + STALE : fail (le cron `schedule run` enchaîne déjà
  `status --check`)
- compter les erreurs dans le résumé (N ok / N fail)

### 3. Complétude de run 1min

Pas de reprise fine après interruption réseau. Un run `--force` tout
re-télécharge. Un journal de segments réussis permettrait un resume.

## Priorité moyenne (sémantique / UX)

### 4. Deux sens de `timeframe`

- Config `[fetch] timeframe = "1min"` = taille de barre **API Massive**
- CLI `--timeframe all|1min|1day` = **track de stockage**

Si quelqu'un met `timeframe = "5min"` dans le TOML, des barres 5 min sont
stockées sous `1min/` et resamplées comme du 1 min. Valider
`[fetch] timeframe == "1min"` (ou hardcoder 1min et retirer le knobs).

### 5. `run_fetch` défaut `resolutions=[1min]`

La CLI passe `all`. Un appel interne oublié skippe silencieusement le track
Yahoo. Défaut = `[1min, 1day]` ou liste obligatoire.

### 6. `--adjust` sur futures **1day**

Yahoo `=F` est déjà une série continue (souvent back-adjusted).
`apply_rollover_adjustment` matche des tickers de **contrats** (`ESM5`) →
no-op silencieux. Documenter clairement, ou refuser `--adjust` sur le track
1day futures.

### 7. CLI `--limit` vs `query(limit=)` vs serve

- `query(limit=N)` = `df.head(N)` (les **plus anciennes**)
- CLI `--limit` = plafond **d'affichage** uniquement (`--output` écrit tout)
- serve : pas de limit

Piège documenté dans `TODO_SERVE.md`. Renommer le flag CLI en
`--display-rows` ; ne jamais passer `head` pour « les N dernières ».

### 8. Timezone query / serve

Le chart honore `[chart] timezone` pour `intraday_begin/end`. CLI `query` et
`serve` restent en UTC. Ajouter `--timezone` / query param pour aligner les
sessions (ex. 09:30–16:00 America/New_York).

### 9. `status --check` sur instrument neuf

`has_problems` est vrai aussi pour un agrégé **absent** (WARN). Un `status
--check` échoue sur un univers tout juste ajouté — voulu pour le cron, surprenant
en interactif. Distinguer STALE (fail) vs missing (warn) via un flag.

## Priorité basse (qualité / hygiene)

### 10. Formule tick-size

`abs(p/tick - round(p/tick)) > trigger * tick` mélange fraction de tick et
prix. Pour ES (tick 0.25, trigger 0.1) le seuil est ~0.025 tick — très serré.
Préférer `> trigger` (fraction de tick) ou une distance en prix
`abs(p - round(p/tick)*tick) > trigger * tick`. Spec et code sont alignés :
c'est un choix de design, pas un drift.

### 11. Écriture agrégée atomique

`docs/TODO_SERVE.md` le note : un `query` concurrent pendant `aggregate`
peut lire un Parquet à moitié écrit. Pattern : fichier temporaire + `rename`.

### 12. Clés TOML storage ignorées

`load_settings` n'applique pas `contracts_cache_subdir`,
`corporate_actions_cache_subdir`, `tickers_cache_subdir`. Seul
`yahoo_actions_cache_subdir` est mappé. Soit les charger, soit les retirer
des templates.

### 13. Ne pas instancier `MassiveClient` pour un job Yahoo-only

Évite un header `Authorization: Bearer ` vide. `run_fetch` accepte déjà
d'ignorer le client pour 1day.

### 14. Dead code / hygiene

- `_SKIP_SUFFIXES*` dans `tickers/yahoo_map.py` (seul `_SKIP_RE` est utilisé)
- `_check_api_key` no-op dans `config.py`
- bytecode orphelin possible (`aggregates.pyc`, `migrate_layout.pyc`)
- `query/__init__.py` vide — exporter `query` / `parse_query_datetime`
- `__init__.py` futures : typer `RolloverSegment` au lieu de `object` +
  `type: ignore`

### 15. TECHNICAL_DESIGN (gros chantier doc)

§5–7 / §10 / §16 sont encore partiellement « futures-only 2026-07 » :

- layout raw/aggregate pré-multi-résolution
- arbre sans `serve/`, `schedule/`, `analytics/`
- « 143 tests » + plan d'implémentation lu comme du travail courant
- extraits de config sans `[health]` / `[yahoo]` / `[portfolio]`

Archiver §16 ; une passe de réécriture des chemins / noms d'API.

### 16. Templates config

`config.toml.example` est en retard sur `resources/config.full.toml`
(`[chart] timezone`, couleurs, `[chart.overlay]`). Générer l'example depuis
le template embarqué, ou pointer uniquement `myquantstore init`.

Le `config.toml` à la racine du dépôt est un fichier **personnel** (chemins
absolus, bind LAN) — ne pas le documenter comme modèle.

### 17. `docs/TODO_SERVE.md`

v1 est fait. Renommer en `SERVE.md` ; garder le hors-v1 comme backlog.

### 18. Portfolio chart

L'optim chart utilise maintenant `resolve_risk_free_rate` (comme la CLI).
Reste : indiquer sur le dashboard « rf = Yahoo ^IRX » vs static ; cacher
`portfolio:*` si l'univers stocks est vide.

### 19. Serve v1+

Backlog déjà dans `TODO_SERVE.md` : auth, cascade optionnelle, `timezone`,
`check_ticksize_accuracy`, format JSON colonnes, pagination curseur.

### 20. Options

Toujours scaffold. Pas une régression — rester explicite dans le README.

### 21. `myquantstore instruments`

Roadmap MULTI_TYPE uniquement. Utile pour une vue d'ensemble multi-type
(sans passer par `status` verbeux).

### 22. Tests manquants utiles

- fetch partiel futures + skip lendemain
- `--end YYYY-MM-DD` bout-en-bout CLI 1min (plus que le parseur)
- chart portfolio : flags `--adjust` / `--no-split` (wiring maintenant corrigé)
- `next_url` en log DEBUG ne contient plus la clé (redact unitaire OK ;
  ajouter un test `get_paginated` + caplog)
