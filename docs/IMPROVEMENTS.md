# Propositions d'amélioration

Audit 2026-08-22. Mise à jour après vague d'implémentation (triage utilisateur).

## Fait (cette vague)

| # | Sujet | Implémentation |
|---|---|---|
| 1 | Skip partiel futures | `has_run_today(..., ticker=)` + skip par segment dans `FuturesFetcher` |
| 2 | Résumé fetch | Compteurs par status ; exit 1 = error/not_implemented only |
| 4 | Deux sens timeframe | Barre Massive hardcodée `1min` ; TOML `timeframe` déprécié (warning) |
| 5 | `run_fetch` défaut | `[1min, 1day]` si `resolutions=None` |
| 6 | `--adjust` futures 1day | Warning logger + note CLI (no-op Yahoo `=F`) |
| 7 | CLI `--limit` | `--display-rows` (+ alias `--limit`) |
| 8 | Timezone query/serve | CLI `--timezone` ; serve `?timezone=` ; défaut `[chart] timezone` |
| 9 | `status --check` | Fail STALE only ; `--strict-missing` pour cron (`schedule run`) |
| 14 | Dead code | yahoo_map suffixes, `_check_api_key`, pyc orphelins, `query/__init__`, typer segments |
| 15–17 | Docs | TD arbre + §16 archivé ; `TODO_SERVE` → `SERVE.md` ; templates sans knobs timeframe |
| 18 | Portfolio chart | Hide si stocks vides (déjà) + label rf sur dashboard |
| 22 | Tests | coverage check, raw_dumps per-ticker, historian defaults, schedule strict |

## Reporté / backlog

### 3. Resume journal segments 1min
Couvert en pratique par le skip par segment. Journal dédié = plus tard si besoin.

### 10. Formule tick-size
Smell de design (seuil très serré) — code et spec alignés. Ne pas changer sans décision produit.

### 11. Écriture agrégée atomique
Temp file + rename pour lectures concurrentes serve/query pendant aggregate.

### 12. Clés TOML storage ignorées
Charger `contracts_cache_subdir` / `corporate_actions_cache_subdir` / `tickers_cache_subdir`
ou les retirer des templates.

### 13. MassiveClient optionnel Yahoo-only
Éviter `Authorization: Bearer ` vide sur jobs 1day purs.

### 19. Serve v1+
Auth, cascade optionnelle, `check_ticksize_accuracy`, JSON colonnes, pagination curseur.
Voir `docs/SERVE.md` §4.

### 20. Options
Scaffold — rester explicite dans le README.

### 21. `myquantstore instruments`
Vue d'ensemble multi-type (roadmap MULTI_TYPE).

## Notes d'usage post-vague

- Cron fraîcheur stricte : `status --check --strict-missing` (déjà branché dans `schedule run`).
- Interactif : `status --check` ne fail plus sur instrument neuf (agrégé absent).
- Futures partiel : re-lancer sans `--force` reprend les contrats non dumpés aujourd'hui.
- Affichage query : préférer `--display-rows N` ; `--limit` reste un alias.
