# Overlays backtest — contrat `meta.json` v2

Statut : **fait**. Implémentation `src/myquantstore/chart/overlay.py`, API
`chart/server.py`, sélecteur `chart/static/chart.html`, validation CLI
`myquantstore doctor overlays`, tests `tests/test_chart_overlay.py`.

Ce document est le **contrat du producteur** : MQS ne génère pas les overlays,
il les lit. Un service externe écrit les fichiers, MQS les indexe, les nomme et
les rend navigables.

**Le parseur est strict et v2 uniquement.** Aucune rétrocompatibilité avec
l'ancien format plat `{backtest_id: {...}}`. Un fichier non conforme est
**ignoré et listé** — il n'interrompt pas le catalogue et ne disparaît pas en
silence : sa raison de rejet remonte dans l'API (`skipped`), dans le sélecteur
du chart et dans `doctor overlays`.

---

## 1. Arborescence

```
{overlay_dir}/Backtests/
├── YM_242_920.meta.json            # obligatoire — contrat ci-dessous
├── YM_242_920.transactions.parquet # optionnel — markers d'entrée/sortie
└── YM_242_920.orders.parquet       # optionnel — segments d'ordres en attente
```

`overlay_dir` vient de `[chart.overlay] overlay_dir` (rétrocompat
`[chart] overlay_dir`). Vide ⇒ overlays désactivés.

Le préfixe commun des trois fichiers est le **stem**. Il doit matcher
`^[A-Za-z0-9_.-]+$` (anti-traversal) et ne peut donc pas contenir `|`.

Un stem = une unité de stockage, typiquement un run d'optimisation. Le
regroupement logique ne passe **pas** par le stem mais par `backtest_type`,
qui peut couvrir plusieurs stems.

---

## 2. `meta.json` — schéma

```json
{
  "mqs_overlay": 2,
  "backtest_type": "cth_factor",
  "instrument": "futures:YM",
  "timeframe": { "unit": "min", "nb": 1 },
  "label_params": ["is_short", "entry_factor", "factor"],
  "shared": {
    "ticksize": 1.0,
    "cth_open": 242,
    "cth_close": 920,
    "session": { "tz": "America/Chicago" }
  },
  "backtests": {
    "short_20_35_0": {
      "params": { "is_short": true, "entry_factor": 20, "factor": 35, "stop_factor": 0 }
    },
    "long_-75_0_150": {
      "params": { "is_short": false, "entry_factor": -75, "factor": 0, "stop_factor": 150 }
    }
  }
}
```

### Champs racine

| Clé | Requis | Type | Rôle |
|---|---|---|---|
| `mqs_overlay` | ✅ | `2` (entier) | Version de schéma. Toute autre valeur ⇒ fichier rejeté. |
| `backtest_type` | ✅ | string non vide | Regroupe les backtests d'une même optimisation (`rsi`, `macd`, `cth_factor`…). Valeur libre, **non connue de MQS à l'avance**. |
| `instrument` | ✅ | string non vide | Clé produit MQS (`futures:YM`). Filtre le catalogue du chart. |
| `timeframe` | ✅ | `{unit, nb}` | **UT sur laquelle le backtest a été calculé.** Voir §3. |
| `backtests` | ✅ | objet non vide | `{backtest_id: entrée}`. Les ids sont libres. |
| `shared` | ⬜ | objet | Params communs à toutes les entrées du fichier. Évite la duplication. |
| `label_params` | ⬜ | liste de strings | Force la sélection et l'ordre des params affichés dans le label (§5). |

### Entrée de `backtests`

| Clé | Requis | Rôle |
|---|---|---|
| `params` | ⬜ | Params propres au backtest. Dict **libre** — c'est ce que MQS découvre au scan. |
| `timeframe` | ⬜ | Override de l'UT du fichier. Permet à un sweep qui balaie aussi l'UT de tenir dans un seul fichier. |
| `backtest_type` | ⬜ | Override du type du fichier. |
| `instrument` | ⬜ | Override de l'instrument du fichier. |

Une entrée peut être `{}` ou `null` : elle n'hérite alors que de `shared`.

---

## 3. UT (`timeframe`)

```json
{ "unit": "min", "nb": 15 }
```

`unit` ∈ `min` | `hour` | `day` | `week` — **même vocabulaire que le sélecteur
d'UT du chart**, pas un dialecte séparé. `nb` est un entier > 0.

MQS canonicalise en minutes pour pouvoir comparer :

| unit | × | UT exposées par le chart |
|---|---|---|
| `min` | 1 | 1, 2, 5, 10, 15, 30 |
| `hour` | 60 | 60, 120, 240 |
| `day` | 1440 | 1440, 2880 |
| `week` | 10080 | 10080 |

L'API renvoie `{unit, nb, minutes}`. `minutes` est ce qui sert aux filtres.

Rien n'oblige l'UT d'un backtest à figurer dans la liste du chart : un backtest
en `{unit: "min", nb: 3}` est valide, il sera simplement comparé à `3`.

---

## 4. Params effectifs

**Params effectifs = `shared` ∪ `params`**, l'entrée gagnant en cas de collision.

Les objets imbriqués sont **aplatis en chemins pointés** :

```json
"shared": { "session": { "tz": "America/Chicago" } }
```

devient `session.tz = "America/Chicago"`. C'est sous ce nom que le param est
cherchable et affiché.

Types conservés tels quels : `bool`, `int`, `float`, `string`, `null`.
Les listes et types exotiques sont **stringifiés** (`[5, 10]` → `"[5, 10]"`) :
consultables au tooltip, hors comparaison numérique.

---

## 5. Labels parlants

Le problème résolu : avec beaucoup d'overlays, `short_20_35_0` ne dit rien.

MQS calcule les labels **côté serveur**, par détection des params discriminants :

1. Toutes les entrées du produit sont groupées par `backtest_type`, tous stems
   confondus.
2. Une clé de param est **saillante** si elle prend ≥ 2 valeurs distinctes dans
   le groupe, ou si elle est absente de certaines entrées.
3. Label = `type · UT · params saillants`.
4. Les booléens sortent en nom nu, préfixe `is_` retiré : `short` si vrai,
   `¬short` si faux. Les autres en `clé=valeur`.
5. Aucun saillant (groupe à une seule entrée) ⇒ repli sur l'id.
6. L'id brut reste toujours affiché à côté du label dans le sélecteur.

```
cth_factor · 1min · short · entry_factor=20 · factor=35 · stop_factor=0
rsi · 15min · length=14
```

**Conséquence pratique** : les params constants d'une optimisation (`ticksize`,
`cth_open`, `session.tz`) sont du bruit — ils n'apparaissent jamais dans le
label, seulement au tooltip. Inutile de les retirer de `shared` pour « nettoyer »
les noms, MQS s'en charge.

**Labels trop longs ?** C'est le signe d'une optimisation à beaucoup d'axes.
Utiliser `label_params` pour choisir les 2-3 axes qui comptent :

```json
"label_params": ["entry_factor", "factor"]
```

---

## 6. API `GET /api/overlays?product=`

Une ligne **par backtest** (et non par fichier), plus les facettes de navigation.

```json
{
  "overlays": [
    {
      "key": "YM_242_920|short_20_35_0",
      "stem": "YM_242_920",
      "id": "short_20_35_0",
      "backtest_type": "cth_factor",
      "instrument": "futures:YM",
      "timeframe": { "unit": "min", "nb": 1, "minutes": 1 },
      "label": "cth_factor · 1min · short · entry_factor=20 · factor=35",
      "salient": ["is_short", "entry_factor", "factor"],
      "params": { "ticksize": 1.0, "cth_open": 242, "session.tz": "America/Chicago" }
    }
  ],
  "facets": {
    "types": [{ "name": "cth_factor", "count": 6 }, { "name": "rsi", "count": 24 }],
    "timeframes": [{ "unit": "min", "nb": 1, "minutes": 1 }],
    "param_keys": {
      "entry_factor": { "kind": "number", "values": [-75, 0, 20, 50], "truncated": false }
    }
  },
  "skipped": [{ "file": "old.meta.json", "reason": "mqs_overlay absent ou != 2 (trouvé None)" }]
}
```

- `key` est l'identifiant stable `{stem}|{id}` — le stem ne pouvant contenir `|`,
  le premier séparateur marque la frontière.
- `facets.types` porte un compte par type : c'est ce qui alimente le sélecteur de
  type et permet de voir les types disponibles sans déplier les groupes.
- `facets.param_keys[].values` est plafonné à 50 valeurs distinctes
  (`truncated: true` au-delà).
- Tri déterministe : `backtest_type`, puis UT en minutes, puis stem, puis id.

`GET /api/overlay/{stem}?id={backtest_id}` renvoie le payload d'un backtest :
métadonnées (`backtest_type`, `timeframe`, `params`), `transactions` et `orders`.
`id` absent ⇒ premier backtest du fichier.

**Cache** : le parsing est mémoïsé par dossier et invalidé sur l'empreinte
`(nom, mtime_ns, taille)` des `*.meta.json`. Réécrire un fichier suffit à le
faire reprendre en compte — pas de TTL, pas de redémarrage.

---

## 7. Sélecteur du chart

Combobox popover (remplace le `<select>` natif plat) :

- **Recherche** : tokens séparés par des espaces, tous requis, en sous-chaîne sur
  le label, l'id, le stem, le type, l'UT et les `clé=valeur` des params.
- **Sélecteur de type** : types découverts avec leur compte, + « Tous les types ».
- **Chips de relation UT**, comparées à l'UT courante du graph :

| Chip | Sémantique | Défaut |
|---|---|---|
| `UT ≥ graph` | UT de calcul égale ou plus grossière que l'affichage | ✅ |
| `UT = graph` | correspondance exacte | |

  `≥` est le défaut parce que c'est la direction **non lossy** : un backtest 15min
  affiché sur un graph 1min place chaque event sur sa candle exacte, alors qu'un
  backtest 1min sur un graph 15min écrase jusqu'à 15 events sur la même candle.
  Sur un graph 1min tout le catalogue est donc visible ; le filtre se resserre
  naturellement en montant en UT.

- **Section repliable « Hors filtre UT »** : garde joignables les backtests que
  la relation exclut — sans elle, un backtest 1min serait inatteignable depuis un
  graph 5min.
- **Tooltip au survol** : liste complète des params (saillants mis en avant), UT,
  instrument, fichier.
- La sélection courante **reste affichée même hors filtre**, signalée par un ⚠ sur
  le déclencheur. Changer l'UT du graph ne retire pas le calque.
- Filtres (type, relation UT) et dernière sélection persistés dans
  `localStorage['myquantstore-overlay-filters']`.
- Deep-link `?overlay={stem}&id={backtest_id}` prioritaire sur la restauration.

Le rendu est un **calque pur** : sélectionner un overlay ne recharge pas les
chandeliers et ne déplace pas la vue.

---

## 8. Parquet associés

Colonnes attendues, filtrées sur `backtest_id` quand la colonne est présente.

`{stem}.transactions.parquet` → markers :

| Colonne | Rôle |
|---|---|
| `backtest_id` | Filtre l'entrée sélectionnée. |
| `time` | Timestamp de l'exécution. |
| `price` | Prix d'exécution. |
| `side` | `buy` / `sell` → couleur (`[chart.overlay.backtest]`). |
| `kind` | `entry` / `exit` (informatif). |

`{stem}.orders.parquet` → segments horizontaux :

| Colonne | Rôle |
|---|---|
| `backtest_id` | Filtre l'entrée sélectionnée. |
| `time_from` / `time_to` | Fenêtre de l'ordre en attente. `time_to <= time_from` ⇒ 1 chandelier. |
| `price` | Niveau du segment. |
| `side` | `buy` / `sell` → couleur. |
| `order_type` | `LMT` (trait plein), `STP` (pointillé), `MKT` (**ignoré** — un ordre marché n'a pas d'attente). |

Les timestamps sont snappés sur la dernière candle `<= time` (dichotomie côté
client), ce qui rend le rendu correct quelle que soit l'UT d'affichage.

---

## 9. Validation — `myquantstore doctor overlays`

Lecture seule, sans serveur chart. Sert à vérifier la sortie du producteur.

```bash
myquantstore doctor overlays
myquantstore doctor overlays --overlay-dir /chemin/vers/overlays
```

Affiche par produit les backtests trouvés (type, UT, label généré, params
saillants) puis les fichiers rejetés avec leur raison. **Exit 1** si au moins un
fichier est rejeté — scriptable en garde-fou côté producteur.

---

## 10. Erreurs de validation

Messages de rejet possibles :

| Raison | Cause |
|---|---|
| `mqs_overlay absent ou != 2 (trouvé …)` | Fichier legacy, ou version inconnue. |
| `backtest_type manquant ou vide` | Champ obligatoire. |
| `instrument manquant ou vide` | Champ obligatoire. |
| `timeframe manquant (UT de calcul obligatoire)` | Champ obligatoire. |
| `timeframe.unit invalide (…)` | Hors `min` / `hour` / `day` / `week`. |
| `timeframe.nb doit être un entier > 0 (…)` | `0`, négatif, booléen ou non entier. |
| `backtests manquant ou vide` | Aucun backtest dans le fichier. |
| `shared doit être un objet` | Type incorrect. |
| `label_params doit être une liste de chaînes` | Type incorrect. |
| `backtests[id].params doit être un objet` | Type incorrect. |
| `JSON invalide: …` | Fichier illisible. |
| `stem overlay invalide: …` | Nom de fichier hors `^[A-Za-z0-9_.-]+$`. |

---

## 11. Migration depuis le format legacy

L'ancien format répétait tout à chaque entrée et planquait l'UT dans
`session.ut_minutes` :

```json
{
  "short_20_35_0": {
    "instrument": "futures:YM", "ticksize": 1.0,
    "cth_open": 242, "cth_close": 920,
    "is_short": true, "entry_factor": 20, "factor": 35, "stop_factor": 0,
    "session": { "tz": "America/Chicago", "ut_minutes": 1 },
    "extra": {}
  }
}
```

Transformation :

1. Envelopper dans `{ "mqs_overlay": 2, ..., "backtests": { … } }`.
2. Remonter `instrument` à la racine.
3. `session.ut_minutes: 1` → `"timeframe": { "unit": "min", "nb": 1 }` à la racine.
   L'UT devient un champ de premier plan, obligatoire et typé.
4. Choisir un `backtest_type` — c'est lui qui regroupera l'optimisation.
5. Factoriser dans `shared` ce qui est constant (`ticksize`, `cth_open`,
   `cth_close`, `session.tz`).
6. Ne garder dans `params` que les axes de l'optimisation.
7. `extra: {}` n'est plus un champ spécial : soit le supprimer, soit verser son
   contenu dans `params` — il sera indexé et cherchable comme le reste.

Vérifier avec `myquantstore doctor overlays` : exit 0 et zéro `SKIP`.

---

## 12. Hors scope

- MQS ne **produit** pas d'overlays et ne valide pas la cohérence financière des
  transactions — seulement la conformité du format.
- Pas de filtrage côté serveur par query params : le catalogue part en une fois,
  le filtrage est client (latence de frappe nulle).
- **Multi-overlay simultané** : non implémenté, mais le modèle est prêt — la
  sélection est un tableau de clés (`selectedOverlayKeys`), les payloads sont
  indexés par clé et le rendu markers/canvas itère déjà sur N overlays avec une
  paire de couleurs par overlay. Passer au multi ne demande aucune évolution
  d'API (`/api/overlay/{stem}` reste appelé N fois).
