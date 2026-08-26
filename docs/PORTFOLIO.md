# Analyse de portefeuille (MPT)

Commande CLI : `myquantstore portfolio …`

## Principes

- **Univers v1** : stocks configurés (track **1day** Yahoo).
- **Returns total-return** : prix split-adjusted (défaut query) + dividend adjust (`adjust_rollover`).
- **Fréquence** : `day` (défaut) ou `week` (resample).
- **Stack** : Polars (panel) + numpy + **scipy** (frontier QP). Pas de pandas / PyPortfolioOpt.
- **Optim** long-only \(\sum w=1\), \(w_i\ge 0\) via candidats analytiques projetés + tirages Dirichlet.
- **Frontière** : grille de **target-return** + SLSQP (`min w'Σw` s.t. `w'μ = target`), fallback Dirichlet si besoin.
- **RF dynamique** : par défaut Yahoo `^IRX` (13-week T-bill, close en % → /100). Override `--rf` ou `rf_source = "static"`.

## Sous-commandes

| Cmd | Description |
|---|---|
| `stats` | μ_ann, σ_ann, Sharpe par titre |
| `corr` | Matrice de corrélation |
| `cov` | Covariance annualisée |
| `optimize --objective equal\|min-vol\|max-sharpe` | Poids optimaux |
| `allocate --objective … [--value V]` | Lots entiers + cash + poids effectifs (`default_value` config) |
| `frontier [--points N] [--method qp\|sample]` | Frontière efficiente (QP target-return par défaut) |

### Allocation : `weights_eff` vs capital

- `weights_th` : cibles d’optim (somment à 1 sur le capital théorique).
- `weights_eff` : `notional_i / invested` (**hors cash**). Donc `Σ weights_eff = 1` sur les lots achetés, mais **pas** par rapport à `value` tant que `cash > 0`.
- Identité : `invested + cash = value`.

### Chart paniers (lazy)

Dashboard section Stocks : boutons **Max Sharpe** / **Min Vol** →
`/portfolio:max-sharpe` et `/portfolio:min-vol`.

- Optim calculée **au premier accès** (pas au boot).
- Cache mémoire process avec TTL `[chart] pf_optim_cache_ttl_days`
  (défaut **1** jour ; `0` = recalcul à chaque accès). Après expiration
  du TTL, le prochain `/api/candles` (ou meta/thumbnail) re-optimise.
- Série OHLCV : combinaison linéaire des legs sur la **barre de base**
  (1min ou 1day), puis resample UT ; **rebase 100** à t0. La série est
  toujours reconstruite à la demande ; seuls poids / métriques d'optim
  sont cachés.
- Invariant : jamais de combo sur barres déjà resamplées.

Flags communs : `--from`, `--to`, `--timescale day|week`, `--rf`, `--log-returns`, `--no-div`, `-i` (répétable), `--export path.parquet|.csv`.

Affichage stdout tronqué via `[display]` (`max_rows` / `max_columns`) — comme `query`/`status`. Export = matrice complète.

## Config `[portfolio]`

```toml
risk_free_rate = 0.04          # fallback static
rf_source = "yahoo"            # "yahoo" | "static"
rf_yahoo_ticker = "^IRX"       # 13w T-bill yield (%)
rf_cache_ttl_days = 1
trading_days_per_year = 252
min_coverage = 0.95
frontier_samples = 5000        # fallback Dirichlet / seeds optim
default_lookback_years = 5
optim_seed = 42
default_value = 20000.0
```

## Formules

- Returns simple : \(r_t = P_t/P_{t-1}-1\)
- Annualisation daily : \(\mu_{ann}=\bar r\cdot 252\), \(\sigma_{ann}=s\sqrt{252}\)
- Sharpe : \((\mu_p - r_f)/\sigma_p\)
- Min-vol : \(\min w^\top\Sigma w\)
- Max-Sharpe : \(\max (w^\top\mu - r_f)/\sqrt{w^\top\Sigma w}\)
- Frontier QP : pour chaque target \(\mu^\star\) sur une grille
  \([\mu_{\min\text{-vol}}, \max_i \mu_i]\) :
  \(\min w^\top\Sigma w\) s.t. \(w^\top\mu=\mu^\star\), \(\sum w=1\), \(w\ge 0\)

## Limites v1

- Sample covariance (pas Ledoit-Wolf)
- Long-only, pas de shorts / market-neutral
- Stocks only
- Biais de sélection de l’univers config (survivorship)
- RF Yahoo = dernier close ^IRX (pas de courbe de taux multi-maturités)

## Exemples

```bash
myquantstore portfolio stats
myquantstore portfolio optimize --objective min-vol
myquantstore portfolio optimize --objective max-sharpe -i AAPL -i NVDA -i COST
myquantstore portfolio allocate --objective min-vol --value 20000
myquantstore portfolio corr --from 2020-01-01 --export /tmp/corr.parquet
myquantstore portfolio frontier --timescale week --points 30
myquantstore portfolio frontier --method sample   # legacy Dirichlet
myquantstore chart   # boutons Max Sharpe / Min Vol dans Stocks
```
