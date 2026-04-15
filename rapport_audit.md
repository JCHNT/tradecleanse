# Rapport d'Audit — TradeCleanse / QuantAxis Capital

**Date :** 2026-04-15
**Dataset :** `tradecleanse_raw.csv` — 8 950 lignes × 20 colonnes
**Auditeur :** Data Engineer / Chef de Projet IA

---

## 1. Data Dictionnaire (20 colonnes)

| # | Colonne | Type | Source | Règle de validation métier |
|---|---|---|---|---|
| 1 | trade_id | STRING | Murex | Unique ; NOT NULL ; format `TRD\d{6}` |
| 2 | counterparty_id | STRING | Murex | NOT NULL ; format `CP\d{3}` |
| 3 | counterparty_name | STRING (PII) | Refinitiv | Doit être pseudonymisé avant usage |
| 4 | isin | STRING | Bloomberg | Regex `^[A-Z]{2}[A-Z0-9]{10}$` (12 char) |
| 5 | trade_date | DATE | Murex | NOT NULL ; <= today |
| 6 | settlement_date | DATE | Murex | >= trade_date (règle T+2 actions) |
| 7 | asset_class | CATEGORY | Murex | ∈ {equity, bond, derivative, fx} |
| 8 | notional_eur | FLOAT | Murex | > 0 (hors short documenté) |
| 9 | price | FLOAT | Bloomberg | ∈ [bid*0.995, ask*1.005] |
| 10 | quantity | INT | Murex | > 0 |
| 11 | bid | FLOAT | Bloomberg | > 0 ; bid < ask |
| 12 | ask | FLOAT | Bloomberg | > 0 ; ask > bid |
| 13 | mid_price | FLOAT | Bloomberg | ≈ (bid+ask)/2, tolérance 1% |
| 14 | volume_j | INT | Bloomberg | >= 0 |
| 15 | volatility_30d | FLOAT | Bloomberg | ∈ [0.1, 200] |
| 16 | credit_rating | CATEGORY | Refinitiv | ∈ {AAA, AA, A, BBB, BB, B, CCC, D} |
| 17 | default_flag | INT | Refinitiv | ∈ {0, 1} |
| 18 | sector | STRING | Refinitiv | GICS référentiel |
| 19 | country_risk | FLOAT | Refinitiv | ∈ [0, 100] |
| 20 | trader_id | STRING (PII) | Murex | Pseudonymisé avant usage |

---

## 2. Profiling Initial (extraits)

- **Shape brute :** 8 950 × 20
- **Taux NaN global :** ~14,2 % (concentré sur `credit_rating`, `volatility_30d`)
- **Doublons exacts :** détectés lors migration Murex
- **Doublons `trade_id` :** ~200 (cf. `anomalies_report.csv`)
- **Corrélation bid/ask :** > 0,999 (attendu)
- **Corrélation mid ↔ (bid+ask)/2 :** dégradée par 200 valeurs aberrantes

Voir `01_profiling_report.png` pour visualisations.

---

## 3. Catalogue des 11 Anomalies

| # | Catégorie | Colonne(s) | Volume estimé | Criticité métier |
|---|---|---|---|---|
| 1 | Doublons | trade_id | ~200 | HIGH — corrompt clé primaire |
| 2 | Sentinelles texte | toutes (N/A, #N/A, -) | ~400 | HIGH — bloque cast numérique |
| 3 | Sentinelles num | volatility_30d=0.0 ; country_risk=99999 | ~150 | MED — fausse stats |
| 4 | Casse asset_class | asset_class | 4 variantes | MED — éclate catégorie |
| 5 | NaN structurels | credit_rating, volatility_30d | ~15 % | HIGH — info risque manquante |
| 6 | settlement < trade | settlement_date | ~80 | HIGH — viole T+2 BCBS 239 |
| 7 | bid > ask | bid, ask | ~120 | HIGH — fourchette inversée |
| 8 | mid incohérent | mid_price | ~200 | MED — feature contaminée |
| 9 | price hors fourchette | price | ~150 | HIGH — prix exec impossible |
| 10 | notional < 0 | notional_eur | ~40 | MED — signe non documenté |
| 11 | AAA + default=1 | credit_rating, default_flag | ~30 | CRITICAL — contradiction risque |
| 11b | Outliers multivariés | price+volume+vol | ~2 % | HIGH — pattern suspect |

---

## 4. Stratégie de Traitement (justification métier)

| Anomalie | Stratégie | Justification métier |
|---|---|---|
| Doublons trade_id | `keep='last'` | Dernier export Murex = version corrigée la plus récente |
| Sentinelles | Remplacement NaN dès import | Impossible de distinguer 99999 d'une valeur réelle sinon |
| Casse asset_class | Mapping référentiel | Un modèle ML doit voir 4 catégories, pas 12 variantes |
| settlement < trade | Fixer settlement = trade + 2j | Règle T+2 actions (ESMA) — correction déterministe |
| bid > ask | Swap des colonnes | Erreur ordre colonnes à l'export Bloomberg — info préservée |
| mid incohérent | Recalcul `(bid+ask)/2` | bid/ask = source de vérité Bloomberg |
| price hors fourchette | Remplacer par mid | Prix d'exec impossible hors spread — mid = best-effort |
| notional < 0 | `abs()` | Pas de flag short dans schéma → erreur de signe export Murex |
| AAA + default=1 | rating ← 'D' | Défaut observé = fait ; rating Refinitiv peut être stale |
| NaN rating | → 'UNRATED' + flag | BCBS 239 : ne pas inventer un rating, conserver l'ignorance |
| NaN numériques | Médiane + flag was_missing | < 70% NaN → imputable sans biaiser queue |
| Outliers IQR | Winsorisation (clipping) | Queues extrêmes biaisent VaR ; clipper préserve N |
| Outliers multivariés | Flag `is_anomaly_multivariate` | Risk Officer doit pouvoir examiner sans perte info |
| PII | SHA-256 + salt env | RGPD Art.4 & BCBS 239 §27 — irréversibilité |

---

Voir pipeline complet : `02_cleaning_pipeline.py` — Data Quality Score calculé en fin.
