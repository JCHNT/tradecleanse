# Note Réglementaire BCBS 239 — Pipeline TradeCleanse

**Destinataire :** Risk Officer, QuantAxis Capital
**Émetteur :** Data Engineering / Chef de Projet IA
**Date :** 2026-04-15
**Objet :** Conformité du pipeline de qualité de données aux Principes BCBS 239

---

## Contexte

Suite au constat du responsable quantitatif (modèle prédisant risque nul sur
contreparties ayant fait défaut), un pipeline de nettoyage et de certification
a été mis en place sur le dataset consolidé Bloomberg + Murex + Refinitiv
(8 950 observations, 180 jours, 47 contreparties). La présente note démontre
la conformité de ce pipeline aux principes BCBS 239 applicables aux données
de risque.

---

## Principe 2 — Exactitude et intégrité

Le pipeline garantit l'exactitude par **13 contrôles métier déterministes**
appliqués à l'import :

- Fourchette de prix : `bid < ask` imposé par swap automatique (120 lignes)
- Prix d'exécution : clampé dans `[bid*0.995, ask*1.005]`, sinon remplacé
  par `mid_price` (150 lignes)
- `mid_price` recalculé à partir de `(bid+ask)/2`, source de vérité
  Bloomberg (200 lignes corrigées)
- Règle T+2 : `settlement_date` forcé ≥ `trade_date + 2j` (80 lignes)
- Contradiction `AAA/AA/A + default_flag=1` résolue : rating ← `D`
  (un défaut observé est un fait documenté, un rating Refinitiv peut être
  stale jusqu'à 30 jours)
- Référentiel `asset_class` normalisé à 4 valeurs (mapping exhaustif)

L'intégrité est assurée par l'**immutabilité du dataset brut** :
`tradecleanse_raw.csv` n'est jamais modifié — toutes transformations
s'opèrent sur une copie `df = df_raw.copy()`.

---

## Principe 3 — Complétude

Taux de complétude avant / après pipeline :

| Métrique | Avant | Après |
|---|---|---|
| Complétude globale | ~85,8 % | > 99 % |
| NaN sur colonnes obligatoires | ~1 500 | 0 |
| Doublons `trade_id` | ~200 | 0 |

**Méthode d'imputation différenciée par colonne :**
- `< 70 % NaN` : médiane (numérique) ou mode (catégoriel) + colonne
  `<col>_was_missing` pour traçabilité
- `credit_rating` manquant → `UNRATED` (jamais inventé — BCBS 239
  exige de signaler l'ignorance plutôt que de fabriquer une donnée)
- `trade_id` manquant → suppression de ligne (clé métier obligatoire)
- `settlement_date` NaT → `trade_date + 2j` (déterministe)

Complétude validée par **14 expectations automatisées** (cf.
`validation_report.html`).

---

## Principe 6 — Adaptabilité

Le pipeline est conçu modulaire :
- 10 étapes indépendantes et loggées (`tradecleanse_pipeline.log`)
- Une classe de flux par source (Bloomberg / Murex / Refinitiv) avec
  colonne `source` en consolidation — ajout d'une 4e source trivial
- Les règles métier sont centralisées (mapping `asset_class`, seuils IQR,
  liste sentinelles) → paramétrage sans modification du code
- Portabilité Prefect/Airflow directe (chaque étape = fonction pure)

Le pipeline s'exécute aussi bien en traitement batch (mensuel) qu'en
streaming (intégration micro-batch quotidien).

---

## Principe 8 — Exactitude du reporting (traçabilité)

Chaque étape produit :
1. **Log horodaté** (`tradecleanse_pipeline.log`) : `lignes_avant`,
   `lignes_après`, `NaN_avant`, `NaN_après`, décision prise
2. **Flag de modification** sur colonnes imputées (`*_was_missing`) —
   chaque ligne sait ce qu'elle a subi
3. **Pseudonymisation SHA-256** des PII (`counterparty_name`, `trader_id`)
   avec salt variable d'environnement (RGPD Art.4 & BCBS 239 §27)
4. **Rapport HTML** (`validation_report.html`) généré à chaque run
5. **Data Quality Score** calculé : `DQS = (complétude·0.6 + unicité·0.4)·100`

Ces artefacts permettent à la fonction Audit Interne et au régulateur
(ACPR) de reconstituer chaque transformation ligne à ligne, conformément
aux exigences du §27 (gouvernance) et §36 (traçabilité) de BCBS 239.

---

## Conclusion

Le pipeline TradeCleanse satisfait les Principes 2, 3, 6 et 8 de BCBS 239.
Un Data Quality Score supérieur à 95 % est atteint sur le dataset nettoyé,
et les 14 expectations de validation passent à 100 %. Le dataset est
certifié apte à l'entraînement du modèle de scoring de risque de
contrepartie, sous réserve d'un monitoring continu du data drift
(cf. `04_bonus_expert.py`).
