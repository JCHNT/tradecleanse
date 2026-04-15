# TradeCleanse — Projet de fin de module
## DCLE821 — Atelier Data Cleansing | EPSI Mastère Expert IA 2025-2026
### Client fictif : QuantAxis Capital (Hedge Fund — 2,3 Mds EUR d'actifs)

---

## Contexte

QuantAxis Capital exploite des stratégies de trading algorithmique sur
actions, obligations et dérivés. Le département Data Engineering a consolidé
trois flux de données dans un entrepôt unique pour entraîner un modèle de
scoring de risque de contrepartie.

La consolidation a révélé de sérieux problèmes de qualité.
Votre mission : auditer, nettoyer et certifier le dataset.

---

## Structure du projet

```
tradecleanse_projet/
├── data/
│   └── tradecleanse_raw.csv    ← Dataset brut — NE JAMAIS MODIFIER
├── 01_profiling.py             ← Audit initial et visualisations
├── 02_cleaning_pipeline.py     ← Pipeline de nettoyage complet
├── 03_validation.py            ← Suite de 14 tests de validation
├── 04_bonus_expert.py          ← Bonus : wash trading, drift, ML
├── requirements.txt
└── README.md
```

---

## Installation

```bash
python -m venv venv
source venv/bin/activate      # Linux / Mac
venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

---

## Ordre d'exécution

```bash
python 01_profiling.py          # Audit — à faire EN PREMIER
python 02_cleaning_pipeline.py  # Nettoyage — génère tradecleanse_clean.csv
python 03_validation.py         # Validation — nécessite tradecleanse_clean.csv
python 04_bonus_expert.py       # Bonus — nécessite tradecleanse_clean.csv
```

---

## Dataset — tradecleanse_raw.csv

**8 950 lignes × 20 colonnes**
Données de marché et transactions sur 180 jours (2024), 47 contreparties.

| Colonne | Type attendu | Source | Description |
|---|---|---|---|
| trade_id | STRING | Murex | Identifiant unique de la transaction |
| counterparty_id | STRING | Murex | Identifiant de la contrepartie |
| counterparty_name | STRING | Refinitiv | Nom de l'entreprise |
| isin | STRING | Bloomberg | Code ISIN (12 caractères) |
| trade_date | DATE | Murex | Date d'exécution du trade |
| settlement_date | DATE | Murex | Date de règlement (T+2 pour actions) |
| asset_class | CATEGORY | Murex | equity / bond / derivative / fx |
| notional_eur | FLOAT | Murex | Montant notionnel en euros |
| price | FLOAT | Bloomberg | Prix d'exécution |
| quantity | INTEGER | Murex | Quantité d'instruments |
| bid | FLOAT | Bloomberg | Prix acheteur |
| ask | FLOAT | Bloomberg | Prix vendeur |
| mid_price | FLOAT | Bloomberg | (bid + ask) / 2 théorique |
| volume_j | INTEGER | Bloomberg | Volume journalier |
| volatility_30d | FLOAT | Bloomberg | Volatilité historique 30j (%) |
| credit_rating | CATEGORY | Refinitiv | AAA / AA / A / BBB / BB / B / CCC / D |
| default_flag | INTEGER | Refinitiv | 1 = défaut dans les 90j, 0 = sain |
| sector | STRING | Refinitiv | Secteur GICS |
| country_risk | FLOAT | Refinitiv | Score risque pays (0-100) |
| trader_id | STRING | Murex | Identifiant du trader |

---

## Règles métier à respecter

Ces règles doivent guider chaque décision de nettoyage.
Toute correction doit être justifiée par une règle métier, pas seulement statistique.

| Colonne | Règle |
|---|---|
| trade_id | Doit être unique dans tout le dataset |
| settlement_date | Doit être >= trade_date (règle T+2) |
| bid / ask | bid doit toujours être < ask |
| mid_price | Doit être égal à (bid + ask) / 2 (tolérance 1%) |
| price | Doit se trouver dans [bid * 0.995, ask * 1.005] |
| notional_eur | Doit être > 0 pour une transaction standard |
| asset_class | Valeurs valides : equity, bond, derivative, fx |
| credit_rating | Valeurs valides : AAA AA A BBB BB B CCC D |
| country_risk | Doit être dans [0, 100] |
| volatility_30d | Doit être dans [0.1, 200] |
| default_flag | Valeurs valides : 0 ou 1 uniquement |
| credit_rating + default_flag | Un émetteur AAA/AA/A ne peut pas être en défaut |

---

## Livrables attendus

| # | Livrable | Points |
|---|---|---|
| 1 | Rapport d'audit (data dictionnaire + profiling + anomalies) | 4 |
| 2 | Pipeline Python documenté (02_cleaning_pipeline.py complet) | 10 |
| 3 | Suite de 14 validations avec score final | 3 |
| 4 | Note réglementaire BCBS 239 pour le Risk Officer (PDF, 2 pages max) | 3 |
| B1 | Bonus wash trading | +1 |
| B2 | Bonus data drift | +1 |
| B3 | Bonus impact ML | +1 |
| **TOTAL** | | **20 (+3)** |

---

## Contraintes obligatoires

- **Ne jamais modifier** `tradecleanse_raw.csv`
- **Toujours travailler** sur une copie : `df = pd.read_csv(...).copy()`
- **Logger chaque étape** : nb lignes avant / après / supprimées
- **Justifier chaque décision** en commentaire avec une raison métier
- **Pseudonymiser** toutes les colonnes PII avant tout entraînement ML
- Le dépôt Git doit avoir **au minimum 5 commits distincts**

---

## Référence réglementaire

**BCBS 239** — Principes pour l'agrégation des données de risque et le reporting
Texte officiel : https://www.bis.org/publ/bcbs239.pdf (gratuit, 32 pages)

Principes à couvrir dans votre note :
- Principe 2 : Exactitude et intégrité
- Principe 3 : Complétude
- Principe 6 : Adaptabilité
- Principe 8 : Exactitude (reporting)
