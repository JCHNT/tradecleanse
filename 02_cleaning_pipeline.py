# ============================================================
# TRADECLEANSE — NOTEBOOK 02 : Pipeline de Nettoyage Complet
# DCLE821 — QuantAxis Capital
# ============================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
import hashlib
import os
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('tradecleanse_pipeline.log', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

SENTINELS = ['N/A', '#N/A', '#VALUE!', 'n/a', 'NA', '-', 'nd', 'null',
             'NULL', 'None', '', ' ', '99999', '#DIV/0!']

# ============================================================
# CHARGEMENT (immutabilité dataset brut → copie)
# ============================================================
df_raw = pd.read_csv('data/tradecleanse_raw.csv', low_memory=False)
df = df_raw.copy()
logger.info(f"Dataset chargé : {df.shape[0]} lignes, {df.shape[1]} colonnes")

step_log = []
def log_step(name, before_rows, before_nans, df_now):
    after_rows = len(df_now)
    after_nans = int(df_now.isna().sum().sum())
    step_log.append({'etape': name, 'lignes_avant': before_rows,
                     'lignes_apres': after_rows,
                     'nans_avant': before_nans, 'nans_apres': after_nans})
    logger.info(f"[{name}] lignes {before_rows}->{after_rows} | NaN {before_nans}->{after_nans}")

# ============================================================
# ÉTAPE 1 — Remplacement des valeurs sentinelles
# ============================================================
# Règle métier : "N/A", "#N/A", "-", "99999", "0.0" sur volatility_30d sont
# des codes "valeur manquante" hérités d'exports Bloomberg/Murex.
# On les convertit en NaN avant tout traitement numérique.
before_rows, before_nans = len(df), int(df.isna().sum().sum())
df = df.replace(SENTINELS, np.nan)
# 0.0 sur volatility_30d = sentinelle (volatilité nulle impossible sur 30j)
df.loc[pd.to_numeric(df['volatility_30d'], errors='coerce') == 0.0, 'volatility_30d'] = np.nan
# 99999 sur country_risk = sentinelle
df.loc[pd.to_numeric(df['country_risk'], errors='coerce') == 99999, 'country_risk'] = np.nan
log_step('Sentinelles', before_rows, before_nans, df)

# ============================================================
# ÉTAPE 2 — Suppression des doublons
# ============================================================
# Justification : dans Murex, l'ordre d'export est chronologique.
# La DERNIÈRE ligne pour un trade_id donné reflète la correction
# la plus récente → keep='last'.
before_rows, before_nans = len(df), int(df.isna().sum().sum())
n_exact = df.duplicated().sum()
df = df.drop_duplicates()
n_tid = df.duplicated(subset=['trade_id']).sum()
df = df.drop_duplicates(subset=['trade_id'], keep='last')
logger.info(f"Doublons exacts : {n_exact} | Doublons trade_id : {n_tid}")
log_step('Doublons', before_rows, before_nans, df)

# ============================================================
# ÉTAPE 3 — Conversion et normalisation des types
# ============================================================
before_rows, before_nans = len(df), int(df.isna().sum().sum())
df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
df['settlement_date'] = pd.to_datetime(df['settlement_date'], errors='coerce')

num_cols = ['bid', 'ask', 'mid_price', 'price', 'notional_eur',
            'quantity', 'volume_j', 'volatility_30d', 'country_risk',
            'default_flag']
for c in num_cols:
    df[c] = pd.to_numeric(df[c], errors='coerce')

for c in ['asset_class', 'credit_rating', 'sector']:
    df[c] = df[c].astype(str).str.strip().str.lower().replace('nan', np.nan)

log_step('Types', before_rows, before_nans, df)

# ============================================================
# ÉTAPE 4 — Normalisation asset_class
# ============================================================
before_rows, before_nans = len(df), int(df.isna().sum().sum())
asset_map = {
    'equity': 'equity', 'equities': 'equity', 'eq': 'equity', 'stock': 'equity',
    'stocks': 'equity', 'action': 'equity', 'actions': 'equity',
    'bond': 'bond', 'bonds': 'bond', 'bd': 'bond', 'obligation': 'bond',
    'obligations': 'bond', 'fixed income': 'bond',
    'derivative': 'derivative', 'derivatives': 'derivative',
    'derivs': 'derivative', 'deriv': 'derivative', 'drv': 'derivative',
    'fx': 'fx', 'forex': 'fx', 'foreign exchange': 'fx', 'fx spot': 'fx',
}
df['asset_class'] = df['asset_class'].map(asset_map)
valid_ac = {'equity', 'bond', 'derivative', 'fx'}
logger.info(f"asset_class distinct après : {df['asset_class'].dropna().unique().tolist()}")
log_step('asset_class', before_rows, before_nans, df)

# ============================================================
# ÉTAPE 5 — Incohérences structurelles financières
# ============================================================
before_rows, before_nans = len(df), int(df.isna().sum().sum())

# 5a. settlement < trade → fixer à trade_date + 2j (règle T+2 actions)
mask = df['settlement_date'] < df['trade_date']
n5a = int(mask.sum())
df.loc[mask, 'settlement_date'] = df.loc[mask, 'trade_date'] + pd.Timedelta(days=2)
logger.info(f"5a settlement<trade corrigés : {n5a}")

# 5b. bid > ask → swap (erreur d'ordre de colonne à l'export)
mask = df['bid'] > df['ask']
n5b = int(mask.sum())
df.loc[mask, ['bid', 'ask']] = df.loc[mask, ['ask', 'bid']].values
logger.info(f"5b bid>ask swaps : {n5b}")

# 5c. mid_price incohérent → recalcul (source de vérité = bid+ask)
theo = (df['bid'] + df['ask']) / 2
mask = (df['mid_price'].isna()) | (np.abs(df['mid_price'] - theo) > 0.01 * theo.abs())
n5c = int(mask.sum())
df.loc[mask, 'mid_price'] = theo[mask]
logger.info(f"5c mid_price recalculés : {n5c}")

# 5d. price hors [bid*0.995, ask*1.005] → remplacement par mid (impossible opérationnellement)
lo = df['bid'] * 0.995; hi = df['ask'] * 1.005
mask = (df['price'] < lo) | (df['price'] > hi)
n5d = int(mask.sum())
df.loc[mask, 'price'] = df.loc[mask, 'mid_price']
logger.info(f"5d price hors fourchette → mid : {n5d}")

# 5e. notional négatif → abs (pas de flag short dans le dataset → on
# considère qu'il s'agit d'une erreur de signe à l'export Murex)
mask = df['notional_eur'] < 0
n5e = int(mask.sum())
df.loc[mask, 'notional_eur'] = df.loc[mask, 'notional_eur'].abs()
logger.info(f"5e notional<0 → abs : {n5e}")

# 5f. rating AAA/AA/A + default_flag=1 → on fait confiance au flag
# (un défaut observé est un fait, un rating Refinitiv peut être stale)
mask = (df['credit_rating'].isin(['aaa', 'AAA', 'aa', 'AA', 'a', 'A']) &
        (df['default_flag'] == 1))
# normalisation rating majuscule
df['credit_rating'] = df['credit_rating'].astype(str).str.upper().replace('NAN', np.nan)
mask = (df['credit_rating'].isin(['AAA', 'AA', 'A'])) & (df['default_flag'] == 1)
n5f = int(mask.sum())
df.loc[mask, 'credit_rating'] = 'D'  # défaut observé = notation D
logger.info(f"5f rating AAA/AA/A + default=1 → D : {n5f}")

log_step('Incoherences', before_rows, before_nans, df)

# ============================================================
# ÉTAPE 6 — Règles métier
# ============================================================
before_rows, before_nans = len(df), int(df.isna().sum().sum())
df.loc[(df['country_risk'] < 0) | (df['country_risk'] > 100), 'country_risk'] = np.nan
df.loc[(df['volatility_30d'] < 0.1) | (df['volatility_30d'] > 200), 'volatility_30d'] = np.nan
df.loc[~df['default_flag'].isin([0, 1]), 'default_flag'] = np.nan
df.loc[df['quantity'] <= 0, 'quantity'] = np.nan
log_step('Regles_metier', before_rows, before_nans, df)

# ============================================================
# ÉTAPE 7 — Outliers (IQR + Isolation Forest)
# ============================================================
from sklearn.ensemble import IsolationForest
before_rows, before_nans = len(df), int(df.isna().sum().sum())

fig, axes = plt.subplots(1, 3, figsize=(15, 4))
for i, c in enumerate(['notional_eur', 'volatility_30d', 'volume_j']):
    axes[i].boxplot(df[c].dropna())
    axes[i].set_title(f'Boxplot {c} (avant)')
    q1, q3 = df[c].quantile([0.25, 0.75])
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    n_out = int(((df[c] < lo) | (df[c] > hi)).sum())
    logger.info(f"IQR {c}: bounds=[{lo:.2f}, {hi:.2f}] outliers={n_out}")
    # Stratégie : winsorisation (clipping). Justification métier :
    # en finance quantitative, supprimer des queues extrêmes biaise
    # l'estimation de la VaR. Clipper préserve la taille d'échantillon.
    df[c] = df[c].clip(lower=lo, upper=hi)

plt.tight_layout(); plt.savefig('02_outliers_boxplots.png', dpi=110); plt.close()

# Isolation Forest multivariée (flag, pas suppression)
mv_cols = ['price', 'volume_j', 'volatility_30d', 'notional_eur']
mv_df = df[mv_cols].copy().fillna(df[mv_cols].median())
iso = IsolationForest(contamination=0.02, random_state=42, n_estimators=200)
df['is_anomaly_multivariate'] = (iso.fit_predict(mv_df) == -1).astype(int)
n_mv = int(df['is_anomaly_multivariate'].sum())
logger.info(f"Isolation Forest : {n_mv} lignes flaggées (2%)")

log_step('Outliers', before_rows, before_nans, df)

# ============================================================
# ÉTAPE 8 — Valeurs manquantes
# ============================================================
before_rows, before_nans = len(df), int(df.isna().sum().sum())
nan_rates = (df.isna().sum() / len(df)).sort_values(ascending=False)
logger.info(f"NaN rates pré-imputation :\n{nan_rates[nan_rates>0]}")

# trade_id NaN → drop (clé métier obligatoire)
n_drop_tid = int(df['trade_id'].isna().sum())
df = df.dropna(subset=['trade_id'])
logger.info(f"trade_id NaN supprimés : {n_drop_tid}")

# settlement_date NaT → trade_date + 2j (règle T+2)
mask = df['settlement_date'].isna() & df['trade_date'].notna()
df.loc[mask, 'settlement_date'] = df.loc[mask, 'trade_date'] + pd.Timedelta(days=2)

# Numériques < 70% NaN → médiane + flag was_missing
num_impute = ['bid', 'ask', 'mid_price', 'price', 'notional_eur', 'quantity',
              'volume_j', 'volatility_30d', 'country_risk']
for c in num_impute:
    rate = df[c].isna().mean()
    if rate > 0.7:
        logger.warning(f"{c} >70% NaN → colonne droppée")
        df = df.drop(columns=[c])
        continue
    if rate > 0:
        df[f'{c}_was_missing'] = df[c].isna().astype(int)
        df[c] = df[c].fillna(df[c].median())

# credit_rating → imputation "unrated" (mettre le mode masquerait le risque)
# Règle BCBS 239 : conserver information de manquance plutôt qu'inventer rating.
df['credit_rating_was_missing'] = df['credit_rating'].isna().astype(int)
df['credit_rating'] = df['credit_rating'].fillna('UNRATED')

# asset_class NaN → mode (pas d'alternative, suppression = perte trade)
df['asset_class_was_missing'] = df['asset_class'].isna().astype(int)
df['asset_class'] = df['asset_class'].fillna(df['asset_class'].mode()[0])

# sector NaN → "unknown"
df['sector'] = df['sector'].fillna('unknown')

# default_flag NaN → 0 (absence d'info = pas de défaut observé, conservateur pour ML)
df['default_flag'] = df['default_flag'].fillna(0).astype(int)

log_step('Valeurs_manquantes', before_rows, before_nans, df)

# ============================================================
# ÉTAPE 9 — Pseudonymisation RGPD
# ============================================================
# PII : counterparty_name (RGPD art.4 — donnée indirecte), trader_id
# (donnée interne sensible, BCBS 239 §27 accès restreint).
before_rows, before_nans = len(df), int(df.isna().sum().sum())
salt = os.environ.get('CLEANSE_SALT', 'default_salt_dev')

def hash_val(v):
    if pd.isna(v): return np.nan
    return hashlib.sha256(f"{salt}|{v}".encode()).hexdigest()[:16]

for c in ['counterparty_name', 'trader_id']:
    if c in df.columns:
        df[f'{c}_hash'] = df[c].apply(hash_val)
        df = df.drop(columns=[c])
        logger.info(f"PII pseudonymisée : {c} → {c}_hash")

log_step('Pseudonymisation', before_rows, before_nans, df)

# ============================================================
# ÉTAPE 10 — Rapport qualité final
# ============================================================
completude = 1 - df.isna().sum().sum() / (df.shape[0] * df.shape[1])
unicite = 1 - df.duplicated(subset=['trade_id']).mean()
dqs = (completude * 0.6 + unicite * 0.4) * 100

before_shape = df_raw.shape
after_shape = df.shape
raw_comp = 1 - df_raw.isna().sum().sum() / (df_raw.shape[0] * df_raw.shape[1])

report = f"""
╔══════════════════════════════════════════════════╗
║       RAPPORT QUALITÉ — AVANT / APRÈS            ║
╠══════════════════════════════════════════════════╣
║ Shape brut    : {str(before_shape):>30} ║
║ Shape clean   : {str(after_shape):>30} ║
║ Complétude raw: {raw_comp*100:>27.2f} % ║
║ Complétude clean: {completude*100:>25.2f} % ║
║ Unicité trade_id: {unicite*100:>24.2f} % ║
║ Data Quality Score : {dqs:>24.2f}    ║
╚══════════════════════════════════════════════════╝
"""
print(report)
logger.info(report)

step_df = pd.DataFrame(step_log)
step_df.to_csv('pipeline_steps.csv', index=False)
print(step_df.to_string())

df.to_csv('data/tradecleanse_clean.csv', index=False)
logger.info(f"Dataset nettoyé sauvegardé : data/tradecleanse_clean.csv")
print(f"Shape finale : {df.shape}")
