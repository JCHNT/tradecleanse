# ============================================================
# TRADECLEANSE — NOTEBOOK 01 : Audit & Profiling Initial
# DCLE821 — QuantAxis Capital
# ============================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
import warnings
warnings.filterwarnings('ignore')

SENTINELS = ['N/A', '#N/A', '#VALUE!', 'n/a', 'NA', '-', 'nd', 'null',
             'NULL', 'None', '', ' ', '99999']

# ============================================================
# CELLULE 1 — Chargement multi-sources
# ============================================================
df = pd.read_csv(
    'data/tradecleanse_raw.csv',
    sep=',',
    encoding='utf-8',
    low_memory=False,
    na_values=SENTINELS,
    keep_default_na=True,
)
print(f"[LOAD] shape brute : {df.shape}")

# Simulation des 3 flux sources
bloomberg_cols = ['isin', 'bid', 'ask', 'mid_price', 'price',
                  'volume_j', 'volatility_30d']
murex_cols = ['trade_id', 'counterparty_id', 'trade_date', 'settlement_date',
              'asset_class', 'notional_eur', 'quantity', 'trader_id']
refinitiv_cols = ['counterparty_id', 'counterparty_name', 'credit_rating',
                  'default_flag', 'sector', 'country_risk']

df_bloomberg = df[bloomberg_cols].copy(); df_bloomberg['source'] = 'BLOOMBERG'
df_murex = df[murex_cols].copy(); df_murex['source'] = 'MUREX'
df_refinitiv = df[refinitiv_cols].copy(); df_refinitiv['source'] = 'REFINITIV'

# Simulation SQL Murex via sqlite in-memory
conn = sqlite3.connect(':memory:')
df_murex.to_sql('murex_trades', conn, if_exists='replace', index=False)
df_murex_sql = pd.read_sql('SELECT * FROM murex_trades', conn)
print(f"[SOURCES] Bloomberg={df_bloomberg.shape}, Murex={df_murex_sql.shape}, Refinitiv={df_refinitiv.shape}")

# ============================================================
# CELLULE 2 — Profiling initial
# ============================================================
print("\n=== PROFILING INITIAL ===")
print(f"Shape : {df.shape}")
print(f"\nTypes pandas :\n{df.dtypes}")

nan_stats = pd.DataFrame({
    'nan_count': df.isna().sum(),
    'nan_pct': (df.isna().sum() / len(df) * 100).round(2),
    'cardinality': df.nunique(),
})
print(f"\n--- NaN + cardinalité ---\n{nan_stats}")

num_cols = df.select_dtypes(include=[np.number]).columns
print(f"\n--- Stats descriptives num ---\n{df[num_cols].describe().T}")

cat_cols = ['asset_class', 'credit_rating', 'sector']
for c in cat_cols:
    if c in df.columns:
        print(f"\n--- value_counts {c} ---\n{df[c].value_counts(dropna=False).head(15)}")

n_exact_dup = df.duplicated().sum()
n_tid_dup = df.duplicated(subset=['trade_id']).sum()
print(f"\nDoublons exacts : {n_exact_dup}")
print(f"Doublons trade_id : {n_tid_dup}")

# Corrélation bid/ask/mid
corr_cols = ['bid', 'ask', 'mid_price', 'price']
num_subset = df[corr_cols].apply(pd.to_numeric, errors='coerce')
print(f"\n--- Corrélation bid/ask/mid/price ---\n{num_subset.corr().round(3)}")

# ============================================================
# CELLULE 3 — Détection des anomalies
# ============================================================
print("\n=== DÉTECTION ANOMALIES ===")
anomalies = []

for col in num_cols:
    s = pd.to_numeric(df[col], errors='coerce')
    # skip
anomalies.append({'type': 'duplicate', 'col': 'trade_id', 'count': int(n_tid_dup),
                  'criticity': 'HIGH — corrompt clé unique → biais aval'})

# Cast num for financial checks
for c in ['bid', 'ask', 'mid_price', 'price', 'notional_eur',
          'volatility_30d', 'country_risk', 'quantity', 'volume_j']:
    df[c] = pd.to_numeric(df[c], errors='coerce')

df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
df['settlement_date'] = pd.to_datetime(df['settlement_date'], errors='coerce')

n_settle = ((df['settlement_date'] < df['trade_date'])).sum()
anomalies.append({'type': 'incoherence_temporelle', 'col': 'settlement_date<trade_date',
                  'count': int(n_settle), 'criticity': 'HIGH — viole T+2 BCBS 239'})

n_bidask = ((df['bid'] > df['ask'])).sum()
anomalies.append({'type': 'incoherence_prix', 'col': 'bid>ask', 'count': int(n_bidask),
                  'criticity': 'HIGH — fourchette inversée impossible'})

theo = (df['bid'] + df['ask']) / 2
n_mid = (np.abs(df['mid_price'] - theo) > 0.01 * theo).sum()
anomalies.append({'type': 'incoherence_calc', 'col': 'mid_price',
                  'count': int(n_mid), 'criticity': 'MED — feature contaminée'})

lo = df['bid'] * 0.995; hi = df['ask'] * 1.005
n_px_out = ((df['price'] < lo) | (df['price'] > hi)).sum()
anomalies.append({'type': 'price_out_range', 'col': 'price',
                  'count': int(n_px_out), 'criticity': 'HIGH — prix exécution impossible'})

n_neg_not = (df['notional_eur'] < 0).sum()
anomalies.append({'type': 'notional_neg', 'col': 'notional_eur',
                  'count': int(n_neg_not), 'criticity': 'MED — notional signé non doc'})

n_aaa_def = ((df['credit_rating'].astype(str).isin(['AAA', 'AA', 'A'])) &
             (pd.to_numeric(df['default_flag'], errors='coerce') == 1)).sum()
anomalies.append({'type': 'rating_vs_default', 'col': 'credit_rating/default_flag',
                  'count': int(n_aaa_def), 'criticity': 'CRITICAL — contradiction rating/défaut'})

n_nan_vol = df['volatility_30d'].isna().sum()
n_nan_rating = df['credit_rating'].isna().sum()
anomalies.append({'type': 'nan', 'col': 'volatility_30d', 'count': int(n_nan_vol),
                  'criticity': 'MED — ~15% NaN'})
anomalies.append({'type': 'nan', 'col': 'credit_rating', 'count': int(n_nan_rating),
                  'criticity': 'HIGH — rating manquant = risque non évaluable'})

ac_variants = df['asset_class'].astype(str).str.strip().unique()
anomalies.append({'type': 'casse_inconsistante', 'col': 'asset_class',
                  'count': len(ac_variants),
                  'criticity': 'MED — variantes casse / abrev'})

# Outliers multivariés
from sklearn.ensemble import IsolationForest
mv = df[['price', 'volume_j', 'volatility_30d']].dropna()
if len(mv) > 10:
    iso = IsolationForest(contamination=0.02, random_state=42).fit(mv)
    n_mv_out = int((iso.predict(mv) == -1).sum())
    anomalies.append({'type': 'outlier_multivarie',
                      'col': 'price+volume_j+volatility_30d',
                      'count': n_mv_out, 'criticity': 'HIGH — pattern suspect multidim'})

anomalies_report = pd.DataFrame(anomalies)
print(anomalies_report.to_string())
anomalies_report.to_csv('anomalies_report.csv', index=False)

# ============================================================
# CELLULE 4 — Visualisations
# ============================================================
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

nan_pct = (df.isna().sum() / len(df) * 100).sort_values(ascending=True)
axes[0, 0].barh(nan_pct.index, nan_pct.values, color='indianred')
axes[0, 0].set_title('Taux NaN par colonne (%)')
axes[0, 0].set_xlabel('% NaN')

ac_counts = df['asset_class'].astype(str).value_counts().head(10)
axes[0, 1].bar(ac_counts.index, ac_counts.values, color='steelblue')
axes[0, 1].set_title('asset_class — variantes')
axes[0, 1].tick_params(axis='x', rotation=45)

axes[1, 0].scatter(df['bid'], df['ask'], s=4, alpha=0.4, c='navy')
mx = float(np.nanmax([df['bid'].max(), df['ask'].max()]))
axes[1, 0].plot([0, mx], [0, mx], 'r--', label='bid=ask')
axes[1, 0].set_title('bid vs ask (sous la diag = bid>ask impossible)')
axes[1, 0].set_xlabel('bid'); axes[1, 0].set_ylabel('ask'); axes[1, 0].legend()

delta = (df['settlement_date'] - df['trade_date']).dt.days
axes[1, 1].hist(delta.dropna(), bins=40, color='seagreen')
axes[1, 1].axvline(0, color='red', ls='--', label='seuil 0')
axes[1, 1].set_title('Delta settlement - trade (jours)')
axes[1, 1].legend()

plt.tight_layout()
plt.savefig('01_profiling_report.png', dpi=110)
print("\n[OK] 01_profiling_report.png généré")
print("[OK] anomalies_report.csv généré")
