# ============================================================
# TRADECLEANSE — NOTEBOOK 04 : Bonus Expert
# DCLE821 — QuantAxis Capital
# ============================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import ks_2samp
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (roc_auc_score, precision_score, recall_score,
                             f1_score, roc_curve)
import warnings
warnings.filterwarnings('ignore')

df_raw = pd.read_csv('data/tradecleanse_raw.csv', low_memory=False)
df_clean = pd.read_csv('data/tradecleanse_clean.csv', low_memory=False)
df_clean['trade_date'] = pd.to_datetime(df_clean['trade_date'], errors='coerce')

# ============================================================
# BONUS 1 — Wash Trading
# ============================================================
print("\n=== BONUS 1 — Wash Trading ===")
# Critères cumulatifs : même ISIN, même trader_id_hash, même trade_date,
# qty delta < 5%, price delta < 0.1%. Signal : MAR Art.12 (manipulation).
df_wt = df_clean.dropna(subset=['isin', 'trader_id_hash', 'trade_date',
                                 'quantity', 'price'])
df_wt = df_wt.sort_values(['isin', 'trader_id_hash', 'trade_date'])

suspects = []
grouped = df_wt.groupby(['isin', 'trader_id_hash', 'trade_date'])
for (isin, th, td), grp in grouped:
    if len(grp) < 2:
        continue
    rows = grp.to_dict('records')
    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            r1, r2 = rows[i], rows[j]
            q1, q2 = r1['quantity'], r2['quantity']
            p1, p2 = r1['price'], r2['price']
            if q1 == 0 or p1 == 0:
                continue
            dq = abs(q1 - q2) / max(abs(q1), 1e-9)
            dp = abs(p1 - p2) / max(abs(p1), 1e-9)
            if dq < 0.05 and dp < 0.001:
                suspects.append({
                    'trade_id_1': r1['trade_id'], 'trade_id_2': r2['trade_id'],
                    'isin': isin, 'trader_hash': th, 'trade_date': td,
                    'delta_price_pct': round(dp * 100, 4),
                    'delta_qty_pct': round(dq * 100, 4),
                })

wt_suspects = pd.DataFrame(suspects)
print(f"Paires wash-trading suspectes : {len(wt_suspects)}")
wt_suspects.to_csv('wash_trading_suspects.csv', index=False)
# Interprétation : paires à même trader/ISIN/jour avec qty et prix quasi
# identiques = signal fort de self-trade gonflant les volumes (MAR Art.12).

# ============================================================
# BONUS 2 — Data Drift
# ============================================================
print("\n=== BONUS 2 — Data Drift (KS 2-sample) ===")
dmin = df_clean['trade_date'].min()
dmax = df_clean['trade_date'].max()
cut_early = dmin + pd.Timedelta(days=90)
cut_late = dmax - pd.Timedelta(days=90)

early = df_clean[df_clean['trade_date'] < cut_early]
late = df_clean[df_clean['trade_date'] >= cut_late]

drift_rows = []
vars_ = ['price', 'volatility_30d', 'notional_eur', 'volume_j', 'country_risk']
fig, axes = plt.subplots(1, len(vars_), figsize=(22, 4))
for i, v in enumerate(vars_):
    a = early[v].dropna(); b = late[v].dropna()
    stat, p = ks_2samp(a, b)
    drift = p < 0.05
    drift_rows.append({'variable': v, 'ks_stat': round(stat, 4),
                       'p_value': round(p, 6), 'drift': 'YES' if drift else 'NO'})
    axes[i].hist(a, bins=40, alpha=0.5, label='early', color='steelblue', density=True)
    axes[i].hist(b, bins=40, alpha=0.5, label='late', color='indianred', density=True)
    axes[i].set_title(f'{v}\nKS={stat:.3f} p={p:.3g}' + (' DRIFT' if drift else ''))
    axes[i].legend()

plt.tight_layout(); plt.savefig('04_drift_monitor.png', dpi=110); plt.close()
drift_df = pd.DataFrame(drift_rows)
print(drift_df.to_string())
drift_df.to_csv('drift_report.csv', index=False)

# ============================================================
# BONUS 3 — Impact ML
# ============================================================
print("\n=== BONUS 3 — Impact Random Forest ===")
feats = ['price', 'quantity', 'bid', 'ask', 'mid_price', 'volume_j',
         'volatility_30d', 'country_risk']

def prep(d):
    X = d[feats].apply(pd.to_numeric, errors='coerce').fillna(0)
    y = pd.to_numeric(d['default_flag'], errors='coerce').fillna(0).astype(int)
    return X, y

fig, ax = plt.subplots(figsize=(8, 6))
model_rows = []
for label, d in [('Brut', df_raw), ('Nettoye', df_clean)]:
    X, y = prep(d)
    if y.nunique() < 2:
        continue
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2,
                                          random_state=42, stratify=y)
    clf = RandomForestClassifier(n_estimators=150, max_depth=6,
                                 random_state=42, n_jobs=-1)
    clf.fit(Xtr, ytr)
    proba = clf.predict_proba(Xte)[:, 1]
    pred = clf.predict(Xte)
    auc = roc_auc_score(yte, proba)
    prec = precision_score(yte, pred, zero_division=0)
    rec = recall_score(yte, pred, zero_division=0)
    f1 = f1_score(yte, pred, zero_division=0)
    print(f"[{label}] AUC={auc:.4f} | P={prec:.4f} | R={rec:.4f} | F1={f1:.4f}")
    model_rows.append({'dataset': label, 'auc': round(auc, 4),
                       'precision': round(prec, 4),
                       'recall': round(rec, 4), 'f1': round(f1, 4)})
    fpr, tpr, _ = roc_curve(yte, proba)
    ax.plot(fpr, tpr, label=f"{label} AUC={auc:.3f}", lw=2)

ax.plot([0, 1], [0, 1], 'k--', alpha=0.3)
ax.set_xlabel('FPR'); ax.set_ylabel('TPR')
ax.set_title('ROC — Brut vs Nettoyé')
ax.legend()
plt.tight_layout(); plt.savefig('04_roc_comparison.png', dpi=110); plt.close()

pd.DataFrame(model_rows).to_csv('model_comparison.csv', index=False)

print("""
ANALYSE :
Le dataset nettoyé élimine les incohérences bid/ask, prix hors fourchette et
contradictions rating/défaut qui créaient du bruit dans les features. Gain AUC
attendu 3-8 pts — modéré car le signal principal (default_flag) est peu corrélé
aux features de marché directes. Pour aller plus loin : feature engineering
(spread, liquidité, historique), rééquilibrage SMOTE, et ajout features
macro pour capturer mieux risque de défaut.
""")
