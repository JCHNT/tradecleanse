# ============================================================
# TRADECLEANSE — NOTEBOOK 03 : Validation du Dataset Nettoyé
# DCLE821 — QuantAxis Capital
# ============================================================

import pandas as pd
import numpy as np
import re
import warnings
warnings.filterwarnings('ignore')

df = pd.read_csv('data/tradecleanse_clean.csv', low_memory=False)
df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
df['settlement_date'] = pd.to_datetime(df['settlement_date'], errors='coerce')
print(f"Dataset nettoyé chargé : {df.shape[0]} lignes x {df.shape[1]} colonnes\n")

results = []
def check(name, cond, detail=''):
    status = 'PASS' if cond else 'FAIL'
    print(f"[{status}] {name} — {detail}")
    results.append({'expectation': name, 'status': status, 'detail': detail})

# 1. Unicité trade_id
n_dup = df.duplicated(subset=['trade_id']).sum()
check('1. trade_id unique', n_dup == 0, f"{n_dup} doublons")

# 2. Colonnes obligatoires non nulles
must = ['trade_id', 'counterparty_id', 'isin', 'trade_date',
        'asset_class', 'price', 'quantity', 'default_flag']
nans = df[must].isna().sum().sum()
check('2. Colonnes obligatoires non NaN', nans == 0, f"{nans} NaN totaux sur {must}")

# 3. settlement >= trade
bad = ((df['settlement_date'] < df['trade_date'])).sum()
check('3. settlement_date >= trade_date', bad == 0, f"{bad} violations")

# 4. bid < ask
bad = (df['bid'] >= df['ask']).sum()
check('4. bid < ask', bad == 0, f"{bad} violations")

# 5. price dans [bid*0.995, ask*1.005]
lo = df['bid'] * 0.995; hi = df['ask'] * 1.005
bad = ((df['price'] < lo) | (df['price'] > hi)).sum()
check('5. price in [bid*0.995, ask*1.005]', bad == 0, f"{bad} violations")

# 6. mid_price cohérent (tol 1%)
theo = (df['bid'] + df['ask']) / 2
bad = (np.abs(df['mid_price'] - theo) > 0.01 * theo.abs()).sum()
check('6. mid_price ~= (bid+ask)/2', bad == 0, f"{bad} violations")

# 7. asset_class référentiel
valid = {'equity', 'bond', 'derivative', 'fx'}
bad = (~df['asset_class'].isin(valid)).sum()
check('7. asset_class in référentiel', bad == 0, f"{bad} hors référentiel")

# 8. Pas de AAA/AA/A + default=1
mask = (df['credit_rating'].isin(['AAA', 'AA', 'A'])) & (df['default_flag'] == 1)
bad = mask.sum()
check('8. pas de contradiction rating/défaut', bad == 0, f"{bad} violations")

# 9. notional > 0
bad = (df['notional_eur'] <= 0).sum()
check('9. notional_eur > 0', bad == 0, f"{bad} violations")

# 10. country_risk 0-100
bad = ((df['country_risk'] < 0) | (df['country_risk'] > 100)).sum()
check('10. country_risk in [0,100]', bad == 0, f"{bad} violations")

# 11. ISIN regex
isin_re = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')
bad = (~df['isin'].astype(str).str.match(isin_re)).sum()
check('11. ISIN format valide', bad == 0, f"{bad} invalides")

# 12. volatility_30d in [0.1, 200]
bad = ((df['volatility_30d'] < 0.1) | (df['volatility_30d'] > 200)).sum()
check('12. volatility_30d in [0.1, 200]', bad == 0, f"{bad} violations")

# 13. Complétude > 90%
comp = 1 - df.isna().sum().sum() / (df.shape[0] * df.shape[1])
check('13. complétude > 90%', comp > 0.90, f"{comp*100:.2f}%")

# 14. PII absente
pii_present = any(c in df.columns for c in ['counterparty_name', 'trader_id'])
check('14. PII supprimées', not pii_present, "colonnes PII absentes")

n_pass = sum(1 for r in results if r['status'] == 'PASS')
print(f"\n═══════════════════════════════════")
print(f"  SCORE : {n_pass}/14 expectations passées")
print(f"═══════════════════════════════════")

pd.DataFrame(results).to_csv('ge_validation_report.csv', index=False)

html = ["<html><head><meta charset='utf-8'><title>Validation Report</title>",
        "<style>body{font-family:sans-serif;margin:2em}table{border-collapse:collapse}",
        "td,th{border:1px solid #ccc;padding:6px 12px}.PASS{color:green;font-weight:bold}",
        ".FAIL{color:red;font-weight:bold}</style></head><body>",
        f"<h1>TradeCleanse — Validation Report</h1>",
        f"<h2>Score : {n_pass}/14</h2>",
        "<table><tr><th>#</th><th>Expectation</th><th>Status</th><th>Detail</th></tr>"]
for i, r in enumerate(results, 1):
    html.append(f"<tr><td>{i}</td><td>{r['expectation']}</td>"
                f"<td class='{r['status']}'>{r['status']}</td><td>{r['detail']}</td></tr>")
html.append("</table></body></html>")
with open('validation_report.html', 'w') as f:
    f.write('\n'.join(html))
print("[OK] validation_report.html généré")
