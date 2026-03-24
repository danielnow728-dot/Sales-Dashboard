import pandas as pd

df = pd.read_excel(r'C:\Users\danie\.gemini\antigravity\playground\Sale Meeting\General Ledger 031826.xlsx', header=None)
df = df[df[0].astype(str).str.contains(r'\d{2}/\d{2}/\d{2}', na=False)].copy()

print("General Ledger Columns:")
for i in range(15):
    try:
        print(f"Col {i}: {df[i].head(3).tolist()}")
    except KeyError:
        break
