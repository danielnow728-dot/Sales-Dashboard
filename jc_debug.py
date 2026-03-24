import pandas as pd

try:
    df = pd.read_excel('Job Cost Query 031826.xlsx', skiprows=4)
    print("--- FIRST 20 ROWS ---")
    print(df.head(20).to_string())
    print("\n--- ANY ROWS CONTAINING 'TOTAL' ANYWHERE ---")
    # check if any cell contains 'total'
    mask = df.astype(str).apply(lambda x: x.str.contains('total', case=False, na=False)).any(axis=1)
    print(df[mask].head(20).to_string())
except Exception as e:
    print(f"Error: {e}")
