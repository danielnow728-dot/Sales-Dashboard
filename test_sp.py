import pandas as pd

try:
    df_sp = pd.read_excel(r'C:\Users\danie\.gemini\antigravity\playground\Sale Meeting\Sales Analysis by Sales Person 031826.xlsx', header=None)
    
    # Let's find rows with salesperson
    mask = df_sp.astype(str).apply(lambda x: x.str.contains('Salesperson', case=False, na=False)).any(axis=1)
    print("--- RAW SALESPERSON HEADER ROWS ---")
    headers = df_sp[mask].head(5)
    for index, row in headers.iterrows():
        print(f"Row {index}:")
        for i in range(10):
            try:
                val = row[i]
                if pd.notna(val):
                    print(f"  Col {i}: '{val}'")
            except:
                pass
except Exception as e:
    print("Error:", e)
