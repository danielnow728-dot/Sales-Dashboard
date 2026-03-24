import pandas as pd

try:
    df_sp = pd.read_excel(r'C:\Users\danie\.gemini\antigravity\playground\Sale Meeting\Sales Analysis by Sales Person 031826.xlsx', header=None)
    df_sp['SP_Letter'] = df_sp[1].where(df_sp[0].astype(str).str.contains('Salesperson', case=False, na=False)).ffill()
    
    invoices_sp = df_sp[df_sp[0].astype(str).str.contains(r'\d{2}/\d{2}/\d{2}', na=False)].copy()
    invoices_sp['Invoiced'] = pd.to_numeric(invoices_sp[8], errors='coerce').fillna(0)
    
    # Strip spaces
    invoices_sp['SP_Letter'] = invoices_sp['SP_Letter'].astype(str).str.strip().str.upper()
    
    totals = invoices_sp.groupby('SP_Letter')['Invoiced'].sum()
    print("--- RAW SALESPERSON COMMISSIONABLE TOTALS ---")
    print(totals)
    
except Exception as e:
    print("Error:", e)
