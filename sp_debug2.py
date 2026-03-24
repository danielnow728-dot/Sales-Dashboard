import sqlite3
import pandas as pd

try:
    conn = sqlite3.connect(r'C:\Users\danie\.gemini\antigravity\playground\Sale Meeting\data\sales_dashboard.db')
    df = pd.read_sql("SELECT salesperson, SUM(invoiced) as Revenue, SUM(cost) as Cost FROM sales_records WHERE year=2026 GROUP BY salesperson", conn)
    print("--- DB ALLOCATIONS ---")
    print(df.to_string())
    
    df_sp = pd.read_excel(r'C:\Users\danie\.gemini\antigravity\playground\Sale Meeting\Sales Analysis by Sales Person 031826.xlsx', header=None)
    df_sp['SP_Letter'] = df_sp[1].where(df_sp[0].astype(str).str.contains('Salesperson', case=False, na=False)).ffill()
    invoices_sp = df_sp[df_sp[0].astype(str).str.contains(r'\d{2}/\d{2}/\d{2}', na=False)].copy()
    
    print("\n--- INVOICES SP_LETTER CHECK ---")
    print(invoices_sp[['SP_Letter', 8]].head(10).to_string())
    print("\nTotal Commissionable across all Invoices:", pd.to_numeric(invoices_sp[8], errors='coerce').sum())
    
    # Check HZ revenue
    hz_invoices = invoices_sp[invoices_sp['SP_Letter'].astype(str).str.contains('HZ', case=False, na=False)]
    print("Total HZ Commissionable:", pd.to_numeric(hz_invoices[8], errors='coerce').sum())
except Exception as e:
    print("Error:", e)
