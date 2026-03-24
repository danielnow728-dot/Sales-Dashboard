import sqlite3
import pandas as pd

try:
    conn = sqlite3.connect(r'C:\Users\danie\.gemini\antigravity\playground\Sale Meeting\sales_dashboard.db')
    df = pd.read_sql("SELECT salesperson, SUM(invoiced) as Revenue, SUM(cost) as Cost FROM sales_records WHERE year=2026 GROUP BY salesperson", conn)
    print("--- 2026 DATABASE AGGREGATES BY SALESPERSON ---")
    print(df.to_string())

    print("\n--- RAW HEADERS FROM SALESPERSON EXCEL FILE ---")
    df_sp = pd.read_excel(r'C:\Users\danie\.gemini\antigravity\playground\Sale Meeting\Sales Analysis by Sales Person 031826.xlsx', header=None)
    headers = df_sp[df_sp[0].astype(str).str.contains('Salesperson', case=False, na=False)]
    print(headers[[0, 1, 2]].to_string())

except Exception as e:
    print(f"Error: {e}")
