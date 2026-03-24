import sqlite3
import pandas as pd
import sys

try:
    conn = sqlite3.connect(r'C:\Users\danie\.gemini\antigravity\playground\Sale Meeting\sales_dashboard.db')
    df = pd.read_sql("SELECT * FROM sales_data", conn)
    print("ALL YEARS/MONTHS IN DB:")
    print(df.groupby(['year', 'month']).size())
    print("\nTotal Cost across whole DB:", df['cost'].sum())
    
    df_2026 = df[df['year'] == 2026]
    print("Total Cost in 2026:", df_2026['cost'].sum())
    
    # Check what df_cost parses to
    df_cost = pd.read_excel(r'C:\Users\danie\.gemini\antigravity\playground\Sale Meeting\Job Cost Query 031826.xlsx', skiprows=4)
    print("Raw df_cost Amount sum:", pd.to_numeric(df_cost['Amount'], errors='coerce').sum())
    
    df_cost = df_cost.dropna(subset=['Job'])
    df_cost = df_cost[~df_cost['Job'].astype(str).str.contains('Total', case=False, na=False)]
    df_cost = df_cost.dropna(subset=['Cost Code'])
    
    print("Cleaned df_cost Amount sum:", pd.to_numeric(df_cost['Amount'], errors='coerce').sum())
    
except Exception as e:
    print("Error:", e)
