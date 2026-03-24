import pandas as pd

df = pd.read_excel('General Ledger 031826.xlsx', header=None, nrows=15)
print(df.to_string())
