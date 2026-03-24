import pandas as pd

files = [
    "General Ledger 031826.XLS",
    "Job Cost Query 031826.XLS",
    "Job Summary of Billings and Cost 031826.XLS",
    "Sales Analysis by Inventory Item 031826.XLS",
    "Sales Analysis by Sales Person 031826.XLS"
]

for f in files:
    output_name = f.replace('.XLS', '') + '_output.txt'
    try:
        df = pd.read_excel(f, engine='xlrd')
        with open(output_name, "w", encoding="utf-8") as out:
            out.write("Columns: " + str(list(df.columns)) + "\n\n")
            out.write("Head:\n")
            out.write(df.head(20).to_string())
    except Exception as e:
        with open(output_name, "w", encoding="utf-8") as out:
            out.write("Error: " + str(e))
