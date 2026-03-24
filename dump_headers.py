import pandas as pd
import glob
import sys

def scan_files():
    files = glob.glob("*.xlsx")
    if not files:
        print("No .xlsx files found in this directory.")
        return
        
    for f in files:
        print(f"\n==========================================")
        print(f"FILE: {f}")
        try:
            # We read the first 15 rows without a header to see the raw layout
            df = pd.read_excel(f, header=None, nrows=15)
            print("--- First 15 rows (Raw Layout) ---")
            print(df.to_string())
        except Exception as e:
            print(f"Error reading {f}: {e}")

if __name__ == "__main__":
    scan_files()
