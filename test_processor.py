import io
import pandas as pd
from data_processor import process_sales_upload

try:
    with open("General Ledger 031826.xlsx", "rb") as f:
        gl = io.BytesIO(f.read())
        gl.name = "General Ledger 031826.xlsx"

    with open("Job Cost Query 031826.xlsx", "rb") as f:
        jc = io.BytesIO(f.read())
        jc.name = "Job Cost Query 031826.xlsx"

    with open("Job Summary of Billings and Cost 031826.xlsx", "rb") as f:
        js = io.BytesIO(f.read())
        js.name = "Job Summary of Billings and Cost 031826.xlsx"

    with open("Sales Analysis by Inventory Item 031826.xlsx", "rb") as f:
        sa = io.BytesIO(f.read())
        sa.name = "Sales Analysis by Inventory Item 031826.xlsx"

    with open("Sales Analysis by Sales Person 031826.xlsx", "rb") as f:
        sp = io.BytesIO(f.read())
        sp.name = "Sales Analysis by Sales Person 031826.xlsx"

    files = [gl, jc, js, sa, sp]
    print("Testing process_sales_upload...")
    success, msg = process_sales_upload(files, 2026, 3)
    print("Result:", success)
    print("Message:", msg)

except Exception as e:
    import traceback
    traceback.print_exc()
