import pandas as pd
from database import SessionLocal, SalesRecord, UploadLog, BacklogSnapshot, BudgetRecord
from datetime import datetime

def identify_files(uploaded_files):
    files_map = {'job_summary': None, 'gl': None, 'sales_person': None, 'inventory_item': None, 'job_cost': None}
    for f in uploaded_files:
        name = f.name.lower()
        if 'summary' in name or 'billings' in name: files_map['job_summary'] = f
        elif 'general ledger' in name: files_map['gl'] = f
        elif 'sales person' in name: files_map['sales_person'] = f
        elif 'inventory item' in name: files_map['inventory_item'] = f
        elif 'job cost' in name: files_map['job_cost'] = f
    return files_map

import re

def map_salesperson(val):
    val = str(val).upper().strip()
    mapping = {
        'B': 'BRANDON MCGINNIS', 'BF': 'BRANDON MCGINNIS', 'C': 'MARCOS CARMONA', 'D': 'HOUSE',
        'F': 'HOUSE', 'G': 'HOUSE', 'GI': 'GREG CHOMENKO', 'GN': 'DANIEL LOPEZ', 'GW': 'DANIEL LOPEZ',
        'HZ': 'HOUSE', 'L': 'ANTHONY LOPEZ', 'M': 'HOUSE', 'N': 'DANIEL LOPEZ', 'NF': 'DANIEL LOPEZ',
        'P': 'ALEX GAVALDON', 'W': 'HOUSE', 'Z': 'DAVE MCCOMBS'
    }
    prefix = val.split('-')[0].strip() if '-' in val else ''.join(filter(str.isalpha, val))
    return mapping.get(prefix, 'HOUSE')

KNOWN_SALESPEOPLE = {
    'ALEX GAVALDON', 'ANTHONY LOPEZ', 'BRANDON MCGINNIS',
    'DANIEL LOPEZ', 'DAVE MCCOMBS', 'GREG CHOMENKO', 'MARCOS CARMONA',
}

def get_salesperson_from_job(job_str):
    job_str = str(job_str).upper()
    match = re.search(r'-([A-Z]+)', job_str)
    if match:
        return map_salesperson(match.group(1))
    return 'HOUSE'

def salesperson_from_meta(job_num, meta):
    """Use Job Summary project_manager if it's a known salesperson, else fall back to job number."""
    pm = str(meta.get('project_manager', '')).strip().upper()
    if pm in KNOWN_SALESPEOPLE:
        return pm
    return get_salesperson_from_job(job_num)

def norm_text(x) -> str:
    if pd.isna(x): return ""
    return re.sub(r"\s+", " ", str(x).strip()).upper()

def is_blank(x):
    if pd.isna(x): return True
    return str(x).strip() == ""

def find_header_row(file_obj, required: list, scan=350) -> int:
    preview = pd.read_excel(file_obj, header=None, nrows=scan)
    req = [norm_text(r) for r in required]
    for i in range(len(preview)):
        row = [norm_text(v) for v in preview.iloc[i].tolist()]
        if all(any(r == cell for cell in row) for r in req):
            return i
    return 0

INTERNAL_CATEGORY_MAP = {
    "RETENTION WITHHELD": "Other", "LABOR - YARD": "Labor", "LABOR - GENERAL MAINTENANCE": "Labor",
    "LABOR - INSULATION": "Labor", "LABOR - ASBESTOS": "Labor", "CONTROLLED INSURANCE DISCOUNT": "Other",
    "LABOR - SCAFFOLD": "Labor", "DEDUCT": "Other", "SWING STAGE - LABOR": "Labor",
    "SWING STAGE - RENTAL": "Rent", "SWING STAGE - DELIVERY/PICK-UP": "Delivery",
    "SWING STAGE - ENG. / DRAWINGS": "Sub", "SWING STAGE - PERM.EQUIP.SALE": "Material",
    "SWING STAGE - CONSUMABLES": "Material", "SWING STAGE - SAFETY EQUIPMENT": "Material",
    "SWING STAGE - INDIRECT COST": "Material", "CHANGE ORDER": "Other", "ENGINEERING / DRAWINGS": "Sub",
    "SWING STAGE - INSPECTIONS": "Labor", "PERMANENT SCAFFOLD SALE": "Material", "EQUIPMENT": "Material",
    "MOBILIZATION": "Labor", "DE-MOBILIZATION": "Labor", "SCAFFOLD RENTAL": "Rent",
    "CONSUMABLES / DIRECT MATERIAL": "Material", "LABOR - SCAFFOLD INSPECTIONS": "Labor",
    "SWITCH RAIL - LABOR": "Labor", "SWITCH RAIL - RENTAL": "Rent", "SWITCH RAIL - DELIVERY/PICK UP": "Delivery",
    "SITE TRUCK": "Delivery", "DELIVERY / PICK-UP": "Delivery", "TRASH CHUTE RENTAL": "Rent",
    "PER DIEM": "Labor", "TRAVEL": "Labor", "REPLACEMENT SCAFFOLD": "Material", "CONTRACT AMOUNT": "Other",
    "INDIRECT COST": "Other", "SUBCONTRACTOR CONTRACT AMOUNT": "Sub", "TEXTURA CHARGE": "Other",
    "CREDIT CARD PROCESSING": "Other"
}

def map_category(desc):
    return INTERNAL_CATEGORY_MAP.get(norm_text(desc), 'Other')

def process_sales_upload(uploaded_files, year: int, month: int):
    fmap = identify_files(uploaded_files)
    missing = [k for k, v in fmap.items() if v is None]
    if missing:
        return False, f"Missing recognized files for: {', '.join(missing)}"
        
    session = SessionLocal()
    try:
        # --- 1. General Ledger (Invoice -> Job mapping) ---
        df_gl = pd.read_excel(fmap['gl'], header=None)
        # Data rows start with a date in Col 0 (e.g., '01/01/26')
        df_gl = df_gl[df_gl[0].astype(str).str.contains(r'\d{2}/\d{2}/\d{2}', na=False)].copy()
        # Col 2 is Invoice Number (Reference), Col 8 is Job Number (Cost Center)
        invoice_to_job = dict(zip(df_gl[2].astype(str).str.strip(), df_gl[8].astype(str).str.strip()))

        # --- 2. Sales Analysis by Sales Person (Invoice -> Salesperson & Commissionable Amt) ---
        df_sp = pd.read_excel(fmap['sales_person'], header=None)
        df_sp['SP_Letter'] = df_sp[1].where(df_sp[0] == 'Salesperson:').ffill()
        invoices_sp = df_sp[df_sp[0].astype(str).str.contains(r'\d{2}/\d{2}/\d{2}', na=False)].copy()
        
        # --- 3. Sales Analysis by Inventory Item (Invoice -> Categories & Amounts) ---
        inv_hdr = find_header_row(fmap['inventory_item'], required=["Amount"])
        df_inv = pd.read_excel(fmap['inventory_item'], header=inv_hdr)
        
        amount_col = next((c for c in df_inv.columns if norm_text(c) == "AMOUNT"), None)
        invoice_col = next((c for c in df_inv.columns if "INVOICE" in norm_text(c)), None)
        cust_id_col = next((c for c in df_inv.columns if norm_text(c) == "CUSTOMER ID"), None)
        cust_name_col = next((c for c in df_inv.columns if norm_text(c) == "CUSTOMER NAME"), None)
        if cust_name_col is None:
            cust_name_col = df_inv.columns[3] if len(df_inv.columns) > 3 else df_inv.columns[0]
            
        inv2 = df_inv[[df_inv.columns[0], invoice_col, cust_id_col, cust_name_col, amount_col]].copy()
        inv2.columns = ["Inv Date", "Invoice#", "Customer Id", "Customer Name", "Amount"]
        
        INV_RE_5 = re.compile(r"\b(\d{5})\b")
        inv2["Invoice#"] = inv2["Invoice#"].astype(str).str.extract(INV_RE_5, expand=False)
        inv2["Amount"] = pd.to_numeric(inv2["Amount"], errors="coerce")
        
        valid_section_keys = {norm_text(k) for k in INTERNAL_CATEGORY_MAP.keys()}
        labels = []
        current_label = None
        for _, row in inv2.iterrows():
            cid_raw = "" if pd.isna(row["Customer Id"]) else str(row["Customer Id"]).strip()
            cid = norm_text(cid_raw)
            if pd.isna(row["Amount"]) and is_blank(row["Invoice#"]) and is_blank(row["Customer Name"]) and cid in valid_section_keys:
                current_label = cid_raw
            labels.append(current_label)
        
        inv2["Section"] = labels
        invoices_inv = inv2.dropna(subset=["Amount"]).copy()
        invoices_inv = invoices_inv[~invoices_inv["Invoice#"].isna()].copy()
        invoices_inv["Invoice"] = invoices_inv["Invoice#"].astype(str).str.strip()
        invoices_inv["Customer"] = invoices_inv["Customer Name"].astype(str).str.strip().replace("nan", "")
        invoices_inv["Category"] = invoices_inv["Section"].apply(lambda x: INTERNAL_CATEGORY_MAP.get(norm_text(x), "Other"))
        
        # --- 4. Job Cost Query (Monthly Expenses per Job) ---
        cost_hdr = find_header_row(fmap['job_cost'], required=["Amount"])
        jc = pd.read_excel(fmap['job_cost'], header=cost_hdr)
        cols = list(jc.columns)
        jobc = cols[0]
        codec = cols[1] if len(cols) > 1 else cols[0]
        amtc = next((c for c in cols if norm_text(c) == "AMOUNT"), None)
        if amtc is None: amtc = cols[5] if len(cols) > 5 else cols[-1]
        
        df_cost = jc[[jobc, codec, amtc]].copy()
        df_cost.columns = ["Job", "Cost Code", "Amount"]
        df_cost["Job"] = df_cost["Job"].astype(str).str.strip()
        # Only keep valid job number rows (pattern: LETTERS/NUMS-LETTERS/NUMS)
        df_cost = df_cost[df_cost["Job"].str.match(r'^[A-Z0-9]+-[A-Z0-9]+', na=False)]
        df_cost = df_cost[~df_cost["Job"].str.contains("Total", case=False, na=False)]
        df_cost = df_cost[~df_cost["Cost Code"].astype(str).str.contains("Total", case=False, na=False)]
        df_cost = df_cost.dropna(subset=["Cost Code"])
        
        # --- 5. Job Summary (Metadata + Backlog) ---
        df_jobs = pd.read_excel(fmap['job_summary'], header=None)
        job_header_mask = df_jobs[0].astype(str).str.match(r'^[A-Z0-9]+-[A-Z0-9]+$', na=False)
        job_header_indices = df_jobs.index[job_header_mask].tolist()

        DATE_RE = re.compile(r'\d{2}/\d{2}/\d{2}')

        job_meta_dict = {}
        backlog_records = []

        for idx in job_header_indices:
            row = df_jobs.iloc[idx]
            job_num = str(row[0]).strip()
            desc = str(row[1]).strip() if pd.notna(row[1]) else ''
            pm = str(row[2]).strip() if pd.notna(row[2]) else ''
            date_raw = row[3]
            is_completed = bool(DATE_RE.search(str(date_raw).strip())) if pd.notna(date_raw) else False
            date_str = str(date_raw).strip() if is_completed else None

            revised_contract = 0.0
            billed_to_date = 0.0
            if idx + 1 < len(df_jobs):
                data_row = df_jobs.iloc[idx + 1]
                revised_contract = pd.to_numeric(data_row[0], errors='coerce') or 0.0
                billed_to_date = pd.to_numeric(data_row[1], errors='coerce') or 0.0

            job_meta_dict[job_num] = {
                'description': desc,
                'project_manager': pm,
                'date_completed': date_str,
                'revised_contract': revised_contract,
                'billed_to_date': billed_to_date,
                'is_open': not is_completed,
            }

            sp = get_salesperson_from_job(job_num)
            hard_bl = max(0.0, revised_contract - billed_to_date) if not is_completed else 0.0
            backlog_records.append(BacklogSnapshot(
                snapshot_year=year, snapshot_month=month,
                job_number=job_num, description=desc, project_manager=pm,
                salesperson=sp, revised_contract=revised_contract,
                billed_to_date=billed_to_date, hard_backlog=hard_bl,
                is_open=not is_completed,
            ))

        # --- JOIN AND AGGREGATE LOGIC ---
        jobs_data = {}

        def get_job_struct(job):
            if job not in jobs_data:
                j_meta = job_meta_dict.get(job, {})
                jobs_data[job] = {
                    'customer': 'Unknown',
                    'description': j_meta.get('description', 'Unknown'),
                    'project_manager': j_meta.get('project_manager', ''),
                    'date_completed': j_meta.get('date_completed', None),
                    'salesperson': salesperson_from_meta(job, j_meta),
                    'invoiced': 0.0, 'rental_income': 0.0,
                    'labor_income': 0.0, 'material_income': 0.0,
                    'delivery_income': 0.0, 'sub_income': 0.0,
                    'cost': 0.0, 'labor_cost': 0.0, 'other_costs': 0.0
                }
            return jobs_data[job]

        # 1. Aggregate Category Splits (From Inventory Item)
        for _, row in invoices_inv.iterrows():
            inv = row['Invoice']
            job = invoice_to_job.get(inv)
            if job:
                jd = get_job_struct(job)
                amt = row['Amount']
                cat = row['Category']
                jd['customer'] = str(row['Customer'])
                if cat == 'Rent': jd['rental_income'] += amt
                elif cat == 'Labor': jd['labor_income'] += amt
                elif cat == 'Material': jd['material_income'] += amt
                elif cat == 'Delivery': jd['delivery_income'] += amt
                elif cat == 'Sub': jd['sub_income'] += amt
                # Note: We NO LONGER add to jd['invoiced'] here, taking it from SalesPerson instead.
        
        # 2. Assign Salesperson & Aggregate True Revenue (Commissionable)
        for _, row in invoices_sp.iterrows():
            inv = str(row[1]).strip()
            # If an invoice isn't mapped to a job in GL, we still capture its revenue!
            job = invoice_to_job.get(inv, f"UNMAPPED-INV-{inv}")
            jd = get_job_struct(job)
            # Salesperson is determined by job number prefix only — SP file billing
            # category reflects non-commissionable vs commissionable revenue, not ownership

            comm_amt = pd.to_numeric(row[8], errors='coerce')
            if pd.notna(comm_amt):
                jd['invoiced'] += comm_amt
                
        # 3. Aggregate Costs
        for _, row in df_cost.iterrows():
            job = str(row['Job']).strip()
            jd = get_job_struct(job)
            cost_amt = pd.to_numeric(row['Amount'], errors='coerce')
            if pd.isna(cost_amt): continue
            
            jd['cost'] += cost_amt
            ccode = str(row['Cost Code']).strip().upper()
            if ccode in ['L', 'T', 'PD']:
                jd['labor_cost'] += cost_amt
            else:
                jd['other_costs'] += cost_amt

        # --- SAVE TO DATABASE ---
        # Fallback: use customer name as description when Job Summary had no entry
        for d in jobs_data.values():
            if not d['description'] or d['description'] == 'Unknown':
                d['description'] = d['customer']

        session.query(SalesRecord).filter(SalesRecord.year == year, SalesRecord.month == month).delete()
        session.query(BacklogSnapshot).filter(BacklogSnapshot.snapshot_year == year, BacklogSnapshot.snapshot_month == month).delete()
        session.add_all(backlog_records)

        insert_records = []
        for job_id, d in jobs_data.items():
            r = SalesRecord(
                year=year, month=month, job_number=job_id,
                customer=d['customer'], description=d['description'],
                salesperson=d['salesperson'], date_completed=d['date_completed'],
                invoiced=d['invoiced'], rental_income=d['rental_income'],
                labor_income=d['labor_income'],
                material_income=d['material_income'], delivery_income=d['delivery_income'],
                sub_income=d['sub_income'],
                cost=d['cost'],
                labor_cost=float(d['labor_cost']), other_costs=float(d['other_costs']),
                gross_profit=float(d['invoiced']) - float(d['cost'])
            )
            insert_records.append(r)

        session.add_all(insert_records)
        
        log = UploadLog(upload_timestamp=datetime.utcnow(), data_type="Sales")
        session.add(log)
        session.commit()
        
        return True, f"Successfully processed all 5 files for {month}/{year}. Extracted {len(insert_records)} distinct jobs."
        
    except Exception as e:
        session.rollback()
        return False, f"Error processing file loop: {str(e)}"
    finally:
        session.close()

def process_annual_upload(uploaded_files, year: int):
    """
    Process 5 annual report files (full year) and split into monthly DB records.
    Dates in each file determine which month each row belongs to.
    Job Summary is used for metadata only — no backlog snapshots for historical data.
    """
    fmap = identify_files(uploaded_files)
    missing = [k for k, v in fmap.items() if v is None]
    if missing:
        return False, f"Missing recognized files for: {', '.join(missing)}"

    session = SessionLocal()
    try:
        # ── 1. General Ledger: full-year invoice→job mapping ──────────────────
        df_gl = pd.read_excel(fmap['gl'], header=None)
        gl_data = df_gl[df_gl[0].astype(str).str.contains(r'\d{2}/\d{2}/\d{2}', na=False)].copy()
        invoice_to_job = dict(zip(
            gl_data[2].astype(str).str.strip(),
            gl_data[8].astype(str).str.strip()
        ))

        # ── 2. Sales Person: tag each invoice row with its month ───────────────
        df_sp = pd.read_excel(fmap['sales_person'], header=None)
        df_sp['SP_Letter'] = df_sp[1].where(df_sp[0] == 'Salesperson:').ffill()
        sp_all = df_sp[df_sp[0].astype(str).str.contains(r'\d{2}/\d{2}/\d{2}', na=False)].copy()
        # Drop page-footer timestamp rows (no valid invoice number)
        sp_all = sp_all[sp_all[1].astype(str).str.strip().str.match(r'^\d+$')]
        sp_all['month'] = pd.to_datetime(sp_all[0], format='%m/%d/%y', errors='coerce').dt.month

        # ── 3. Inventory Item: parse categories, tag by month ─────────────────
        inv_hdr = find_header_row(fmap['inventory_item'], required=["Amount"])
        df_inv_raw = pd.read_excel(fmap['inventory_item'], header=inv_hdr)

        amount_col   = next((c for c in df_inv_raw.columns if norm_text(c) == "AMOUNT"), None)
        invoice_col  = next((c for c in df_inv_raw.columns if "INVOICE" in norm_text(c)), None)
        cust_id_col  = next((c for c in df_inv_raw.columns if norm_text(c) == "CUSTOMER ID"), None)
        cust_name_col= next((c for c in df_inv_raw.columns if norm_text(c) == "CUSTOMER NAME"), None)
        if cust_name_col is None:
            cust_name_col = df_inv_raw.columns[3] if len(df_inv_raw.columns) > 3 else df_inv_raw.columns[0]

        inv2 = df_inv_raw[[df_inv_raw.columns[0], invoice_col, cust_id_col, cust_name_col, amount_col]].copy()
        inv2.columns = ["Inv Date", "Invoice#", "Customer Id", "Customer Name", "Amount"]
        INV_RE_5 = re.compile(r"\b(\d{5})\b")
        inv2["Invoice#"] = inv2["Invoice#"].astype(str).str.extract(INV_RE_5, expand=False)
        inv2["Amount"]   = pd.to_numeric(inv2["Amount"], errors="coerce")

        valid_section_keys = {norm_text(k) for k in INTERNAL_CATEGORY_MAP.keys()}
        labels, current_label = [], None
        for _, row in inv2.iterrows():
            cid_raw = "" if pd.isna(row["Customer Id"]) else str(row["Customer Id"]).strip()
            cid = norm_text(cid_raw)
            if (pd.isna(row["Amount"]) and is_blank(row["Invoice#"])
                    and is_blank(row["Customer Name"]) and cid in valid_section_keys):
                current_label = cid_raw
            labels.append(current_label)
        inv2["Section"] = labels

        inv_all = inv2.dropna(subset=["Amount"]).copy()
        inv_all = inv_all[~inv_all["Invoice#"].isna()].copy()
        inv_all["Invoice"]  = inv_all["Invoice#"].astype(str).str.strip()
        inv_all["Customer"] = inv_all["Customer Name"].astype(str).str.strip().replace("nan", "")
        inv_all["Category"] = inv_all["Section"].apply(
            lambda x: INTERNAL_CATEGORY_MAP.get(norm_text(x), "Other"))
        inv_all["month"] = pd.to_datetime(inv_all["Inv Date"], format='%m/%d/%y', errors='coerce').dt.month

        # ── 4. Job Cost: tag each cost row with its month ─────────────────────
        cost_hdr = find_header_row(fmap['job_cost'], required=["Amount"])
        jc = pd.read_excel(fmap['job_cost'], header=cost_hdr)
        cols  = list(jc.columns)
        jobc  = cols[0]
        codec = cols[1] if len(cols) > 1 else cols[0]
        amtc  = next((c for c in cols if norm_text(c) == "AMOUNT"), None) or (cols[5] if len(cols) > 5 else cols[-1])
        datec = next((c for c in cols if norm_text(c) == "DATE"), None) or (cols[4] if len(cols) > 4 else None)

        keep = [jobc, codec, amtc] + ([datec] if datec else [])
        cost_all = jc[keep].copy()
        cost_all.columns = ["Job", "Cost Code", "Amount"] + (["Date"] if datec else [])
        if "Date" not in cost_all.columns:
            cost_all["Date"] = None

        cost_all["Job"] = cost_all["Job"].astype(str).str.strip()
        cost_all = cost_all[cost_all["Job"].str.match(r'^[A-Z0-9]+-[A-Z0-9]+', na=False)]
        cost_all = cost_all[~cost_all["Job"].str.contains("Total", case=False, na=False)]
        cost_all = cost_all[~cost_all["Cost Code"].astype(str).str.contains("Total", case=False, na=False)]
        cost_all = cost_all.dropna(subset=["Cost Code"])
        cost_all["month"] = pd.to_datetime(cost_all["Date"], format='%m/%d/%y', errors='coerce').dt.month

        # ── 5. Job Summary: metadata only ─────────────────────────────────────
        df_jobs = pd.read_excel(fmap['job_summary'], header=None)
        job_header_mask    = df_jobs[0].astype(str).str.match(r'^[A-Z0-9]+-[A-Z0-9]+$', na=False)
        job_header_indices = df_jobs.index[job_header_mask].tolist()
        DATE_RE_LOCAL      = re.compile(r'\d{2}/\d{2}/\d{2}')

        job_meta_dict = {}
        for idx in job_header_indices:
            row     = df_jobs.iloc[idx]
            job_num = str(row[0]).strip()
            desc    = str(row[1]).strip() if pd.notna(row[1]) else ''
            pm      = str(row[2]).strip() if pd.notna(row[2]) else ''
            date_raw= row[3]
            done    = bool(DATE_RE_LOCAL.search(str(date_raw).strip())) if pd.notna(date_raw) else False
            job_meta_dict[job_num] = {
                'description':    desc,
                'project_manager':pm,
                'date_completed': str(date_raw).strip() if done else None,
            }

        # ── Process each detected month ────────────────────────────────────────
        months = sorted(set(
            sp_all['month'].dropna().astype(int).tolist() +
            cost_all['month'].dropna().astype(int).tolist()
        ))
        if not months:
            return False, "No dated rows detected. Check that the files contain transaction dates."

        total_records = 0
        for month in months:
            sp_m   = sp_all[sp_all['month'] == month]
            inv_m  = inv_all[inv_all['month'] == month]
            cost_m = cost_all[cost_all['month'] == month]

            jobs_data = {}

            def _get(job, jd=jobs_data):
                if job not in jd:
                    meta = job_meta_dict.get(job, {})
                    jd[job] = {
                        'customer':       'Unknown',
                        'description':    meta.get('description', 'Unknown'),
                        'salesperson':    salesperson_from_meta(job, meta),
                        'date_completed': meta.get('date_completed'),
                        'invoiced': 0.0, 'rental_income': 0.0,
                        'labor_income': 0.0, 'material_income': 0.0,
                        'delivery_income': 0.0, 'sub_income': 0.0,
                        'cost': 0.0, 'labor_cost': 0.0, 'other_costs': 0.0,
                    }
                return jd[job]

            for _, row in inv_m.iterrows():
                job = invoice_to_job.get(row['Invoice'])
                if not job:
                    continue
                jd, amt, cat = _get(job), row['Amount'], row['Category']
                jd['customer'] = str(row['Customer'])
                if cat == 'Rent':  jd['rental_income'] += amt
                elif cat == 'Labor': jd['labor_income'] += amt
                elif cat == 'Material': jd['material_income'] += amt
                elif cat == 'Delivery': jd['delivery_income'] += amt
                elif cat == 'Sub': jd['sub_income'] += amt

            for _, row in sp_m.iterrows():
                inv = str(row[1]).strip()
                job = invoice_to_job.get(inv, f"UNMAPPED-INV-{inv}")
                jd  = _get(job)
                amt = pd.to_numeric(row[8], errors='coerce')
                if pd.notna(amt):
                    jd['invoiced'] += amt

            for _, row in cost_m.iterrows():
                jd      = _get(str(row['Job']).strip())
                cost_amt = pd.to_numeric(row['Amount'], errors='coerce')
                if pd.isna(cost_amt):
                    continue
                jd['cost'] += cost_amt
                if str(row['Cost Code']).strip().upper() in ['L', 'T', 'PD']:
                    jd['labor_cost'] += cost_amt
                else:
                    jd['other_costs'] += cost_amt

            # Fallback: use customer name as description when Job Summary had no entry
            for d in jobs_data.values():
                if not d['description'] or d['description'] == 'Unknown':
                    d['description'] = d['customer']

            session.query(SalesRecord).filter(
                SalesRecord.year == year, SalesRecord.month == month).delete()

            records = [
                SalesRecord(
                    year=year, month=month, job_number=job_id,
                    customer=d['customer'], description=d['description'],
                    salesperson=d['salesperson'], date_completed=d['date_completed'],
                    invoiced=d['invoiced'], rental_income=d['rental_income'],
                    labor_income=d['labor_income'],
                    material_income=d['material_income'], delivery_income=d['delivery_income'],
                    sub_income=d['sub_income'],
                    cost=d['cost'],
                    labor_cost=float(d['labor_cost']), other_costs=float(d['other_costs']),
                    gross_profit=float(d['invoiced']) - float(d['cost'])
                )
                for job_id, d in jobs_data.items()
            ]
            session.add_all(records)
            total_records += len(records)

        log = UploadLog(upload_timestamp=datetime.utcnow(), data_type=f"Annual-{year}")
        session.add(log)
        session.commit()

        month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        months_str  = ", ".join(month_names[m - 1] for m in months)
        return True, f"Loaded {len(months)} months for {year} ({months_str}) — {total_records} job records total."

    except Exception as e:
        session.rollback()
        return False, f"Error during annual processing: {str(e)}"
    finally:
        session.close()


def process_budget_upload(file_obj, year: int):
    """
    Parse a budget xlsx file and store monthly budget amounts per salesperson.
    Expected layout:
      Row 1 : header — month dates in cols 1-12
      Row 2 : 'Income' — company-wide total
      Rows 3+: individual salesperson rows
    Salesperson names are stored uppercased so they match SalesRecord values.
    'Income' row is stored as salesperson='COMPANY' for use in All-Salespeople view.
    """
    import openpyxl
    try:
        wb = openpyxl.load_workbook(file_obj, data_only=True)
    except Exception as e:
        return False, f"Could not open budget file: {e}"

    ws = wb.active
    all_rows = [r for r in ws.iter_rows(values_only=True) if any(c is not None for c in r)]

    if len(all_rows) < 2:
        return False, "Budget file appears empty or unrecognized."

    # ── Detect month columns from header row ──────────────────────────────────
    header = all_rows[0]
    month_cols = []  # list of (column_index, month_number)
    for i, cell in enumerate(header):
        if hasattr(cell, 'month'):           # datetime object
            month_cols.append((i, cell.month))
        elif isinstance(cell, (int, float)) and 1 <= cell <= 12:
            month_cols.append((i, int(cell)))

    if not month_cols:
        return False, "Could not detect month columns in the header row."

    session = SessionLocal()
    try:
        # Clear existing budget for this year
        session.query(BudgetRecord).filter(BudgetRecord.year == year).delete()

        records = []
        for data_row in all_rows[1:]:
            label = data_row[0]
            if label is None:
                continue
            label_str = str(label).strip().upper()
            if not label_str:
                continue

            sp_name = 'COMPANY' if label_str == 'INCOME' else label_str

            for col_idx, month in month_cols:
                raw = data_row[col_idx] if col_idx < len(data_row) else None
                try:
                    amount = float(raw) if raw is not None and not isinstance(raw, str) else 0.0
                except (TypeError, ValueError):
                    amount = 0.0

                records.append(BudgetRecord(
                    year=year, month=month,
                    salesperson=sp_name, amount=amount
                ))

        session.add_all(records)
        log = UploadLog(upload_timestamp=datetime.utcnow(), data_type=f"Budget-{year}")
        session.add(log)
        session.commit()

        sp_count = len({r.salesperson for r in records}) - 1  # exclude COMPANY
        return True, f"Budget loaded for {year}: {sp_count} salespeople across {len(month_cols)} months."

    except Exception as e:
        session.rollback()
        return False, f"Error saving budget: {e}"
    finally:
        session.close()
