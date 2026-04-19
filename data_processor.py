import pandas as pd
import os, io
from database import (SessionLocal, SalesRecord, UploadLog, BacklogSnapshot, BudgetRecord,
                       JobHours, JobChangeOrder, JobBudget, CustomerLookup)
from datetime import datetime


# ─── Upload archive (so every upload can be reprocessed later) ─────────────
def _upload_root():
    """Root folder for archived raw uploads. Sits next to the DB so Render's
    persistent disk keeps both together."""
    return os.path.join(os.environ.get('DB_DIR', 'data'), 'uploads')


def _archive_upload(file_obj, subdir, out_name=None):
    """Save a Streamlit UploadedFile / BytesIO to the upload archive, then rewind it."""
    dest_dir = os.path.join(_upload_root(), subdir)
    os.makedirs(dest_dir, exist_ok=True)
    name = out_name or getattr(file_obj, 'name', 'file.xlsx')
    dest = os.path.join(dest_dir, os.path.basename(name))
    if hasattr(file_obj, 'seek'):
        try: file_obj.seek(0)
        except Exception: pass
    data = file_obj.read()
    with open(dest, 'wb') as out:
        out.write(data)
    if hasattr(file_obj, 'seek'):
        try: file_obj.seek(0)
        except Exception: pass


def _wrap_as_upload(path):
    """Wrap a file on disk as a BytesIO with .name, matching Streamlit UploadedFile shape."""
    with open(path, 'rb') as f:
        data = f.read()
    buf = io.BytesIO(data)
    buf.name = os.path.basename(path)
    return buf


def _job_prefix(job_number):
    """Extract the client ID prefix from a job number (e.g., 'ADPE' from 'ADPE-P501')."""
    return job_number.split('-')[0].strip().upper() if '-' in job_number else job_number.strip().upper()


def _update_customer_lookup(session, jobs_data):
    """Learn and apply customer names using the persistent prefix→customer lookup.

    1. Mine ALL existing SalesRecords for known customer→prefix mappings (catches history).
    2. Learn from the current upload's jobs_data.
    3. Fill unknowns by prefix match.
    """
    # Step 0: Seed lookup from ALL existing SalesRecords (historical data)
    from sqlalchemy import distinct
    existing_customers = (
        session.query(SalesRecord.job_number, SalesRecord.customer)
        .filter(SalesRecord.customer != 'Unknown', SalesRecord.customer != '', SalesRecord.customer.isnot(None))
        .all()
    )
    for job_num, cust in existing_customers:
        prefix = _job_prefix(job_num)
        existing_cl = session.query(CustomerLookup).filter(
            CustomerLookup.job_prefix == prefix).first()
        if not existing_cl:
            session.add(CustomerLookup(
                job_prefix=prefix, customer_name=cust,
                last_updated=datetime.utcnow()))
            session.flush()

    # Step 1: Learn from current upload (overrides older values)
    learned = {}
    for job_id, d in jobs_data.items():
        cust = d.get('customer', 'Unknown')
        if cust and cust != 'Unknown':
            learned[_job_prefix(job_id)] = cust

    for prefix, cust in learned.items():
        existing_cl = session.query(CustomerLookup).filter(
            CustomerLookup.job_prefix == prefix).first()
        if existing_cl:
            existing_cl.customer_name = cust
            existing_cl.last_updated = datetime.utcnow()
        else:
            session.add(CustomerLookup(
                job_prefix=prefix, customer_name=cust,
                last_updated=datetime.utcnow()))
            session.flush()

    # Step 2: Fill unknowns from lookup (DB + just-learned)
    for job_id, d in jobs_data.items():
        if not d.get('customer') or d['customer'] == 'Unknown':
            prefix = _job_prefix(job_id)
            if prefix in learned:
                d['customer'] = learned[prefix]
            else:
                lookup = session.query(CustomerLookup).filter(
                    CustomerLookup.job_prefix == prefix).first()
                if lookup:
                    d['customer'] = lookup.customer_name


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

    # Archive originals so we can reprocess later
    for f in uploaded_files:
        _archive_upload(f, f"sales/{year:04d}-{month:02d}")

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
        # Learn + fill customer names via persistent prefix→customer lookup
        _update_customer_lookup(session, jobs_data)

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

    # Archive originals so we can reprocess later
    for f in uploaded_files:
        _archive_upload(f, f"annual/{year:04d}")

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

            # Learn + fill customer names via persistent prefix→customer lookup
            _update_customer_lookup(session, jobs_data)

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
    # Archive original so we can reprocess later
    _archive_upload(file_obj, "budget", out_name=f"{year:04d}.xlsx")

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


# ─── Reprocess every previously-uploaded period from the archive ─────────
def reprocess_all_from_archive():
    """Walk uploads/ and re-run each archived set through its processor.

    Returns a list of (kind, period, ok, msg) tuples for reporting.
    """
    results = []
    root = _upload_root()
    if not os.path.isdir(root):
        return results

    # 1. Monthly sales uploads → uploads/sales/YYYY-MM/*.xlsx
    sales_root = os.path.join(root, 'sales')
    if os.path.isdir(sales_root):
        for period in sorted(os.listdir(sales_root)):
            m = re.match(r'^(\d{4})-(\d{1,2})$', period)
            if not m:
                continue
            y, mo = int(m.group(1)), int(m.group(2))
            pdir = os.path.join(sales_root, period)
            files = [_wrap_as_upload(os.path.join(pdir, f))
                     for f in sorted(os.listdir(pdir)) if f.lower().endswith('.xlsx')]
            if len(files) < 5:
                results.append(('sales', period, False, f"only {len(files)}/5 files archived"))
                continue
            ok, msg = process_sales_upload(files, y, mo)
            results.append(('sales', period, ok, msg))

    # 2. Annual bulk uploads → uploads/annual/YYYY/*.xlsx
    annual_root = os.path.join(root, 'annual')
    if os.path.isdir(annual_root):
        for year_dir in sorted(os.listdir(annual_root)):
            m = re.match(r'^(\d{4})$', year_dir)
            if not m:
                continue
            y = int(m.group(1))
            ydir = os.path.join(annual_root, year_dir)
            files = [_wrap_as_upload(os.path.join(ydir, f))
                     for f in sorted(os.listdir(ydir)) if f.lower().endswith('.xlsx')]
            if len(files) < 5:
                results.append(('annual', year_dir, False, f"only {len(files)}/5 files archived"))
                continue
            ok, msg = process_annual_upload(files, y)
            results.append(('annual', year_dir, ok, msg))

    # 3. Budget files → uploads/budget/YYYY.xlsx
    budget_root = os.path.join(root, 'budget')
    if os.path.isdir(budget_root):
        for fname in sorted(os.listdir(budget_root)):
            m = re.match(r'^(\d{4})\.xlsx$', fname, re.IGNORECASE)
            if not m:
                continue
            y = int(m.group(1))
            ok, msg = process_budget_upload(_wrap_as_upload(os.path.join(budget_root, fname)), y)
            results.append(('budget', str(y), ok, msg))

    return results


def process_labor_distribution(file_obj):
    """Parse a Labor Distribution file and upsert hours budgeted/used per job.

    Extracts the 'Job Totals' row per job:
      - Budget (col 4) → hours_budgeted
      - Total Hours (col 8) → hours_used
    Only updates jobs present in the file; others are untouched.
    """
    _archive_upload(file_obj, "labor_distribution", out_name="Labor Distribution.xlsx")

    try:
        df = pd.read_excel(file_obj, header=None)
    except Exception as e:
        return False, f"Could not read Labor Distribution file: {e}"

    records = []
    current_job = None

    for i, row in df.iterrows():
        c0 = str(row[0]).strip() if pd.notna(row[0]) else ''
        c1 = str(row[1]).strip() if pd.notna(row[1]) else ''
        c2 = str(row[2]).strip() if pd.notna(row[2]) else ''

        if c1 == 'Job' and c2:
            current_job = c2
        elif c1 == 'Job Totals' and current_job:
            budget = pd.to_numeric(row[4], errors='coerce') or 0.0
            total_hours = pd.to_numeric(row[8], errors='coerce') or 0.0
            records.append({
                'job_number': current_job,
                'hours_budgeted': budget,
                'hours_used': total_hours,
            })
            current_job = None

    if not records:
        return False, "No job data found in Labor Distribution file."

    session = SessionLocal()
    try:
        job_nums = [r['job_number'] for r in records]
        session.query(JobHours).filter(JobHours.job_number.in_(job_nums)).delete(
            synchronize_session=False)
        session.add_all([
            JobHours(job_number=r['job_number'],
                     hours_budgeted=r['hours_budgeted'],
                     hours_used=r['hours_used'],
                     last_updated=datetime.utcnow())
            for r in records
        ])
        log = UploadLog(upload_timestamp=datetime.utcnow(), data_type="Labor Distribution")
        session.add(log)
        session.commit()
        return True, f"Labor Distribution loaded: {len(records)} jobs updated."
    except Exception as e:
        session.rollback()
        return False, f"Error saving Labor Distribution: {e}"
    finally:
        session.close()


def process_job_cost_status(file_obj):
    """Parse a Job Cost Status file and upsert budget + change orders per job.

    Per job extracts:
      - 'Budget Totals' → Original Budget (col 3) → JobBudget
      - Each 'Change Order NN' header → CO number, description
      - 'Change Order Totals' → Original Budget (col 3) → JobChangeOrder
    Only updates jobs present in the file; others are untouched.
    """
    _archive_upload(file_obj, "job_cost_status", out_name="Job Cost Status.xls")

    try:
        df = pd.read_excel(file_obj, header=None)
    except Exception as e:
        return False, f"Could not read Job Cost Status file: {e}"

    budget_records = []
    co_records = []
    current_job = None
    current_co = None
    current_co_desc = None

    for i, row in df.iterrows():
        c0 = str(row[0]).strip() if pd.notna(row[0]) else ''
        c1 = str(row[1]).strip() if pd.notna(row[1]) else ''
        c2 = str(row[2]).strip() if pd.notna(row[2]) else ''

        if c1 == 'Job' and c2:
            current_job = c2
            current_co = None

        elif c1 == 'Budget Totals' and current_job:
            orig_budget = pd.to_numeric(row[3], errors='coerce') or 0.0
            budget_records.append({
                'job_number': current_job,
                'original_budget': orig_budget,
            })

        elif c0.startswith('Change Order') and current_job:
            current_co = c0
            current_co_desc = c1 if c1 else c0

        elif c1 == 'Change Order Totals' and current_job and current_co:
            co_amount = pd.to_numeric(row[3], errors='coerce') or 0.0
            co_records.append({
                'job_number': current_job,
                'co_number': current_co,
                'description': current_co_desc,
                'amount': co_amount,
            })
            current_co = None
            current_co_desc = None

    if not budget_records and not co_records:
        return False, "No job data found in Job Cost Status file."

    session = SessionLocal()
    try:
        job_nums = list(set(
            [r['job_number'] for r in budget_records] +
            [r['job_number'] for r in co_records]
        ))
        session.query(JobBudget).filter(JobBudget.job_number.in_(job_nums)).delete(
            synchronize_session=False)
        session.query(JobChangeOrder).filter(JobChangeOrder.job_number.in_(job_nums)).delete(
            synchronize_session=False)

        session.add_all([
            JobBudget(job_number=r['job_number'],
                      original_budget=r['original_budget'],
                      last_updated=datetime.utcnow())
            for r in budget_records
        ])
        session.add_all([
            JobChangeOrder(job_number=r['job_number'],
                           co_number=r['co_number'],
                           description=r['description'],
                           amount=r['amount'],
                           last_updated=datetime.utcnow())
            for r in co_records
        ])
        log = UploadLog(upload_timestamp=datetime.utcnow(), data_type="Job Cost Status")
        session.add(log)
        session.commit()
        return True, (f"Job Cost Status loaded: {len(budget_records)} job budgets, "
                      f"{len(co_records)} change orders.")
    except Exception as e:
        session.rollback()
        return False, f"Error saving Job Cost Status: {e}"
    finally:
        session.close()


def archived_periods_summary():
    """Return a human-readable summary of what's currently archived."""
    root = _upload_root()
    lines = []
    for kind, sub in [('Monthly sales', 'sales'), ('Annual bulk', 'annual'), ('Budget', 'budget')]:
        p = os.path.join(root, sub)
        if not os.path.isdir(p):
            continue
        entries = sorted(os.listdir(p))
        if entries:
            lines.append(f"{kind}: {', '.join(entries)}")
    return lines


def get_file_library():
    """Walk the upload archive and return a list of dicts describing every stored file.

    Each dict: {'type', 'period', 'filename', 'path', 'size_kb', 'uploaded_at'}
    Sorted by type → period → filename.
    """
    root = _upload_root()
    files = []
    if not os.path.isdir(root):
        return files
    for kind in ['sales', 'annual', 'budget']:
        kind_root = os.path.join(root, kind)
        if not os.path.isdir(kind_root):
            continue
        if kind == 'budget':
            for fname in sorted(os.listdir(kind_root)):
                if not fname.lower().endswith('.xlsx'):
                    continue
                fpath = os.path.join(kind_root, fname)
                stat = os.stat(fpath)
                files.append({
                    'type': 'Budget',
                    'period': os.path.splitext(fname)[0],
                    'filename': fname,
                    'path': fpath,
                    'size_kb': round(stat.st_size / 1024, 1),
                    'uploaded_at': datetime.fromtimestamp(stat.st_mtime),
                })
        else:
            for period_dir in sorted(os.listdir(kind_root)):
                pdir = os.path.join(kind_root, period_dir)
                if not os.path.isdir(pdir):
                    continue
                label = 'Monthly' if kind == 'sales' else 'Annual'
                for fname in sorted(os.listdir(pdir)):
                    if not fname.lower().endswith('.xlsx'):
                        continue
                    fpath = os.path.join(pdir, fname)
                    stat = os.stat(fpath)
                    files.append({
                        'type': label,
                        'period': period_dir,
                        'filename': fname,
                        'path': fpath,
                        'size_kb': round(stat.st_size / 1024, 1),
                        'uploaded_at': datetime.fromtimestamp(stat.st_mtime),
                    })
    return files
