from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Boolean, MetaData
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

Base = declarative_base()

class SalesRecord(Base):
    __tablename__ = 'sales'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    
    job_number = Column(String, nullable=False)
    customer = Column(String)
    description = Column(String)
    salesperson = Column(String)
    date_completed = Column(String) # Used for Backlog calculation
    
    invoiced = Column(Float, default=0.0)
    rental_income = Column(Float, default=0.0)
    labor_income = Column(Float, default=0.0)
    material_income = Column(Float, default=0.0)
    delivery_income = Column(Float, default=0.0)
    sub_income = Column(Float, default=0.0)
    
    cost = Column(Float, default=0.0)
    labor_cost = Column(Float, default=0.0)
    other_costs = Column(Float, default=0.0)
    
    gross_profit = Column(Float, default=0.0)

class BudgetRecord(Base):
    __tablename__ = 'budget'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    salesperson = Column(String) # Optional, can be global or per salesperson
    amount = Column(Float, default=0.0)

class BacklogSnapshot(Base):
    __tablename__ = 'backlog_snapshot'
    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_year = Column(Integer, nullable=False)
    snapshot_month = Column(Integer, nullable=False)
    job_number = Column(String, nullable=False)
    description = Column(String)
    project_manager = Column(String)
    salesperson = Column(String)
    revised_contract = Column(Float, default=0.0)
    billed_to_date = Column(Float, default=0.0)
    hard_backlog = Column(Float, default=0.0)
    is_open = Column(Boolean, default=True)

class UploadLog(Base):
    __tablename__ = 'upload_logs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    upload_timestamp = Column(Date, default=datetime.utcnow)
    data_type = Column(String) # "Sales" or "Budget"
    filename = Column(String)

# Database initialization
import os
_db_dir = os.environ.get('DB_DIR', 'data')
os.makedirs(_db_dir, exist_ok=True)
engine = create_engine(f'sqlite:///{_db_dir}/sales_dashboard.db', connect_args={'check_same_thread': False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)
    # Lightweight migration: add new columns to existing 'sales' table if missing
    from sqlalchemy import text
    new_cols = {
        'material_income': 'FLOAT DEFAULT 0.0',
        'delivery_income': 'FLOAT DEFAULT 0.0',
        'sub_income': 'FLOAT DEFAULT 0.0',
    }
    with engine.connect() as conn:
        existing = {row[1] for row in conn.execute(text("PRAGMA table_info(sales)"))}
        for col, col_type in new_cols.items():
            if col not in existing:
                conn.execute(text(f"ALTER TABLE sales ADD COLUMN {col} {col_type}"))
        conn.commit()
