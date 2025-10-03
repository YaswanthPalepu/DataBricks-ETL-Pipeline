# load_gold_to_postgres.py
import os
import pandas as pd
from sqlalchemy import create_engine, text

PG_USER = os.getenv("PGUSER","metabase")
PG_PASS = os.getenv("PGPASSWORD","metabase")
PG_HOST = os.getenv("PGHOST","postgres")
PG_PORT = os.getenv("PGPORT","5432")
PG_DB   = os.getenv("PGDATABASE","metabase")

engine = create_engine(f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

gold_dir = "/data/gold"   # mapped volume

tables = ["dim_customer", "dim_contract", "dim_services", "dim_payment", "fact_churn"]

for t in tables:
    path = os.path.join(gold_dir, t)
    if not os.path.exists(path):
        # support alternative suffix like t_single or directory with parquet parts
        alt = os.path.join(gold_dir, t + "_single")
        if os.path.exists(alt):
            path = alt
    if not os.path.exists(path):
        print(f"[WARN] {t} path not found at {path}, skipping")
        continue
    print(f"Loading {t} from {path} ...")
    # pandas can read a directory of parquet files
    df = pd.read_parquet(path)
    # optional: lower-case columns, strip spaces
    df.columns = [c.strip() for c in df.columns]
    # write to postgres (replace)
    df.to_sql(t, engine, if_exists="replace", index=False)
    print(f"Loaded {len(df)} rows into {t}")

print("Done loading gold tables into Postgres.")
