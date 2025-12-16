import os
import pandas as pd
from sqlalchemy import create_engine

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "dataopsadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "dataopsadmin")

S3_BUCKET = os.getenv("S3_BUCKET", "dataops-bronze")
S3_KEY = os.getenv("S3_KEY", "raw/dirty_store_transactions.csv")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB", "traindb")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

TABLE = os.getenv("PG_TABLE", "clean_data_transactions")
SCHEMA = os.getenv("PG_SCHEMA", "public")

def main():
    s3_url = f"s3://{S3_BUCKET}/{S3_KEY}"
    storage_options = {
        "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
    }

    df = pd.read_csv(s3_url, storage_options=storage_options)
    df.columns = [c.strip() for c in df.columns]

    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).str.strip()
        df.loc[df[c].isin(["", "None", "nan", "NaN", "NULL", "null"]), c] = None

    df = df.drop_duplicates().dropna(how="all")

    pg_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(pg_url)

    df.to_sql(TABLE, engine, schema=SCHEMA, if_exists="replace", index=False)
    print(f"Loaded {len(df)} rows into {SCHEMA}.{TABLE}")

if __name__ == "__main__":
    main()
PY