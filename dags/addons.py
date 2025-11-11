# ====================================================================
# 📦 Airflow ETL: Azure Blob → PostgreSQL (PEP8 + Flake8 Clean)
# ====================================================================
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.types import String
from airflow.models import Variable
from pathlib import Path
from schema_table_config import get_log_tables, get_schema, get_column_mapping


# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------
DAG_DIR = Path(__file__).resolve().parent
JSON_PATH = str(DAG_DIR / "config" / "schema_metadata_config.json")

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
SOURCE_SCHEMA = get_schema("agg", JSON_PATH)
COLUMN_JSON = str(DAG_DIR / "config" / "column_mapping.json")
ZONE_TABLE = get_column_mapping("zonemapping", COLUMN_JSON)
BATCH_SIZE = 50000
TARGET_TABLE = "baseprclaim"
META_TABLE = get_log_tables("metadata", JSON_PATH)
CLAIM_TABLE = get_log_tables("claimlog", JSON_PATH)
LOG_SCHEMA = get_schema("log", JSON_PATH)
FEATURE_LOG = get_log_tables("featurelog", JSON_PATH)
# ---------------------------------------------------------------------
# To Fetch latest claim_merged_with_basepr table from metadata 
# ---------------------------------------------------------------------
def get_latest_claim_merged_with_basepr(engine):
    query = f"""
        SELECT table_name FROM "{LOG_SCHEMA}"."{CLAIM_TABLE}"
        WHERE is_basepr_claim_merged = 'YES'
        ORDER BY last_updated_ts DESC
        LIMIT 1;
    """
    result = engine.execute(text(query)).fetchone()
    return result[0] if result else None

# ---------------------------------------------------------------------
# To Update the Meta data  
# ---------------------------------------------------------------------
def update_feature_log(engine, col_name, target_table, count_val=None):
    # Always prepare count_col
    count_col = f"count_of_{col_name}"

    # Default count_val to 0 if None
    cnt = count_val if count_val is not None else 0

    query = f"""
        INSERT INTO "{LOG_SCHEMA}"."{FEATURE_LOG}"(date, {col_name}, {count_col})
        VALUES (:dt, :tbl, :cnt)
        ON CONFLICT (date)
        DO UPDATE SET
            {col_name} = :tbl,
            {count_col} = :cnt;
    """

    with engine.begin() as conn:
        conn.execute(
            text(query),
            {"dt": datetime.now().date(), "tbl": target_table, "cnt": cnt}
        )
# ---------------------------------------------------------------------
# Extra conditions Addons Before Feature engineering
# ---------------------------------------------------------------------

def addon_column():
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()

    # 1. Get latest source table dynamically
    SOURCE_TABLE = get_latest_claim_merged_with_basepr(engine)
    if not SOURCE_TABLE:
        raise ValueError("❌ No claim_merged_with_basepr table found in metadata!")

    # 2. Read data
    query = f'SELECT * FROM "{SOURCE_SCHEMA}"."{SOURCE_TABLE}"'
    df = pd.read_sql(query, engine)
    print(f"✅ Fetched {len(df)} rows from {SOURCE_SCHEMA}.{SOURCE_TABLE}")

    # 3. Apply zone mapping
    # zone_map_df = json.loads(Variable.get(ZONE_TABLE))
    df['Zone'] = df.apply(
        lambda row: ZONE_TABLE.get(str(row['state']).upper(), row['Zone'])
        if pd.isna(row['Zone']) else row['Zone'],
        axis=1
    )
    print(f"✅ Applied zone mapping from {ZONE_TABLE}")

    # 4. Drop null corrected_name
    if 'corrected_name' in df.columns:
        removed_count = df["corrected_name"].isna().sum()
        print(f"⚠️ Removed {removed_count} rows with null corrected_name")
        df = df[df["corrected_name"].notna()]
    else:
        print("❗ 'corrected_name' column not found in DataFrame")
    
    # BOOKED = 1 ⇒ renewed_flag = 1
    if "booked" in df.columns and "renewed_flag" in df.columns:
        mask = df["booked"] == 1
        df.loc[mask, "renewed_flag"] = 1


    # 5. Save back to same source table
    df.to_sql(
        name=TARGET_TABLE,
        schema=SOURCE_SCHEMA,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=BATCH_SIZE,
        dtype={"Zone": String}
    )
    print(f"✅ Zone mapping completed — {df['Zone'].isna().sum()} nulls remaining")

    # 6. Update feature engineering log with row count + target table name
    row_count = len(df)
    update_feature_log(engine, "addons", TARGET_TABLE, row_count)
    print(f"✅ Feature log updated for addons = {TARGET_TABLE}, row_count = {row_count}")



# with DAG(
#     dag_id = "zone_mapp",
#     default_args = {"owner":"airflow","start_date":datetime(2024,1,1)},
#     schedule_interval = None,
#     catchup = False,
#     tags = ["map","zone"]
# ) as dag:

#     update_zone_task = PythonOperator(
#         task_id = 'update_zone_task',
#         python_callable = addon_column
#     )

#     update_zone_task