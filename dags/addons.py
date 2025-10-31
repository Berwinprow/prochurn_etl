import pandas as pd
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.types import String
from airflow.models import Variable

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
SOURCE_SCHEMA = "pip_aggregation"
ZONE_TABLE = 'zoned_map'
BATCH_SIZE = 50000
TARGET_TABLE = "baseprclaim"
META_TABLE = "etl_metadata_logs"
CLAIM_TABLE = "claim_logs"
LOG_SCHEMA = "pip_log"

# ✅ Fetch latest claim_merged_with_basepr table from metadata
def get_latest_claim_merged_with_basepr(engine):
    query = f"""
        SELECT table_name FROM {LOG_SCHEMA}.{CLAIM_TABLE}
        WHERE is_basepr_claim_merged = 'YES'
        ORDER BY last_updated_ts DESC
        LIMIT 1;
    """
    result = engine.execute(text(query)).fetchone()
    return result[0] if result else None

# ✅ Ensure feature_eng_log table exists with proper columns
def ensure_feature_log_table(engine):
    query = f"""
        CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.feature_eng_log (
            date DATE PRIMARY KEY,
            addons TEXT,
            count_of_addons BIGINT,
            new_col TEXT,
            count_of_new_col BIGINT,
            future_pred TEXT,
            count_of_future_pred BIGINT,
            renewed_policy_count BIGINT,
            non_renewed_policy_count BIGINT,
            segmentation TEXT,
            segmentation_count BIGINT,
            reason TEXT,
            reason_count BIGINT
        );
    """
    with engine.begin() as conn:
        conn.execute(text(query))

# ✅ Update feature engineering log
def update_feature_log(engine, col_name, target_table, count_val=None):
    ensure_feature_log_table(engine)  # make sure table exists

    # Always prepare count_col
    count_col = f"count_of_{col_name}"

    # Default count_val to 0 if None
    cnt = count_val if count_val is not None else 0

    query = f"""
        INSERT INTO {LOG_SCHEMA}.feature_eng_log(date, {col_name}, {count_col})
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


def addon_column():
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()

    # 1. Get latest source table dynamically
    SOURCE_TABLE = get_latest_claim_merged_with_basepr(engine)
    if not SOURCE_TABLE:
        raise ValueError("❌ No claim_merged_with_basepr table found in metadata!")

    # 2. Read data
    query = f"""SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}"""
    df = pd.read_sql(query, engine)
    print(f"✅ Fetched {len(df)} rows from {SOURCE_SCHEMA}.{SOURCE_TABLE}")

    # 3. Apply zone mapping
    zone_map_df = json.loads(Variable.get(ZONE_TABLE))
    df['Zone'] = df.apply(
        lambda row: zone_map_df.get(str(row['state']).upper(), row['Zone'])
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