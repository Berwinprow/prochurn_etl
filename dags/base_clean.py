# ============================================================
# 📦 Airflow ETL: Azure Blob → PostgreSQL (PEP8 + Flake8 Clean)
# ============================================================

import pandas as pd
import re
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from time import sleep
from pathlib import Path
from schema_table_config import get_schema, get_log_tables, get_column_mapping

# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
JSON_PATH = str(DAGS_DIR / "config" / "schema_metadata_config.json")

SOURCE_SCHEMA = get_schema("stage", JSON_PATH)
TARGET_SCHEMA = get_schema("agg", JSON_PATH)
LOG_SCHEMA = get_schema("log", JSON_PATH)
POSTGRES_CONN_ID = "postgres_cloud_prochurn"
META_DATA = get_log_tables("metadata", JSON_PATH)

COLUMN_JSON = str(DAGS_DIR / "config" / "column_mapping.json")
COLUMN_MAPPING = get_column_mapping("base", COLUMN_JSON)

# ---------------------------------------------------------------------
# 🗃️ Update metadata logs
# ---------------------------------------------------------------------   

def update_metadata(table_name, step, status=True, row_count=None,removed_count=None, **context):
    

    for attempt in range(3):  # retry up to 3 times
        try:
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = pg_hook.get_sqlalchemy_engine()
            with engine.begin() as conn:
                status_val = "YES" if status in [True, "YES", "1"] else "NO"
                conn.execute(text(f"""
                    INSERT INTO "{LOG_SCHEMA}"."{META_DATA}" 
                    (table_name, {step}, base_cnt,base_removed_cnt, last_updated_ts)
                    VALUES (:table_name, :status, :row_count,:removed_cnt, :ts)
                    ON CONFLICT (table_name)
                    DO UPDATE SET {step} = :status,
                                  base_cnt = :row_count,
                                  base_removed_cnt = :removed_cnt,
                                  last_updated_ts = :ts
                """), {
                    "table_name": table_name,
                    "status": status_val,
                    "row_count": row_count if row_count is not None else 0,
                    "removed_count": removed_count if removed_count is not None else 0,
                    "ts": datetime.utcnow()
                })
            engine.dispose()
            print(f"✅ Metadata updated for {table_name}")
            return  # exit if successful

        except OperationalError as e:
            print(f"⚠️ Metadata update failed (attempt {attempt+1}/3): {e}")
            sleep(5)
            continue  # retry on transient timeout errors

    print(f"❌ Failed to update metadata for {table_name} after 3 retries.")

# ---------------------------------------------------------------------
# 🧹 Clean column names
# ---------------------------------------------------------------------

def clean_text(value):
    if pd.isna(value) or value is None:
        return None
    value = str(value).strip()
    value = re.sub(r"[^A-Za-z0-9\s]", "", value)
    value = re.sub(r"\s+", " ", value).strip().lower()
    value = value.replace(" ", "")
    return value if value != "" else None

# ---------------------------------------------------------------------
# get source table 
# ---------------------------------------------------------------------

def get_source_table():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        query = f"""
        SELECT table_name 
        FROM "{LOG_SCHEMA}"."{META_DATA}" 
        WHERE table_name ILIKE 'base_%'
        AND stage_loaded = 'YES'
        AND is_base_cleaned = 'NO'
        ORDER BY last_updated_ts DESC;
        """
        results = conn.execute(text(query)).fetchall()
        if results:
            return [r[0] for r in results]
        else:
            print(f"⚠️ No base tables found in {SOURCE_SCHEMA}, skipping base_clean and moving on.")
            return []
        
# ---------------------------------------------------------------------
# Clean the base data and load into postgres aggregation layer
# ---------------------------------------------------------------------

def cleanse_and_load_base_tables(**context):
    

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    pipeline_run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    source_tables = get_source_table()
    if not source_tables:
        print("no base found moving to next step")
        return
    print(f"Found tables to process: {source_tables}")

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";'))
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LOG_SCHEMA}";'))

    for source_table in source_tables:
        try:
            print(f"\n▶ Processing table: {source_table}")
            chunk_iter = pd.read_sql(text(f'SELECT * FROM "{SOURCE_SCHEMA}"."{source_table}"'), engine, chunksize=50000)

            removed_rows_all = pd.DataFrame()

            for i, df in enumerate(chunk_iter):
                print(f"Processing chunk {i+1} with {len(df)} rows")
                df["file_source"] = source_table
                df["pipeline_run_time"] = pipeline_run_time  # ✅ Add processing time to cleaned data

                print("🔍 Raw Columns with repr():")
                for col in df.columns:
                    print(repr(col))

                # Step 1: Normalize column names in DataFrame
                df.columns = [col.strip().lower() for col in df.columns]

                # Step 2: Normalize keys in mapping
                normalized_mapping = {k.strip().lower(): v for k, v in COLUMN_MAPPING.items()}

                # Step 3: Apply column mapping
                original_columns = set(df.columns)
                mapped_columns = set(normalized_mapping.keys()).intersection(original_columns)
                unmapped_columns = original_columns - mapped_columns

                df.rename(columns=normalized_mapping, inplace=True)
                
                # Find mapped columns
                mapped_columns = set(original_columns).intersection(COLUMN_MAPPING.keys())
                updated_columns = {col: COLUMN_MAPPING[col] for col in mapped_columns if col in COLUMN_MAPPING}

                print(f"🔄 Column Mapping Applied: {len(updated_columns)} columns changed.")
                for old_col, new_col in updated_columns.items():
                    print(f"   🔹 `{old_col}` → `{new_col}`")

                # 🧹 Clean core identifiers and create cleaned_* columns
                for src, tgt in {
                    "chassis_no": "cleaned_chassis_no",
                    "engine_no": "cleaned_engine_no",
                    "insured_name": "cleaned_insured_name",
                    "veh_reg_no": "cleaned_veh_reg_no",
                    "new_branch_name_2": "cleaned_new_branch_name",
                    "model": "cleaned_model"
                }.items():
                    if src in df.columns:
                        df[tgt] = df[src].astype(str).apply(clean_text)

                # 📌 Create chassis_engine_no
                if "cleaned_chassis_no" in df.columns and "cleaned_engine_no" in df.columns:
                    df["chassis_engine_no"] = df["cleaned_chassis_no"] + "_" + df["cleaned_engine_no"]

                removed_rows = pd.DataFrame()

                # 🚫 Remove duplicate rows
                duplicate_rows = df[df.duplicated()].copy()
                df.drop_duplicates(inplace=True)
                duplicate_rows["removal_reason"] = "Duplicate row"
                removed_rows = pd.concat([removed_rows, duplicate_rows])

                # 🚫 Invalid premiums
                if "total_premium_payable" in df.columns:
                    df["total_premium_payable"] = pd.to_numeric(df["total_premium_payable"], errors="coerce")
                    invalid = df[df["total_premium_payable"].isna() | (df["total_premium_payable"] <= 0)].copy()
                    df = df[df["total_premium_payable"].notna() & (df["total_premium_payable"] > 0)]
                    invalid["removal_reason"] = "Invalid premium"
                    removed_rows = pd.concat([removed_rows, invalid])

                # 🚫 Invalid dates
                if "policy_start_date" in df.columns and "policy_end_date" in df.columns:
                    df["policy_start_date"] = pd.to_datetime(df["policy_start_date"], errors="coerce")
                    df["policy_end_date"] = pd.to_datetime(df["policy_end_date"], errors="coerce")
                    bad_dates = df[df["policy_start_date"].isna() | df["policy_end_date"].isna()].copy()
                    df = df[df["policy_start_date"].notna() & df["policy_end_date"].notna()]
                    bad_dates["removal_reason"] = "Invalid dates"
                    removed_rows = pd.concat([removed_rows, bad_dates])

                # 📌 Drop duplicate policies keeping highest premium
                if "policy_no" in df.columns and "total_premium_payable" in df.columns:
                    df = df.sort_values("total_premium_payable", ascending=False).drop_duplicates(subset=["policy_no"])

                removed_rows_all = pd.concat([removed_rows_all, removed_rows])
                df["cleaned_timestamp"] = datetime.now()
                # ✅ Write cleaned chunk to Cleaned schema using isolated connection
                with engine.begin() as write_conn:
                    df.to_sql(name=source_table, schema=TARGET_SCHEMA, con=write_conn, if_exists="append", index=False, chunksize=50000, method='multi')
                    row_count = len(df)
            # 🧾 Log removed rows after all chunks using isolated connection
            if not removed_rows_all.empty:
                removed_rows_all["pipeline_run_time"] = pipeline_run_time
                log_table = f"removed_{source_table}"
                # 🔁 Reconnect before writing to log schema
                engine.dispose()
                sleep(2)
                hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
                engine = hook.get_sqlalchemy_engine()

                with engine.begin() as log_conn:
                    removed_rows_all.to_sql(name=log_table, schema=LOG_SCHEMA, con=log_conn, if_exists="replace", index=False, chunksize= 2000)
                print(f"Removed rows logged in {LOG_SCHEMA}.{log_table}")
            
            clean_cnt = row_count
            removed_cnt = len(removed_rows_all)

            print(f"✅ Loaded cleaned data into {TARGET_SCHEMA}.{source_table}")
            print(f"   ➤ Clean rows: {clean_cnt}")
            print(f"   ➤ Removed rows: {removed_cnt}")

            # 🔁 Refresh connection before metadata update
            engine.dispose()
            sleep(2)
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()
            update_metadata(
                source_table,
                "is_base_cleaned",
                True,
                row_count=clean_cnt,
                removed_count=removed_cnt
            )

        except Exception as e:
            print(f"❌ Failed to process {source_table}: {e}")
            continue
  
# with DAG(
#     dag_id="cleanse_and_load_base_data",
#     default_args={
#         "owner": "airflow",
#         "start_date": datetime(2024, 2, 10),
#         "retries": 3,
#         "retry_delay": timedelta(minutes=5)
#     },
#     schedule_interval=None,
#     catchup=False
# ) as dag:
#     cleanse_and_load_task = PythonOperator(
#         task_id="cleanse_and_load_base_data",
#         python_callable=cleanse_and_load_base_tables
#     )