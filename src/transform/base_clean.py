# ============================================================
# 📦 Airflow ETL: Azure Blob → PostgreSQL (PEP8 + Flake8 Clean)
# ============================================================

import pandas as pd
import re
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
logger = logging.getLogger(__name__)

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from airflow import DAG
from airflow.operators.python import PythonOperator
from time import sleep
from pathlib import Path
from datetime import timedelta, datetime
from utils.schema_table_config import get_schema, get_log_tables, get_column_mapping
from crypto.crypto_utils import get_fernet , encrypt_value , decrypt_value
from utils.config_loader import load_sensitive_columns

# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------
DAGS_DIR = Path("/opt/airflow")
JSON_PATH = str(DAGS_DIR / "config" / "schema_metadata_config.json")

SOURCE_SCHEMA = get_schema("stage", JSON_PATH)
TARGET_SCHEMA = get_schema("agg", JSON_PATH)
LOG_SCHEMA = get_schema("log", JSON_PATH)
ARCHIVE_SCHEMA = get_schema("archive_log", JSON_PATH)
POSTGRES_CONN_ID = "postgres_cloud_prochurn"
META_DATA = get_log_tables("metadata", JSON_PATH)
# source_table = "base_22_n"

COLUMN_JSON = str(DAGS_DIR / "config" / "column_mapping.json")
COLUMN_MAPPING = get_column_mapping("base", COLUMN_JSON)

# ---------------------------------------------------------------------
# 🗃️ Archive logs
# --------------------------------------------------------------------- 

def archive_and_log_removed_rows(
    engine,
    removed_df,
    source_table,
    log_schema,
    archive_schema,
):
    """
    Archives existing removed rows table (if exists) and writes new removed rows
    into log schema using if_exists=replace.
    """

    if removed_df.empty:
        logger.info(
            "No removed rows to log for table %s",
            source_table,
        )
        return

    log_table = f"removed_{source_table}"
    archive_ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    archive_table = f"{log_table}_{archive_ts}"

    with engine.begin() as conn:
        exists = conn.execute(
            text(
                f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = '{log_schema}'
                    AND table_name = '{log_table}'
                )
                """
            )
        ).scalar()

        if exists:
            logger.info(
                "Archiving existing removed rows table %s.%s → %s.%s",
                log_schema,
                log_table,
                archive_schema,
                archive_table,
            )

            conn.execute(
                text(
                    f"""
                    CREATE TABLE {archive_schema}.{archive_table}
                    AS
                    SELECT * FROM {log_schema}.{log_table}
                    """
                )
            )

            conn.execute(
                text(
                    f"""
                    DROP TABLE {log_schema}.{log_table}
                    """
                )
            )

        logger.info(
            "Writing %d removed rows into %s.%s",
            len(removed_df),
            log_schema,
            log_table,
        )

        removed_df.to_sql(
            name=log_table,
            schema=log_schema,
            con=conn,
            if_exists="replace",
            index=False,
            chunksize=2000,
            method="multi",
        )

# ---------------------------------------------------------------------
# 🗃️ Update metadata logs
# ---------------------------------------------------------------------   

def update_metadata(table_name, step, status=True, row_count=None,removed_cnt=None, **context):
    

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
                    "removed_cnt": removed_cnt if removed_cnt is not None else 0,
                    "ts": datetime.utcnow()
                })
            engine.dispose()
            logger.info(f"✅ Metadata updated for {table_name}")
            return  # exit if successful

        except OperationalError as e:
            logger.error(f"⚠️ Metadata update failed (attempt {attempt+1}/3): {e}")
            sleep(5)
            continue  # retry on transient timeout errors

    logger.error(f"❌ Failed to update metadata for {table_name} after 3 retries.")

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
            logger.info(f"⚠️ No base tables found in {SOURCE_SCHEMA}, skipping base_clean and moving on.")
            return []
        
# ---------------------------------------------------------------------
# Clean the base data and load into postgres aggregation layer
# ---------------------------------------------------------------------

def cleanse_and_load_base_tables(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    pipeline_run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    source_tables = get_source_table()
    # source_tables = base_2022"
    if not source_tables:
        logger.info("no base found moving to next step")
        return
    logger.info(f"Found tables to process: {source_tables}")

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {ARCHIVE_SCHEMA};"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};"))
    # 🔐 Encryption setup (once per DAG run)
    fernet = get_fernet()
    sensitive_cols = load_sensitive_columns()

    logger.info("🔐 Fernet initialized & sensitive columns loaded")


    for source_table in source_tables:
        try:
            logger.info(f"\n▶ Processing table: {source_table}")
            df = pd.read_sql(text(f'SELECT * FROM "{SOURCE_SCHEMA}"."{source_table}"'), engine)
            # 🔓 Decrypt sensitive columns before cleaning
            for col in df.columns:
                if col in sensitive_cols:
                    df[col] = df[col].apply(
                        lambda x: decrypt_value(x, fernet)
                    )

            logger.info(f"🔓 Decrypted sensitive columns for {source_table}")

            removed_rows_all = pd.DataFrame()
            removed_cnt = 0
            
            df["file_source"] = source_table
            df["pipeline_run_time"] = pipeline_run_time  # ✅ Add processing time to cleaned data

            logger.info("🔍 Raw Columns with repr():")
            for col in df.columns:
                logger.info(repr(col))

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

            logger.info(f"🔄 Column Mapping Applied: {len(updated_columns)} columns changed.")
            for old_col, new_col in updated_columns.items():
                logger.info(f"   🔹 `{old_col}` → `{new_col}`")

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
                df_sorted = df.sort_values("total_premium_payable", ascending=False)
                removed_policy_dupes = df_sorted[df_sorted.duplicated(subset=["policy_no"], keep="first")].copy()
                removed_policy_dupes["removal_reason"] = "Duplicate policy_no (kept highest premium)"
                removed_rows = pd.concat([removed_rows, removed_policy_dupes])
                df = df_sorted.drop_duplicates(subset=["policy_no"], keep="first")

            removed_rows_all = pd.concat([removed_rows_all, removed_rows])
            removed_rows_all = removed_rows_all.copy()
            for col in removed_rows_all.columns:
                if col in sensitive_cols:
                    removed_rows_all[col] = removed_rows_all[col].apply(
                        lambda x: encrypt_value(x, fernet)
                    )
            logger.info(f"re-encrypted the removed rows in log ")

            # 🔐 Re-encrypt sensitive columns before loading
            for col in df.columns:
                if col in sensitive_cols:
                    df[col] = df[col].apply(
                        lambda x: encrypt_value(x, fernet)
                    )

            logger.info(f"🔐 Re-encrypted sensitive columns before loading {TARGET_SCHEMA}.{source_table}")
            # 🔥 TRUNCATE FIRST (exact place)
            with engine.begin() as conn:
                conn.execute(
                    text(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."{source_table}"')
                )
            # ✅ Write cleaned chunk to Cleaned schema using isolated connection
            with engine.begin() as write_conn:
                df.to_sql(name=source_table, schema=TARGET_SCHEMA, con=write_conn, if_exists="append", index=False,chunksize=50000,method='multi')
            logger.info(f"rows get loaded to {TARGET_SCHEMA}.{source_table} with row count of {len(df)}")
            removed_cnt = len(removed_rows_all)

            archive_and_log_removed_rows(
                engine=engine,
                removed_df=removed_rows_all,
                source_table=source_table,
                log_schema=LOG_SCHEMA,
                archive_schema=ARCHIVE_SCHEMA,
            )

            with engine.begin() as conn:
                row_cnt = conn.execute(text(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{source_table}")).scalar()
            logger.info(f"total row count in {TARGET_SCHEMA}.{source_table} is : {row_cnt}")

            # 🔁 Refresh connection before metadata update
            engine.dispose()
            sleep(2)

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()

            update_metadata(source_table, "is_base_cleaned", True, row_count=row_cnt,removed_cnt=removed_cnt)
        except Exception:
            logger.error(f"❌ Failed to process %s" ,source_table, exc_info=True)
            raise
  
with DAG(
    dag_id="cleanse_and_load_base_data",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2024, 2, 10),
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    },
    schedule_interval=None,
    catchup=False
) as dag:
    cleanse_and_load_task = PythonOperator(
        task_id="cleanse_and_load_base_data",
        python_callable=cleanse_and_load_base_tables
    )