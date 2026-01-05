# ====================================================================
# 📦 Airflow ETL: Azure Blob → PostgreSQL (PEP8 + Flake8 Clean)
# ====================================================================
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text
import logging
logger = logging.getLogger(__name__)
from sqlalchemy.types import String
from airflow.models import Variable
from pathlib import Path
from sqlalchemy.exc import OperationalError
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.schema_table_config import get_log_tables, get_schema, get_column_mapping
from crypto.crypto_utils import get_fernet, encrypt_value, decrypt_value
from utils.config_loader import load_sensitive_columns

# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------
DAG_DIR = Path("/opt/airflow")
JSON_PATH = str(DAG_DIR / "config" / "schema_metadata_config.json")

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
ARCHIVE_SCHEMA = get_schema("archive_log", JSON_PATH)
SOURCE_SCHEMA = get_schema("agg", JSON_PATH)
COLUMN_JSON = str(DAG_DIR / "config" / "column_mapping.json")
ZONE_TABLE = get_column_mapping("zonemapping", COLUMN_JSON)
BATCH_SIZE = 50000
TARGET_TABLE = "final_cleaned_basepr"
META_TABLE = get_log_tables("metadata", JSON_PATH)
CLAIM_TABLE = get_log_tables("claimlog", JSON_PATH)
LOG_SCHEMA = get_schema("log", JSON_PATH)
FEATURE_LOG = get_log_tables("featurelog", JSON_PATH)

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
# To Fetch latest claim_merged_with_basepr table from metadata 
# ---------------------------------------------------------------------
def get_latest_claim_merged_with_basepr(engine):
    query = f"""
        SELECT appended_table_name 
        FROM "{LOG_SCHEMA}"."{META_TABLE}"
        WHERE is_basepr_appended = 'YES'
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
    try:

        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = postgres_hook.get_sqlalchemy_engine()

        # 1. Get latest source table dynamically
        SOURCE_TABLE = get_latest_claim_merged_with_basepr(engine)
        if not SOURCE_TABLE:
            raise ValueError("❌ No claim_merged_with_basepr table found in metadata!")
        fernet = get_fernet()
        sensitive_cols = load_sensitive_columns()
        logger.info("🔐 Fernet initialized & sensitive columns loaded")
        # 2. Read data
        query = f'SELECT * FROM "{SOURCE_SCHEMA}"."{SOURCE_TABLE}"'
        df = pd.read_sql(query, engine)
        logger.info(f"✅ Fetched {len(df)} rows from {SOURCE_SCHEMA}.{SOURCE_TABLE}")
        # 🔓 Decrypt sensitive columns before cleaning
        for col in df.columns:
            if col in sensitive_cols:
                df[col] = df[col].apply(
                    lambda x: decrypt_value(x, fernet)
                    )

        logger.info(f"🔓 Decrypted sensitive columns for {SOURCE_TABLE}")
        # 3. Apply zone mapping

        df['zone'] = df.apply(
            lambda row: ZONE_TABLE.get(str(row['state']).upper(), row['zone'])
            if pd.isna(row['zone']) else row['zone'],
            axis=1
        )
        logger.info(f"✅ Applied zone mapping from {ZONE_TABLE}")
        # collect all removed rows here
        removed_rows_all = pd.DataFrame()


        if 'corrected_name' in df.columns:
            removed = df[
                (df["corrected_name"].isna()) |
                (df["corrected_name"].astype(str).str.strip() == "")
            ].copy()

            if not removed.empty:
                removed["removal_reason"] = "corrected_name is NULL or blank"
                removed_rows_all = pd.concat(
                    [removed_rows_all, removed],
                    ignore_index=True
                )

            df = df[
                (df["corrected_name"].notna()) &
                (df["corrected_name"].astype(str).str.strip() != "")
            ]

            logger.info(f"⚠️ Removed {len(removed)} rows with null or blank corrected_name")

        
        # ✅ Vehicle Age Cleaning: blank → 0, datatype → int
        if "vehicle_age" in df.columns:
            df["vehicle_age"] = df["vehicle_age"].replace("(blank)", None)
            df["vehicle_age"] = pd.to_numeric(df["vehicle_age"], errors="coerce")
            df["vehicle_age"] = df["vehicle_age"].round().astype(float)
        logger.info("vechicle age cleaning done")

        # ✅ applicable_discount_with_ncb Cleaning: blank → 0, datatype → int
        if "applicable_discount_with_ncb" in df.columns:
            df["applicable_discount_with_ncb"] = df["applicable_discount_with_ncb"].replace("(blank)", None)
            df["applicable_discount_with_ncb"] = pd.to_numeric(
                df["applicable_discount_with_ncb"], errors="coerce"
            )
            df["applicable_discount_with_ncb"] = df["applicable_discount_with_ncb"].fillna(0)
            df["applicable_discount_with_ncb"] = df["applicable_discount_with_ncb"].round()
            df["applicable_discount_with_ncb"] = df["applicable_discount_with_ncb"].astype(float)

        logger.info("applicable_discount_with_ncb cleaning done")

        if "vehicle_idv" in df.columns:
            df["vehicle_idv"] = df["vehicle_idv"].replace("(blank)", None)
            df["vehicle_idv"] = pd.to_numeric(df["vehicle_idv"], errors="coerce")
            df["vehicle_idv"] = df["vehicle_idv"].fillna(0)
            df["vehicle_idv"] = df["vehicle_idv"].round().astype(float)

        logger.info("vehicle_idv cleaning completed")

        # ✅ previous_year_ncb_percentage Cleaning: blank → 0, datatype → int
        if "previous_year_ncb_percentage" in df.columns:
            df["previous_year_ncb_percentage"] = df["previous_year_ncb_percentage"].replace("(blank)", None)
            df["previous_year_ncb_percentage"] = pd.to_numeric(
                df["previous_year_ncb_percentage"], errors="coerce"
            )
            df["previous_year_ncb_percentage"] = df["previous_year_ncb_percentage"].fillna(0)
            df["previous_year_ncb_percentage"] = df["previous_year_ncb_percentage"].round().astype(float)

        logger.info("previous_year_ncb_percentage cleaning completed")

        if "tie_up" in df.columns:
            df["tie_up"] = df["tie_up"].fillna("Non-OEM")
        logger.info("tie_up cleaning completed")

        if "fuel_type" in df.columns:
            df["fuel_type"] = df["fuel_type"].replace(['-', '(blank)'], pd.NA)
            df["fuel_type"] = df["fuel_type"].fillna("Petrol")
        logger.info("fuel_type cleaning completed")
        # 🔐 Re-encrypt sensitive columns before loading
        for col in df.columns:
            if col in sensitive_cols:
                df[col] = df[col].apply(lambda x: encrypt_value(x, fernet))

        logger.info(f"🔐 Re-encrypted sensitive columns before loading {SOURCE_SCHEMA}.{TARGET_TABLE}")

        # 🔴 Archive & log removed rows (addons)
        if not removed_rows_all.empty:

            removed_rows_all["source_table"] = SOURCE_TABLE
            removed_rows_all["pipeline_run_time"] = datetime.utcnow()

            # encrypt sensitive columns in removed rows
            for col in removed_rows_all.columns:
                if col in sensitive_cols:
                    removed_rows_all[col] = removed_rows_all[col].apply(
                        lambda x: encrypt_value(x, fernet)
                    )

            archive_and_log_removed_rows(
                engine=engine,
                removed_df=removed_rows_all,
                source_table=TARGET_TABLE,
                log_schema=LOG_SCHEMA,
                archive_schema=ARCHIVE_SCHEMA,
            )

            logger.info(
                "📦 Logged %d removed rows for addons",
                len(removed_rows_all)
            )
        else:
            logger.info("📦 No rows removed during addons step")

        # # BOOKED = 1 ⇒ renewed_flag = 1
        # if "booked" in df.columns and "renewed_flag" in df.columns:
        #     mask = df["booked"] == 1
        #     df.loc[mask, "renewed_flag"] = 1
        # 🔥 TRUNCATE FIRST (EXACT PLACE)
        with engine.begin() as conn:
            exists = conn.execute(
                text("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = :schema
                        AND table_name = :table
                    )
                """),
                {"schema": SOURCE_SCHEMA, "table": TARGET_TABLE}
            ).scalar()

            if exists:
                conn.execute(
                    text(f'TRUNCATE TABLE "{SOURCE_SCHEMA}"."{TARGET_TABLE}"')
                )
        
        # 5. Save back to same source table
        df.to_sql(
            name=TARGET_TABLE,
            schema=SOURCE_SCHEMA,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=BATCH_SIZE,
            dtype={"zone": String}
        )
        logger.info(f"✅ zone mapping completed — {df['zone'].isna().sum()} nulls remaining")

        # 6. Update feature engineering log with row count + target table name
        row_count = len(df)
        update_feature_log(engine, "addons", TARGET_TABLE, row_count)
        logger.info(f"✅ Feature log updated for addons = {TARGET_TABLE}, row_count = {row_count}")

    except Exception:
        logger.error(
            "❌ Failed during addons processing",
            exc_info=True
        )
        raise

with DAG(
    dag_id = "zone_mapp",
    default_args = {"owner":"airflow","start_date":datetime(2024,1,1)},
    schedule_interval = None,
    catchup = False,
    tags = ["map","zone"]
) as dag:

    update_zone_task = PythonOperator(
        task_id = 'update_zone_task',
        python_callable = addon_column
    )

    update_zone_task