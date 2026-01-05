# ====================================================================
# 📦 Airflow ETL: Azure Blob → PostgreSQL (PEP8 + Flake8 Clean)
# ====================================================================
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import logging
logger = logging.getLogger(__name__)
import numpy as np
import re
from sqlalchemy.exc import OperationalError
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy.types import Text,Integer,Float,DateTime
from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time, gc
from pathlib import Path
from utils.schema_table_config import get_schema,get_log_tables
from crypto.crypto_utils import get_fernet, encrypt_value, decrypt_value
from utils.config_loader import load_sensitive_columns

# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------

DAGS_DIR = Path("/opt/airflow")
JSON_PATH = str(DAGS_DIR / "config" / "schema_metadata_config.json")

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
SOURCE_SCHEMA = get_schema("stage", JSON_PATH)
TARGET_SCHEMA = get_schema("agg", JSON_PATH)
TARGET_TABLE1 = "claim_append"
TARGET_TABLE2 = "claim_merge"
LOG_SCHEMA = get_schema("log", JSON_PATH)
ARCHIVE_SCHEMA = get_schema("archive_log", JSON_PATH)
CLAIM_LOG = get_log_tables("claimlog", JSON_PATH)
META_DATA = get_log_tables("metadata", JSON_PATH)

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

# # ---------------------------------------------------------------------
# # Removed Log Details
# # ---------------------------------------------------------------------

# def log_removed_rows(df_removed, reason, engine):
#     """Log removed claim rows with reason into pip_log.claim_removed_reason"""
#     if df_removed.empty:
#         return
#     df_removed = df_removed.copy()
#     df_removed["removal_reason"] = reason
#     df_removed["logged_at"] = datetime.utcnow()
#     df_removed.to_sql(
#         name="claim_removed_reason",
#         schema=LOG_SCHEMA,
#         con=engine,
#         if_exists="append",
#         index=False
#     )
#     logger.info(f"⚠️ Logged {len(df_removed)} removed rows – Reason: {reason}")

# ---------------------------------------------------------------------
# Data Type Mapping
# ---------------------------------------------------------------------

def d_mapping(df):
    mapping = {}
    for c in df.columns:
        s = df[c]
        if pd.api.types.is_integer_dtype(s):
            mapping[c] = Integer()
        elif pd.api.types.is_float_dtype(s):
            mapping[c] = Float()
        elif pd.api.types.is_datetime64_any_dtype(s):
            mapping[c] = DateTime()
        else:
            mapping[c] = Text()
    return mapping
# ---------------------------------------------------------------------
# Getting Base & Pr Table from Meta data 
# ---------------------------------------------------------------------
def get_latest_basepr(engine):
    query = f"""
        SELECT renewal_policy_table
        FROM "{LOG_SCHEMA}"."{META_DATA}"
        WHERE renewal_policy_table IS NOT NULL
        ORDER BY last_updated_ts DESC
        LIMIT 1
    """
    result = engine.execute(text(query)).fetchone()
    return result[0] if result else None
# ---------------------------------------------------------------------
# Updating Metadata 
# ---------------------------------------------------------------------
def update_claim_metadata(engine, table_name, row_count=None):
    """Update claim_logs after append step"""
    with engine.begin() as conn:
        conn.execute(text(f"""
            UPDATE "{LOG_SCHEMA}"."{CLAIM_LOG}"
            SET is_appended = 'YES',
                appended_count = :row_count,
                last_updated_ts = :ts
            WHERE table_name = :table_name
        """), {
            "table_name": table_name,
            "row_count": row_count if row_count is not None else 0,
            "ts": datetime.utcnow()
        })

# ---------------------------------------------------------------------
# Appending Claim Tables 
# ---------------------------------------------------------------------
def append_claim_table():
    try:


        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";'))
        fernet = get_fernet()
        sensitive_cols = load_sensitive_columns()
        logger.info("🔐 Fernet initialized & sensitive columns loaded")

        logger.info("connection done")

        # dispose any stale pool first
        engine.dispose()
        time.sleep(2)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        """Append all staged claim tables into one consolidated claim_append table."""
        query = f"""
            SELECT table_name 
            FROM "{LOG_SCHEMA}"."{CLAIM_LOG}"
            WHERE stage_loaded = 'YES' 
            ORDER BY year ASC
        """
        claim_tables = pd.read_sql(query, engine)
        if claim_tables.empty:
            logger.warning("🚫 No claim tables pending append.")
            return

        dfs = []
        for _, row in claim_tables.iterrows():
            table_name = row["table_name"]
            df = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA}"."{table_name}"', engine)
            # 🔓 Decrypt sensitive columns for this claim table
            for col in df.columns:
                if col in sensitive_cols:
                    df[col] = df[col].apply(lambda x: decrypt_value(x, fernet))
            logger.info(f"decrypt done in {table_name}")
            # Fix column renaming if required
            if "status_of_claim.1" in df.columns and "updated_status" not in df.columns:
                df.rename(columns={"status_of_claim.1": "updated_status"}, inplace=True)
            
            dfs.append(df)
            logger.info(f"📂 Extracted {len(df)} rows from {table_name}.")

            # ✅ Update log for this specific table
            update_claim_metadata(engine, table_name, row_count=len(df))

        # Combine all claims into single claim_append
        final_df = pd.concat(dfs, ignore_index=True)
        # 🔐 Re-encrypt sensitive columns before loading
        for col in final_df.columns:
            if col in sensitive_cols:
                final_df[col] = final_df[col].apply(lambda x: encrypt_value(x, fernet))
        logger.info("encrpted the final data getting inside db")
        with engine.begin() as conn:
            conn.execute(
                text(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."claim_append"')
            )
        final_df.to_sql("claim_append", schema=TARGET_SCHEMA, con=engine, if_exists="append", index=False)

        logger.info(f"✅ Appended all claim tables into {TARGET_SCHEMA}.claim_append with {len(final_df)} rows.")
        engine.dispose()
        gc.collect()
    except Exception:
        logger.error(
            "❌ Failed during append_claim_table",
            exc_info=True
        )
        raise
# ---------------------------------------------------------------------
# Merging All Claim table With Aggregated Columns
# ---------------------------------------------------------------------

def merge_claim_table():
    try:

        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        fernet = get_fernet()
        sensitive_cols = load_sensitive_columns()
        logger.info("🔐 Fernet initialized & sensitive columns loaded")
        query = f' SELECT * FROM "{TARGET_SCHEMA}"."{TARGET_TABLE1}"'
        df = pd.read_sql(query,engine)
        # 🔓 Decrypt sensitive columns before claim merge logic
        for col in df.columns:
            if col in sensitive_cols:
                df[col] = df[col].apply(lambda x: decrypt_value(x, fernet))

        def clean_name(name):
            return re.sub(r'[^a-zA-Z0-9]','',str(name)).lower()
        
        # collect all removed rows here
        removed_rows_all = pd.DataFrame()

        df["cleaned_insured_name"] = df["insured_name"].apply(clean_name)
        logger.info("clean doen for insured name")

        # Step 1: Ensure the columns used for grouping have consistent data types
        group_cols = [ 'policy_no', 'policy_start_date', 'policy_end_date']

        df[group_cols] = df[group_cols].astype(str)

        # Step 2: Count the number of claims for each group
        df['Number_of_claims'] = df.groupby(group_cols)['policy_no'].transform('count')
        logger.info(f"policy count done")
        # Step 3: Create the columns for PAID, WITHDRAWN, CLOSURE OF CLAIM, REPUDIATION, CLOSURE OF CLAIMS
        status_cols = ['PAID', 'WITHDRAWN', 'CLOSURE OF CLAIM', 'REPUDIATION', 'CLOSURE OF CLAIMS']

        # Before removing duplicates, count the occurrences of each status in 'Updated Status'
        df_status = df.groupby(group_cols)['updated_status'].value_counts().unstack().fillna(0).astype(int)

        # Rename the columns to match your required output format
        df_status = df_status.rename(columns={
            'PAID': 'PAID',
            'WITHDRAWN': 'WITHDRAWN',
            'CLOSURE OF CLAIM': 'CLOSURE OF CLAIM',
            'REPUDIATION': 'REPUDIATION',
            'CLOSURE OF CLAIMS': 'CLOSURE OF CLAIM'
        })

        if "CLOSURE OF CLAIMS" not in df_status.columns:
            df_status["CLOSURE OF CLAIMS"] = 0

        logger.info(f'status done')
        df['claim_status'] = df['updated_status'].map({'PAID': 'Approved'}).fillna('Denied')
        
        # Step 4: Count the occurrences of 'Approved' and 'Denied' in 'Claim status'
        df_claim_status = (
        df.groupby(group_cols)['claim_status']
        .value_counts()
        .unstack()
        .reindex(columns=['Approved','Denied'], fill_value=0)
        .fillna(0)
        .astype(int)
            )

        # Rename the columns to match the required output format for 'Claim status'
        df_claim_status = df_claim_status.rename(columns={
            'Approved': 'Approved',
            'Denied': 'Denied'
        })

        # Step 5: Convert 'Settle date' to datetime and coerce errors
        df['settle_date'] = pd.to_datetime(df['settle_date'], errors='coerce')
        logger.info(f'approved denined done')
        # capture duplicates before dropping
        removed_dupes = df[df.duplicated(subset=group_cols, keep="last")]
        removed_dupes = removed_dupes.copy()
        for col in removed_dupes.columns:
            if col in sensitive_cols:
                removed_dupes[col] = removed_dupes[col].apply(
                    lambda x: encrypt_value(x, fernet)
                )
        if not removed_dupes.empty:
            removed_dupes["removal_reason"] = "Duplicate claim (keeping latest by settle_date)"
            removed_rows_all = pd.concat(
                [removed_rows_all, removed_dupes],
                ignore_index=True
            )

        # Step 6: Sort by 'Settle date' and select the latest row for each group
        df_latest = df.sort_values(by='settle_date').drop_duplicates(subset=group_cols, keep='last')

        # Step 7: Handling duplicates based on 'Settle date'
        def resolve_duplicates(group):
            if len(group) > 1:
                # Count the null values
                group['null_count'] = group.isnull().sum(axis=1)
                # Select the row with the fewest nulls
                return group.loc[group['null_count'].idxmin()]
            else:
                return group.iloc[0]

        df_latest_cleaned = df_latest.groupby(group_cols).apply(resolve_duplicates).reset_index(drop=True)

        # Step 8: Merge the status counts from 'Updated Status' and 'Claim status' back into the filtered DataFrame to retain all columns
        df_final = pd.merge(df_latest_cleaned, df_status, on=group_cols, how='left')
        df_final = pd.merge(df_final, df_claim_status, on=group_cols, how='left')

        # Step 9: Select ALL columns + the calculated columns
        all_cols = df.columns.tolist() + status_cols + ['Approved', 'Denied', 'Number_of_claims']
        df_final = df_final[all_cols]
        logger.info(f'duplicate removal done')
        # 🔐 Re-encrypt sensitive columns
        for col in df_final.columns:
            if col in sensitive_cols:
                df_final[col] = df_final[col].apply(lambda x: encrypt_value(x, fernet))
        logger.info(f"🔐 Re-encrypted sensitive columns before loading {TARGET_SCHEMA}.{TARGET_TABLE1}")

        # --------------------------------------------------
        # 🕒 DEFINE Postgres datetime columns (EXPLICIT)
        # --------------------------------------------------
        DATETIME_COLS = [
            "policy_start_date",
            "policy_end_date",
            "loss_date",
            "notification_date",
            "settle_date",
            "dat_registration_date",
            "last_runned_date",
        ]

        # --------------------------------------------------
        # 🧼 FIX datetime columns (NULL-safe for Postgres)
        # --------------------------------------------------
        for col in DATETIME_COLS:
            if col in df_final.columns:
                df_final[col] = (
                    df_final[col]
                    .replace("", None)
                    .replace("NaT", None)
                    .replace(pd.NaT, None)
                )

        logger.info("🕒 Datetime columns sanitized (NULL safe)")
        
        # 🔴 Archive & log removed claim rows
        if not removed_rows_all.empty:

            removed_rows_all["source_table"] = TARGET_TABLE1
            removed_rows_all["pipeline_run_time"] = datetime.utcnow()
            # 🔧 FIX: Normalize datetime columns before encryption & DB insert
            for col in removed_rows_all.columns:
                if pd.api.types.is_datetime64_any_dtype(removed_rows_all[col]):
                    removed_rows_all[col] = (
                        removed_rows_all[col]
                        .astype(str)
                        .replace("NaT", None)
                    )

            # 🔧 Fill NaN only for NON-datetime columns
            non_datetime_cols = [
                c for c in removed_rows_all.columns
                if not pd.api.types.is_datetime64_any_dtype(removed_rows_all[c])
            ]

            removed_rows_all[non_datetime_cols] = removed_rows_all[non_datetime_cols].fillna("")
            logger.info(f"converted into string for empty places")
            # encrypt sensitive columns in removed rows
            for col in removed_rows_all.columns:
                if col in sensitive_cols:
                    removed_rows_all[col] = removed_rows_all[col].apply(
                        lambda x: encrypt_value(x, fernet)
                    )

            archive_and_log_removed_rows(
                engine=engine,
                removed_df=removed_rows_all,
                source_table=TARGET_TABLE1,
                log_schema=LOG_SCHEMA,
                archive_schema=ARCHIVE_SCHEMA,
            )

            logger.info(
                "📦 Logged %d removed claim rows with archive",
                len(removed_rows_all)
            )
        else:
            logger.info("📦 No claim rows removed during merge step")
        with engine.begin() as conn:
            conn.execute(
                text(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."{TARGET_TABLE2}"')
            )

        df_final.to_sql(name=TARGET_TABLE2,schema=TARGET_SCHEMA,con=engine,if_exists="append",index=False)
        # ✅ Insert or update log entry for claim_merge
        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO "{LOG_SCHEMA}"."{CLAIM_LOG}" (table_name, is_merged, merged_count, last_updated_ts)
                VALUES ('claim_merge', 'YES', :row_count, :ts)
                ON CONFLICT (table_name) DO UPDATE
                SET is_merged = 'YES',
                    merged_count = :row_count,
                    last_updated_ts = :ts
            """), {
                "row_count": len(df_final),
                "ts": datetime.utcnow()
            })

        logger.info(f"✅ {len(df_final)} rows merged into {TARGET_SCHEMA}.claim_merge")
    except Exception:
        logger.error(
            "❌ Failed during merge claim tables",
            exc_info=True
        )
        raise

# ---------------------------------------------------------------------
# Merging Base Pr Table With Claim Tables 
# ---------------------------------------------------------------------

def merge_basepr_with_claim():
    try:
    
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        logger.info("engine connection done")
        fernet = get_fernet()
        sensitive_cols = load_sensitive_columns()
        engine.dispose()
        time.sleep(2)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        BASE_PR_TABLE = get_latest_basepr(engine)
        if not BASE_PR_TABLE:
            raise ValueError("No renewal policy table found in metadata!")
        CLAIM_TABLE = TARGET_TABLE2
        FINAL_TABLE = "basepr_merged_with_claim"
        # Load claim ONCE - full, since it's smaller and doesn't cause issues
        claim = pd.read_sql(f'SELECT * FROM "{TARGET_SCHEMA}"."{CLAIM_TABLE}"', con=engine)
        for col in claim.columns:
            if col in sensitive_cols:
                claim[col] = claim[col].apply(lambda x: decrypt_value(x, fernet))

        for col in ["policy_start_date", "policy_end_date"]:
            if col in claim.columns:
                claim[col] = pd.to_datetime(claim[col], errors="coerce")

        logger.info("✅ Loaded claim table")

        chunk_size = 100000  # try 100k rows per batch
        offset = 0
        first = True
        total_rows = 0

        with engine.begin() as conn:
            conn.execute(
                text(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."{FINAL_TABLE}"')
            )

        while True:
            query = f"""
                SELECT * FROM "{TARGET_SCHEMA}"."{BASE_PR_TABLE}"
                ORDER BY policy_no
                OFFSET {offset} LIMIT {chunk_size}
            """
            base_pr = pd.read_sql(query, con=engine)
            for col in base_pr.columns:
                if col in sensitive_cols:
                    base_pr[col] = base_pr[col].apply(lambda x: decrypt_value(x, fernet))

            if base_pr.empty:
                break

            for col in ["policy_start_date", "policy_end_date"]:
                if col in base_pr.columns:
                    base_pr[col] = pd.to_datetime(base_pr[col], errors="coerce")

            merged = base_pr.merge(
                claim,
                on=["policy_no", "policy_start_date", "policy_end_date"],
                how="left",
                suffixes=("_basepr", "_claim")
            )

            logger.info(f"✅ Merged chunk at offset {offset} with {len(merged)} rows")
            # ⭐ Add timestamp column
            merged["merge_timestamp"] = datetime.utcnow()
            # 🔐 Re-encrypt sensitive columns before final write
            for col in merged.columns:
                if col in sensitive_cols:
                    merged[col] = merged[col].apply(lambda x: encrypt_value(x, fernet))
            
            merged.to_sql(
                name=FINAL_TABLE,
                schema=TARGET_SCHEMA,
                con=engine,
                if_exists="append",
                index=False,
                dtype=d_mapping(merged)
            )
            total_rows += len(merged)
            offset += chunk_size
            first = False

        # ✅ Insert or update log entry for basepr_merged_with_claim
        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO "{LOG_SCHEMA}"."{CLAIM_LOG}" (table_name, is_basepr_claim_merged,
                                                    cnt, last_updated_ts)
                VALUES ('basepr_merged_with_claim', 'YES', :row_count, :ts)
                ON CONFLICT (table_name) DO UPDATE
                SET is_basepr_claim_merged = 'YES',
                    cnt = :row_count,
                    last_updated_ts = :ts
            """), {
                "row_count": total_rows,
                "ts": datetime.utcnow()
            })

        logger.info(f"🎉 Successfully merged {len(merged)} rows into {TARGET_SCHEMA}.basepr_merged_with_claim")

        # --- ✅ cleanup ---
        engine.dispose()
        gc.collect()
    except Exception:
        logger.error(
            "❌ Failed during merge base pr claim",
            exc_info=True
        )
        raise

with DAG(
    dag_id = "append_merge_claim_tables_dag",
    default_args = {"owner":"airflow","start_date":datetime(2024,1,1)},
    schedule_interval = None,
    catchup = False,
    tags = ["claim","merge"]
)as dag:
    
    append_claim = PythonOperator(
        task_id = "append_claim_tables", 
        python_callable = append_claim_table
    )

    merge_claim = PythonOperator(
        task_id = "merge_claim_table",
        python_callable = merge_claim_table
    )

    final_table = PythonOperator(
        task_id = "basepr_append_with_claim",
        python_callable = merge_basepr_with_claim
    )

    

    append_claim >> merge_claim >> final_table