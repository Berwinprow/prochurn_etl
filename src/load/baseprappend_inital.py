# ====================================================================
# 📦 Airflow ETL: Azure Blob → PostgreSQL (PEP8 + Flake8 Clean)
# ====================================================================
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text
from fuzzywuzzy import fuzz
from airflow.models import Variable
import logging
logger = logging.getLogger(__name__)
import json
from sqlalchemy.exc import OperationalError
from airflow import DAG
from airflow.operators.python import PythonOperator
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
from utils.schema_table_config import get_schema, get_log_tables
from crypto.crypto_utils import get_fernet, encrypt_value, decrypt_value
from utils.config_loader import load_sensitive_columns

# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------
DAGS_DIR = Path("/opt/airflow")
JSON_PATH = str(DAGS_DIR / "config" / "schema_metadata_config.json")
SOURCE_SCHEMA = get_schema("agg",JSON_PATH)
BASE_TABLE = "base_2022"
PR_TABLE = "pr_2022"
TARGET_SCHEMA = get_schema("agg",JSON_PATH) # both source and target has same schema name
TARGET_TABLE = "finalwith_2022_pr"
LOG_TABLE = "removed_duplicate_policies"
ARCHIVE_SCHEMA = get_schema("archive_log", JSON_PATH)
log_schema= get_schema("log",JSON_PATH)
META_TABLE = get_log_tables("metadata",JSON_PATH)
FINAL_TABLE = "final_renewed_policies"

# ---------------------------------------------------------------------
# Defining the Columns
# ---------------------------------------------------------------------
MANUFACTURER_COLUMN = "manufacturer"
REG_NO_COLUMN = "cleaned_veh_reg_no"
MODEL_COLUMN = "cleaned_model"
CHASSIS_COLUMN = "cleaned_chassis_no"
ENGINE_COLUMN = "cleaned_engine_no"
INSURED_NAME_COLUMN = "cleaned_insured_name"
MONTH_COLUMN = "month"
POLICY_START_COLUMN = "policy_start_date"
POLICY_END_COLUMN = "policy_end_date"
POLICY_NUMBER_COLUMN = "policy_no"
BRANCH_COLUMN="cleaned_new_branch_name"
CORRECTED_CHASSIS_ENGINE_NO="corrected_chassis_no"
CORRECT_INSURANCE_NAME="corrected_name"
VECHICAL_SEGMENT = "vehicle_segment"


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
# Updating meta log
# --------------------------------------------------------------------- 
   
def update_metadata(engine, src_table, target_table, row_count=None, removed_count=None):
    with engine.begin() as conn:
        conn.execute(text(f"""
            UPDATE "{log_schema}"."{META_TABLE}"
            SET 
                is_basepr_appended = 'YES',
                appended_table_name = :target,
                basepr_appended_count = :row_count,
                basepr_removed_count = :removed_count,
                last_updated_ts = :ts
            WHERE table_name = :src
        """), {
            "src": src_table,
            "target": target_table,
            "row_count": row_count if row_count is not None else 0,
            "removed_count": removed_count if removed_count is not None else 0,
            "ts": datetime.utcnow()
        })


# ---------------------------------------------------------------------
# Converting Month Format
# ---------------------------------------------------------------------

def convert_month_format(value):
    """Converts month format to YYYY-MM-DD"""
    try:
        if pd.isna(value):
            return None
        value = str(value).strip()
        if "-" in value:  # Already in date format
            return pd.to_datetime(value).strftime("%Y-%m-%d")
        elif "'" in value:  # Format like Apr'21
            return pd.to_datetime(value, format="%b'%y").strftime("%Y-%m-%d")
        else:  # Format like Apr 22
            return pd.to_datetime(value, format="%b %y").strftime("%Y-%m-%d")
    except Exception:
        return None  # Handle invalid values
    
# ---------------------------------------------------------------------
# Appending and loading data to Aggregation Layer
# ---------------------------------------------------------------------

def append_base_pr_initial():
    try:

        """Appends `base` and `pr` data, identifies common & different columns, and performs cleaning."""
        postgres_hook = PostgresHook(postgres_conn_id="postgres_cloud_prochurn")
        engine = postgres_hook.get_sqlalchemy_engine()

        # ✅ Ensure Target Schema Exists
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))

        fernet = get_fernet()
        sensitive_cols = load_sensitive_columns()
        logger.info("🔐 Fernet initialized & sensitive columns loaded")

        # ✅ Extract Data from `base` and `pr`
        query_base = f'SELECT * FROM "{SOURCE_SCHEMA}"."{BASE_TABLE}"'
        query_pr = f'SELECT * FROM "{SOURCE_SCHEMA}"."{PR_TABLE}"'
        
        df_base = pd.read_sql(query_base, engine)
        df_pr = pd.read_sql(query_pr, engine)

        # 🔓 Decrypt sensitive columns (base)
        for col in df_base.columns:
            if col in sensitive_cols:
                df_base[col] = df_base[col].apply(lambda x: decrypt_value(x, fernet))
        logger.info("🔓 Base table decrypted")

        # 🔓 Decrypt sensitive columns (pr)
        for col in df_pr.columns:
            if col in sensitive_cols:
                df_pr[col] = df_pr[col].apply(lambda x: decrypt_value(x, fernet))
        logger.info("🔓 PR table decrypted")
    
        
        logger.info(f"📂 Extracted {len(df_base)} records from `{SOURCE_SCHEMA}.{BASE_TABLE}`.")
        logger.info(f"📂 Extracted {len(df_pr)} records from `{SOURCE_SCHEMA}.{PR_TABLE}`.")

        # ✅ Identify Common & Different Columns
        # base_columns = set(df_base.columns)
        # pr_columns = set(df_pr.columns)

        # # ✅ Ensure all `base` columns exist in `pr`
        # missing_in_pr = base_columns - pr_columns  # Columns present in `base` but missing in `pr`

        # if missing_in_pr:
        #     raise ValueError(f"❌ ERROR: The following columns from `base` are missing in `pr`: {missing_in_pr}. Process aborted!")

        # logger.info(f"✅ All required columns from `base` are present in `pr`.")
        # logger.info(f"🔍 Found {len(base_columns)} matching columns between `base` and `pr`.")
        if INSURED_NAME_COLUMN not in df_pr.columns:
            df_pr[INSURED_NAME_COLUMN] = None

        if VECHICAL_SEGMENT not in df_pr.columns:
            df_pr[VECHICAL_SEGMENT] = None

        common_columns = set(df_base.columns).intersection(set(df_pr.columns))
        logger.info(f"🔍 Found {len(common_columns)} common columns between base and pr.")
        # original_columns = set(df_pr.columns)
        # mapped_columns = set(common_columns.keys()).intersection(original_columns)
        # updated_columns = {col: common_columns[col] for col in mapped_columns}

        # logger.info(f"🔄 common_columns: {len(mapped_columns)} .")
        # for old_col, new_col in updated_columns.items():
        #     logger.info(f"   🔹 `{old_col}` → `{new_col}`")

        # ✅ Ensure `file_source` column exists
        if "file_source" not in df_base.columns:
            df_base["file_source"] = BASE_TABLE

        if "file_source" not in df_pr.columns:
            df_pr["file_source"] = PR_TABLE

        # ✅ Ensure `booked` column exists in both tables before appending
        if "booked" not in df_base.columns:
            df_base["booked"] = None  # Or ""

        if "booked" not in df_pr.columns:
            df_pr["booked"] = None  # Or ""
        
        # ✅ Ensure `tie_up` column exists in both tables before appending
        if "tie_up" not in df_base.columns:
            df_base["tie_up"] = None  # Or ""

        if "tie_up" not in df_pr.columns:
            df_pr["tie_up"] = None  # Or ""

        # ✅ Ensure `tie_up` column exists in both tables before appending
        if "zone" not in df_base.columns:
            df_base["zone"] = None  # Or ""

        if "zone" not in df_pr.columns:
            df_pr["zone"] = None
        
        if "vehicle_segment" not in df_base.columns:
            df_base["vehicle_segment"] = None  # Or ""

        if "vehicle_segment" not in df_pr.columns:
            df_pr["vehicle_segment"] = None

        # ✅ Append `base` and `pr` Data
        df = pd.concat([df_base[list(common_columns) + ["file_source", "booked", "tie_up", "zone"]],
                        df_pr[list(common_columns) + ["file_source", "booked", "tie_up", "zone"]]], ignore_index=True)
        logger.info(f"📌 Appended `base` and `pr` data. Total records: {len(df)}")

        removed_rows_all = pd.DataFrame()
        
        removed_nop = df[df[CHASSIS_COLUMN] ==''].copy() # Capture rows to be removed
        removed_nop["removal_reason"] = "chassis no is blank"
        removed_rows_all = pd.concat([removed_rows_all, removed_nop], ignore_index=True)  
        df = df[df[CHASSIS_COLUMN] != '']  # Keep only rows where `nop` is 1

        logger.info(f"📌 Removed {len(removed_nop)} rows due to blank chassis.")

        # ✅ Step 2: Ensure Chassis & Engine Columns are Strings
        df[CHASSIS_COLUMN] = df[CHASSIS_COLUMN].astype(str).fillna("")
        df[ENGINE_COLUMN] = df[ENGINE_COLUMN].astype(str).fillna("")

        # ✅ Step 3: Create Lookup Tables for Chassis & Engine Numbers
        logger.info("🔍 Creating lookup dictionaries for faster updates...")

        model_lookup = (
            df[df[REG_NO_COLUMN] != "new"]  # Only consider records where `veh_reg_no` is NOT "new"
            .groupby([REG_NO_COLUMN])[MODEL_COLUMN]
            .apply(lambda x: max(x.dropna(), key=len) if x.dropna().any() else "")  # Get longest chassis number per group
            .to_dict()
        )
        def update_model(row):
            if row[REG_NO_COLUMN] == "new":
                return row[MODEL_COLUMN]

            return model_lookup.get(
                row[REG_NO_COLUMN],
                row[MODEL_COLUMN]
            )
        df[MODEL_COLUMN] = df.apply(update_model, axis=1)
        
        chassis_lookup = (
            df[df[REG_NO_COLUMN] != "new"]
            .groupby([REG_NO_COLUMN, MODEL_COLUMN])[CHASSIS_COLUMN]
            .apply(lambda x: max(x.dropna(), key=len) if x.dropna().any() else "")
            .to_dict()
        )

        engine_lookup = (
            df[df[REG_NO_COLUMN] != "new"]
            .groupby([REG_NO_COLUMN, MODEL_COLUMN])[ENGINE_COLUMN]
            .apply(lambda x: max(x.dropna(), key=len) if x.dropna().any() else "")
            .to_dict()
        )

        # ✅ Step 4: Efficiently Update Chassis & Engine Numbers & model

        def update_chassis(row):
            """Update chassis number only if `veh_reg_no` is NOT 'new'."""
            if row[REG_NO_COLUMN] == "new":
                return row[CHASSIS_COLUMN]  # Keep as-is
            return chassis_lookup.get((row[REG_NO_COLUMN], row[MODEL_COLUMN]), row[CHASSIS_COLUMN])

        def update_engine(row):
            """Update engine number only if `veh_reg_no` is NOT 'new'."""
            if row[REG_NO_COLUMN] == "new":
                return row[ENGINE_COLUMN]  # Keep as-is
            return engine_lookup.get((row[REG_NO_COLUMN], row[MODEL_COLUMN]), row[ENGINE_COLUMN])

        df[CHASSIS_COLUMN] = df.apply(update_chassis, axis=1)
        df[ENGINE_COLUMN] = df.apply(update_engine, axis=1)

        df["cleaned_chassis_engine_no"] = df[CHASSIS_COLUMN].astype(str) + "_" + df[ENGINE_COLUMN].astype(str)

        logger.info("✅ Chassis & Engine numbers updated successfully (excluding 'new' vehicles).")

        # ✅ Convert Month Column
        df["formatted_month"] = df[MONTH_COLUMN].apply(convert_month_format)

        df["cleaned_chassis_engine_no"] = df[CHASSIS_COLUMN].astype(str) + "_" + df[ENGINE_COLUMN].astype(str)

        # ✅ Generate Policy & Chassis Keys
        df["policy_key"] = df[POLICY_NUMBER_COLUMN].astype(str) + "_" + df[POLICY_START_COLUMN].astype(str) + "_" + df[POLICY_END_COLUMN].astype(str)
        #df["chassis_key"] = df[CHASSIS_COLUMN].astype(str) + "_" + df[ENGINE_COLUMN].astype(str) + "_" + df[POLICY_START_COLUMN].astype(str) + "_" + df[POLICY_END_COLUMN].astype(str)
        
        # fill prev or next name  value for insured name null values
        df["cleaned_insured_name"] = df.groupby("cleaned_chassis_engine_no")[INSURED_NAME_COLUMN].transform(lambda x: x.ffill().bfill())


        # ✅ **Step: Fuzzy Matching for Insured Names**
        prev_name = None
        prev_chassis = None
        corrected_names = []
        similarity_scores = []

        logger.info("📝 Correcting insured names using fuzzy matching...")

        for index, row in df.iterrows():
            current_name = row[INSURED_NAME_COLUMN]
            chassis_engine_key = row["cleaned_chassis_engine_no"]

            if prev_name and prev_chassis == chassis_engine_key:
                similarity = fuzz.ratio(prev_name, current_name)
                corrected_names.append(prev_name if similarity >= 70 else current_name)
            else:
                corrected_names.append(current_name)

            similarity_scores.append(fuzz.ratio(corrected_names[-1], current_name))
            prev_name = corrected_names[-1]
            prev_chassis = chassis_engine_key

        df["corrected_name"] = corrected_names
        df["name_similarity"] = similarity_scores

        logger.info("✅ Name correction process completed.")
        # ✅ Initialize Previous Values
        prev_chassis = None
        prev_name = None

        corrected_chassis_numbers = []
        similarity_scores = []

        logger.info("🔍 Correcting chassis numbers using fuzzy logic...")
        df = df.sort_values(
            by=["cleaned_chassis_engine_no",CORRECT_INSURANCE_NAME, POLICY_START_COLUMN, POLICY_END_COLUMN], 
            ascending=[True, True, True, True]
        )
        # ✅ Iterate Over Rows Sequentially
        for index, row in df.iterrows():
            current_name = row["corrected_name"]
            chassis_engine_key = row["cleaned_chassis_engine_no"]

            if prev_name and prev_name == current_name:
                similarity = fuzz.ratio(prev_chassis, chassis_engine_key)

                if similarity >= 80:  # ✅ If similarity is 80% or more, replace with previous chassis number
                    corrected_chassis_numbers.append(prev_chassis)
                else:
                    corrected_chassis_numbers.append(chassis_engine_key)
            else:
                corrected_chassis_numbers.append(chassis_engine_key)  # ✅ First record for this name keeps its chassis

            similarity_scores.append(fuzz.ratio(corrected_chassis_numbers[-1], chassis_engine_key))

            # ✅ Update Previous Values
            prev_name = current_name
            prev_chassis = corrected_chassis_numbers[-1]

        # ✅ Add Corrected Columns to DataFrame
        df["corrected_chassis_no"] = corrected_chassis_numbers
        df["chassis_similarity"] = similarity_scores

        logger.info("✅ Chassis number correction process completed.")

        df["chassis_key"] = df[CORRECTED_CHASSIS_ENGINE_NO].astype(str) + "_" + df[POLICY_START_COLUMN].astype(str) + "_" + df[POLICY_END_COLUMN].astype(str)

        # ✅ **Step: Order Data Before Processing**
        df = df.sort_values(by=["chassis_key", POLICY_START_COLUMN], ascending=[True, True])

        # ✅ Remove Duplicates by `chassis_key`, keeping latest
        before_dedup = len(df)
        duplicate_chassis = df[df.duplicated(subset=["chassis_key"], keep="first")]
        duplicate_chassis = duplicate_chassis.copy()
        duplicate_chassis["removal_reason"] = "Duplicate chassis_key (kept latest)"
        # 🔧 Ensure unique columns before concat
        removed_rows_all = removed_rows_all.loc[:, ~removed_rows_all.columns.duplicated()]
        duplicate_chassis = duplicate_chassis.loc[:, ~duplicate_chassis.columns.duplicated()]

        removed_rows_all = pd.concat([removed_rows_all, duplicate_chassis], ignore_index=True)
        df = df.drop_duplicates(subset=["chassis_key"], keep="first")
        removed_chassis_count = before_dedup - len(df)
        logger.info(f"📊 Removed {removed_chassis_count} : {len(removed_rows_all)} duplicate chassis, keeping latest month.")


        # ✅ Remove Duplicates by `policy_key`, keeping latest
        before_dedup = len(df)
        duplicate_policies = df[df.duplicated(subset=["policy_key"], keep="first")]
        duplicate_policies = duplicate_policies.copy()
        duplicate_policies["removal_reason"] = "Duplicate policy_key (kept latest)"
        # 🔧 ENSURE UNIQUE COLUMNS (CRITICAL)
        removed_rows_all = removed_rows_all.loc[:, ~removed_rows_all.columns.duplicated()]
        duplicate_policies = duplicate_policies.loc[:, ~duplicate_policies.columns.duplicated()]
        removed_rows_all = pd.concat([removed_rows_all, duplicate_policies], ignore_index=True)
        df = df.drop_duplicates(subset=["policy_key"], keep="first")
        removed_count = before_dedup - len(df)
        logger.info(f"📊 Removed {removed_count} duplicate policies, keeping latest month.")

        # add identity for audit
        if not removed_rows_all.empty:
            removed_rows_all["source_table"] = f"{TARGET_TABLE}_removedrows_withreason"
            removed_rows_all["pipeline_run_time"] = datetime.utcnow()

            # encrypt sensitive columns in removed rows
            for col in removed_rows_all.columns:
                if col in sensitive_cols:
                    removed_rows_all[col] = removed_rows_all[col].apply(
                        lambda x: encrypt_value(x, fernet)
                    )

            # archive + log (same standard as base / clean_pr)
            archive_and_log_removed_rows(
                engine=engine,
                removed_df=removed_rows_all,
                source_table=TARGET_TABLE,
                log_schema=log_schema,
                archive_schema=ARCHIVE_SCHEMA,
            )

        removed_rows = len(removed_rows_all)

        # 🔐 Re-encrypt sensitive columns before loading final table
        for col in df.columns:
            if col in sensitive_cols:
                df[col] = df[col].apply(lambda x: encrypt_value(x, fernet))

        logger.info(f"🔐 Re-encrypted sensitive columns before loading {TARGET_SCHEMA}.{TARGET_TABLE}")
        # 🔥 TRUNCATE FIRST (exact place)
        with engine.begin() as conn:
            conn.execute(
                text(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."{TARGET_TABLE}"')
            )
        # ✅ Load Cleaned Data into Target Table
        df.to_sql(name=TARGET_TABLE, schema=TARGET_SCHEMA, con=engine, if_exists="append", index=False)
        row_count = len(df)
        logger.info(f"✅ Appended data successfully loaded into `{TARGET_SCHEMA}.{TARGET_TABLE}`.")
        update_metadata(engine, BASE_TABLE,TARGET_TABLE, row_count=row_count,removed_count=removed_rows)

        update_metadata(engine, PR_TABLE,TARGET_TABLE, row_count=row_count,  removed_count=removed_rows)
    except Exception:
        logger.error(
            "❌ Failed during base-pr append process",
            exc_info=True
        )
        raise

# ✅ Define DAG
with DAG(
    dag_id="bap",
    default_args={"owner": "airflow", "start_date": datetime(2024, 2, 10)},
    schedule_interval=None,
    catchup=False
) as dag:

    append_task = PythonOperator(task_id="append_base_pr", python_callable=append_base_pr_initial)
   

    append_task 