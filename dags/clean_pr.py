# ====================================================================
# 📦 Airflow ETL: Azure Blob → PostgreSQL (PEP8 + Flake8 Clean)
# ====================================================================

import pandas as pd
import re
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.types import String
from pathlib import Path
from schema_table_config import get_column_mapping, get_log_tables, get_schema

# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------

DAGS_DIR = Path(__file__).resolve().parent
JSON_PATH = str(DAGS_DIR / "config" / "schema_metadata_config.json")

SOURCE_SCHEMA = get_schema("stage", JSON_PATH)
# SOURCE_TABLE = "2024_pr"  # Change for different PR files
TARGET_SCHEMA = get_schema("agg", JSON_PATH)
LOG_SCHEMA = get_schema("log", JSON_PATH)

# ✅ Column Mapping
COLUMN_JSON = str(DAGS_DIR / "config" / "column_mapping.json")
COLUMN_MAPPING = get_column_mapping("pr", COLUMN_JSON)

# ---------------------------------------------------------------------
# Defining the Columns
# ---------------------------------------------------------------------

NET_PREMIUM_COLUMN = "net_premium"
POLICY_START_COLUMN = "policy_start_date"
POLICY_END_COLUMN = "policy_end_date"
POLICY_ISSUE_COLUMN = "policy_issue_date"
POLICY_NUMBER_COLUMN = "policy_no"
NOP_COLUMN = "nop"
GST_PERCENTAGE = 0.18  # ✅ GST percentage
META_TABLE = get_log_tables("metadata", JSON_PATH)
POSTGRES_CONN_ID = "postgres_cloud_prochurn"


# ---------------------------------------------------------------------
# 🧹 Clean column names
# ---------------------------------------------------------------------

def clean_text(value):
    """Cleans text fields by:
    - Removing extra spaces
    - Removing special characters
    - Converting to lowercase
    - Removing spaces between words (for insured_name)
    """
    if isinstance(value, pd.Series):  # ✅ Ensure it's not a Series
        return value.apply(clean_text)

    if pd.isna(value) or value is None:
        return None  # ✅ Keeps NaN values as is
    value = str(value).strip()
    value = re.sub(r"[^A-Za-z0-9\s]", "", value)  # Remove non-alphanumeric characters
    value = re.sub(r"\s+", " ", value).strip()  # Remove multiple spaces
    value = value.lower()
    value = value.replace(" ", "")

    return value if value != "" else None  # Remove all spaces for insured_name
# ---------------------------------------------------------------------
# 🧹 Clean Policy Numbers
# ---------------------------------------------------------------------
def clean_policy_number(value):
    """Ensures `policy_number` is numeric & removes any leading `'`."""
    if pd.isna(value):
        return None
    value = str(value).strip().lstrip("'")  # Remove leading `'`
    return value if value.isdigit() else None  # Keep valid numbers

# ---------------------------------------------------------------------
# 🧹 Get Pr Table From Metadata Log
# ---------------------------------------------------------------------
def load_pr_table():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        query = f"""SELECT table_name 
        FROM "{LOG_SCHEMA}"."{META_TABLE}"
        WHERE table_name ILIKE 'pr_%'
        AND stage_loaded = 'YES' 
        AND is_pr_cleaned = 'NO'  
        ORDER BY last_updated_ts DESC;
        """
        results = conn.execute(text(query)).fetchall()
        if results:
            return [r[0] for r in results]
        else:
            print(f"⚠️ No PR tables found in {SOURCE_SCHEMA}, skipping base_clean and moving on.")
            return []

# ---------------------------------------------------------------------
# 🧹 Update the Log in Meta Table
# ---------------------------------------------------------------------    
def update_metadata(table_name, step, status="NO", pr_cnt=None, pr_removed_cnt=None, **context):
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        # Convert True/False to YES/NO
        status_val = "YES" if status in [True, "YES", "1"] else "NO"
        
        conn.execute(text(f"""
            INSERT INTO "{LOG_SCHEMA}"."{META_TABLE}"
                (table_name, {step}, pr_cnt, pr_removed_cnt, last_updated_ts)
            VALUES (:table_name, :status, :pr_cnt, :pr_removed_cnt, :ts)
            ON CONFLICT (table_name)
            DO UPDATE SET
                {step} = :status,
                pr_cnt = :pr_cnt,
                pr_removed_cnt = :pr_removed_cnt,
                last_updated_ts = :ts
        """), {
            "table_name": table_name,
            "status": "YES" if status in [True, "YES", "1"] else "NO",
            "pr_cnt": pr_cnt if pr_cnt is not None else 0,
            "pr_removed_cnt": pr_removed_cnt if pr_removed_cnt is not None else 0,
            "ts": datetime.utcnow()
        })


# ---------------------------------------------------------------------
# 🧹clean and load the pr tables into aggregation Layer
# ---------------------------------------------------------------------
def clean_and_load_pr_data():
    """Cleans PR file data, logs removed rows, and writes logs to Airflow."""
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()
    pipeline_run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    print(f"🛠 Starting Data Processing at `{pipeline_run_time}`")
    source_tables = load_pr_table()
    if not source_tables:
        print("🚫 No PR tables pending cleaning.")
        return
    print(f"Found tables to process: {source_tables}")
    # ✅ Ensure schemas exist
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";'))
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LOG_SCHEMA}";'))

    for source_table in source_tables:

        # ✅ Extract data from `SOURCE_TABLE`
        query = f'SELECT * FROM "{SOURCE_SCHEMA}".\"{source_table}\"'
        df = pd.read_sql(query, engine)
        initial_count = len(df)
        print(f"📂 Extracted {initial_count} records from `{SOURCE_SCHEMA}.{source_table}`.")

        # ✅ Apply Column Mapping
        original_columns = set(df.columns)
        df.rename(columns=COLUMN_MAPPING, inplace=True)
        mapped_columns = set(COLUMN_MAPPING.keys()).intersection(original_columns)
        updated_columns = {col: COLUMN_MAPPING[col] for col in mapped_columns}

        print(f"🔄 Column Mapping Applied: {len(mapped_columns)} columns changed.")
        for old_col, new_col in updated_columns.items():
            print(f"   🔹 `{old_col}` → `{new_col}`")

        # 🔍 Handle different variations of branch column
        if "new_branch_name__2" in df.columns:
            df["new_branch_name_2"] = df["new_branch_name__2"]

        elif "office_name" in df.columns or "location" in df.columns:
            df["new_branch_name_2"] = df.get("office_name", pd.Series([None]*len(df))).fillna(
                df.get("location", pd.Series([None]*len(df)))
            )

        # finally clean
        if "new_branch_name_2" in df.columns:
            df["cleaned_new_branch_name"] = df["new_branch_name_2"].astype(str).apply(clean_text)


        columns_to_clean = {
            "chassis_no": "cleaned_chassis_no",
            "engine_no": "cleaned_engine_no",
            "insured_name": "cleaned_insured_name",  # Remove spaces for insured_name
            "veh_reg_no": "cleaned_veh_reg_no",
            "model":"cleaned_model"
        }
        for original_col, cleaned_col in columns_to_clean.items():
            if original_col in df.columns:
                df[cleaned_col] = df[original_col].astype(str).apply(clean_text)

        print("🧼 Cleaned chassis, engine, insured name, vehicle reg no, and branch name.")
        # ---------------------------------------------------------
        # Clean and normalize zone for PR 2023 (your new block)
        # ---------------------------------------------------------
        if "zone" in df.columns:
            df["zone"] = df["zone"].astype(str).str.upper().str.strip()

            df["zone"] = df["zone"].replace(
                {
                    "NORTH ZONE": "NORTH",
                    "SOUTH ZONE": "SOUTH",
                    "WEST ZONE": "WEST",
                    "EAST": "EAST",
                    "CORPORATE OFFICE": None,
                }
            )

            print("✅ PR 2023 → zone cleaned (NORTH/SOUTH/EAST/WEST, Corporate → NULL)")
        else:
            df["zone"] = None
            print("✅ PR 2023 → zone column missing, added as NULL")

        # ✅ Step 2: Concatenate Chassis & Engine Numbers
        if "cleaned_chassis_no" in df.columns and "cleaned_engine_no" in df.columns:
            df["chassis_engine_no"] = df["cleaned_chassis_no"] + "_" + df["cleaned_engine_no"]
            print("🔗 Concatenated chassis_no & engine_no into `concat_chassis_engine`.")


        # ✅ Step 3: Clean & Convert `policy_number`
        removed_invalid_policy = df[df[POLICY_NUMBER_COLUMN].apply(lambda x: clean_policy_number(x) is None)]
        df[POLICY_NUMBER_COLUMN] = df[POLICY_NUMBER_COLUMN].apply(clean_policy_number)
        df = df[df[POLICY_NUMBER_COLUMN].notna()]
        removed_invalid_policy["removal_reason"] = "Invalid policy number"
        print(f"🔍 Removed {len(removed_invalid_policy)} invalid policy numbers.")

        # ✅ Step 4: Filter `Nop=1`
        
        # ✅ Step 4: Filter `Nop=1` (Only if `NOP_COLUMN` exists)
        if NOP_COLUMN in df.columns:
            removed_nop = df[df[NOP_COLUMN] != 1]  # Capture rows to be removed
            df = df[df[NOP_COLUMN] == 1]  # Keep only rows where `nop` is 1
            removed_nop["removal_reason"] = "`nop` != 1"

            print(f"📌 Removed {len(removed_nop)} rows where `nop` != 1.")
        else:
            removed_nop = pd.DataFrame()  # Create an empty DataFrame if column doesn't exist
            print("⚠️ `nop` column not found, skipping `nop` filtering step.")

        # ✅ Step 4: Remove invalid `total_premium`
        df[NET_PREMIUM_COLUMN] = pd.to_numeric(df[NET_PREMIUM_COLUMN], errors="coerce")
        removed_premium_issues = df[df[NET_PREMIUM_COLUMN].isna() | (df[NET_PREMIUM_COLUMN] <= 0)]
        df = df[df[NET_PREMIUM_COLUMN].notna() & (df[NET_PREMIUM_COLUMN] > 0)]
        removed_premium_issues["removal_reason"] = "Invalid total_premium"
        print(f"💰 Removed {len(removed_premium_issues)} rows with invalid `total_premium`.")

        # ✅ Step 5: Remove policies with duration <= 10 months
        df[POLICY_START_COLUMN] = pd.to_datetime(df[POLICY_START_COLUMN], errors="coerce")
        df[POLICY_END_COLUMN] = pd.to_datetime(df[POLICY_END_COLUMN], errors="coerce")
        df["policy_duration_months"] = ((df[POLICY_END_COLUMN] - df[POLICY_START_COLUMN]).dt.days / 30).astype(int)
        removed_short_policies = df[df["policy_duration_months"] <= 10]
        df = df[df["policy_duration_months"] > 10]
        removed_short_policies["removal_reason"] = "Policy duration <= 10 months"
        print(f"📉 Removed {len(removed_short_policies)} policies with duration <= 10 months.")

        # ✅ Step 6: Deduplicate based on `policy_key`
        df["policy_key"] = df[POLICY_START_COLUMN].astype(str) + "_" + df[POLICY_END_COLUMN].astype(str) + "_" + df[POLICY_NUMBER_COLUMN].astype(str)

        removed_duplicate_policies = pd.DataFrame()
        before_dedup = len(df)
        df = df.sort_values(POLICY_ISSUE_COLUMN, ascending=False).drop_duplicates(subset=["policy_key"], keep="first")
        removed_duplicate_policies = df.iloc[before_dedup - len(df):]
        removed_duplicate_policies["removal_reason"] = "Duplicate policy, keeping latest issue date"
        print(f"📊 Removed {len(removed_duplicate_policies)} duplicate policies.")
        
        # ✅ Step 7: Calculate GST & Total Premium Payable
        if NET_PREMIUM_COLUMN in df.columns:
            df[NET_PREMIUM_COLUMN] = pd.to_numeric(df[NET_PREMIUM_COLUMN], errors="coerce")
            df["gst"] = df[NET_PREMIUM_COLUMN] * GST_PERCENTAGE
            df["total_premium_payable"] = df[NET_PREMIUM_COLUMN] + df["gst"]
            print(f"💰 Calculated gst and total_premium_payable.")

        # ✅ Final count
        final_count = len(df)
        print(f"✅ Final record count after cleansing: {final_count} (Removed {initial_count - final_count} total rows).")

        # ✅ Step 7: Log Removed Rows into `log.removed_<SOURCE_TABLE>`
        removed_data = pd.concat([
            removed_invalid_policy,
            removed_nop,
            removed_premium_issues,
            removed_short_policies,
            removed_duplicate_policies
        ])
        
        if not removed_data.empty:
            removed_data["pipeline_run_time"] = pipeline_run_time
            log_table = f"removed_{source_table}"
            removed_data.to_sql(name=log_table, schema=LOG_SCHEMA, con=engine, if_exists="replace", index=False)
            print(f"⚠️ Removed rows logged into `{LOG_SCHEMA}.{log_table}` with timestamp `{pipeline_run_time}`.")
            
        df["last_runned_date"] = datetime.utcnow()
        # ✅ Step 8: Load cleaned data into `bi_dwh`
        target_table = f"{source_table}"
        df.to_sql(name=target_table, schema=TARGET_SCHEMA, con=engine, if_exists="replace", index=False, dtype={POLICY_NUMBER_COLUMN: String})
        row_count = len(df)

        print(f"✅ Data successfully loaded into `{TARGET_SCHEMA}.{target_table}` with {row_count} rows.")
        clean_cnt = len(df)
        removed_cnt = len(removed_data)

        # update metadata with row count
        update_metadata(
            table_name=source_table,
            step="is_pr_cleaned",
            status="YES",
            pr_cnt=clean_cnt,
            pr_removed_cnt=removed_cnt
        )


        print(f"data successfully updated to {META_TABLE}")


# with DAG(
#     dag_id="clean_pr_file_data",
#     default_args={"owner": "airflow", "start_date": datetime(2024, 2, 10)}, 
#     schedule_interval=None, catchup=False) as dag:
#     clean_pr_data = PythonOperator(
#         task_id="clean_pr_file_data", 
#         python_callable=clean_and_load_pr_data
#         )
#     clean_pr_data
