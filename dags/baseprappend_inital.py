# ====================================================================
# 📦 Airflow ETL: Azure Blob → PostgreSQL (PEP8 + Flake8 Clean)
# ====================================================================
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text
from fuzzywuzzy import fuzz
from airflow.models import Variable
import json
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
from schema_table_config import get_schema, get_log_tables
# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
JSON_PATH = str(DAGS_DIR / "config" / "schema_metadata_config.json")
SOURCE_SCHEMA = get_schema("agg",JSON_PATH)
BASE_TABLE = "base_2022"
PR_TABLE = "pr_2022"
TARGET_SCHEMA = get_schema("agg",JSON_PATH)# both source and target has same schema name
TARGET_TABLE = "finalwith_2022_pr"
LOG_TABLE = "removed_duplicate_policies"
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
# Defining the Sensitive Columns
# ---------------------------------------------------------------------
SENSITIVE_COLUMNS = json.loads(Variable.get("sensitive_columns", default_var="[]"))

# ENCRYPTION_KEY = Variable.get("encryption_key")
# FERNET = Fernet(ENCRYPTION_KEY)

# def decrypt_column(value, fernet):
#     try:
#         if pd.isna(value) or not isinstance(value, str) or value.strip() == "":
#             return value
#         return fernet.decrypt(base64.urlsafe_b64decode(value.encode())).decode()
#     except Exception:
#         return value

# # 🔐 Encryption
# def encrypt_column(value, fernet):
#     try:
#         if pd.isna(value) or not isinstance(value, str) or value.strip() == "":
#             return value
#         return base64.urlsafe_b64encode(fernet.encrypt(value.encode())).decode()
#     except Exception:
#         return value

# def decrypt_chunk(df):
#     """Decrypt sensitive columns in dataframe."""
#     for col in SENSITIVE_COLUMNS:
#         if col in df.columns:
#             df[col] = df[col].apply(lambda x: FERNET.decrypt(x.encode()).decode() if pd.notna(x) else x)
#     return df

# def encrypt_chunk(df):
#     """Encrypt sensitive columns in dataframe."""
#     for col in SENSITIVE_COLUMNS:
#         if col in df.columns:
#             df[col] = df[col].apply(lambda x: FERNET.encrypt(str(x).encode()).decode() if pd.notna(x) else x)
#     return df

# @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
# def safe_read_sql(query, engine):
#     """Read SQL with decryption."""
#     df = pd.read_sql(query, engine)     
#     return decrypt_chunk(df)

# @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
# def safe_to_sql(df, *args, **kwargs):
#     """Write SQL with encryption."""
#     if df.empty:
#         print("⚠️ Skipping empty dataframe write.")
#         return
#     df = encrypt_chunk(df)
#     return df.to_sql(*args, **kwargs)

# ---------------------------------------------------------------------
# Create log Table 
# ---------------------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def safe_to_sql_log(df, table_name, schema, engine, if_exists="replace"):
    """
    Safe wrapper for to_sql with error handling and rollback protection.
    """
    if df.empty:
        print(f"⚠️ Skipping empty write to `{schema}.{table_name}`.")
        return

    try:
        with engine.begin() as conn:  # ensures commit/rollback safety
            df.to_sql(name=table_name, schema=schema, con=conn, if_exists=if_exists, index=False)
        print(f"✅ Logged {len(df)} records to `{schema}.{table_name}`.")
    except SQLAlchemyError as e:
        engine.dispose()  # discard all pooled connections
        print(f"❌ Logging failed for `{schema}.{table_name}` due to DB error: {e}")
        raise

    
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
    """Appends `base` and `pr` data, identifies common & different columns, and performs cleaning."""
    postgres_hook = PostgresHook(postgres_conn_id="postgres_cloud_prochurn")
    engine = postgres_hook.get_sqlalchemy_engine()

    # ✅ Ensure Target Schema Exists
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))

    # ✅ Extract Data from `base` and `pr`
    query_base = f'SELECT * FROM "{SOURCE_SCHEMA}"."{BASE_TABLE}"'
    query_pr = f'SELECT * FROM "{SOURCE_SCHEMA}"."{PR_TABLE}"'
    
    df_base = pd.read_sql(query_base, engine)
    df_pr = pd.read_sql(query_pr, engine)

    # for col in SENSITIVE_COLUMNS:
    #             if col in df_base.columns:
    #                 df_base[col] = df_base[col].astype(str).apply(lambda x: decrypt_column(x, FERNET))
    #                 print("🔓 base Decryption done")
    # for col in SENSITIVE_COLUMNS:
    #             if col in df_pr.columns:
    #                 df_pr[col] = df_pr[col].astype(str).apply(lambda x: decrypt_column(x, FERNET))
    #                 print("🔓 PR Decryption done")
    
    
    print(f"📂 Extracted {len(df_base)} records from `{SOURCE_SCHEMA}.{BASE_TABLE}`.")
    print(f"📂 Extracted {len(df_pr)} records from `{SOURCE_SCHEMA}.{PR_TABLE}`.")

    # ✅ Identify Common & Different Columns
    # base_columns = set(df_base.columns)
    # pr_columns = set(df_pr.columns)

    # # ✅ Ensure all `base` columns exist in `pr`
    # missing_in_pr = base_columns - pr_columns  # Columns present in `base` but missing in `pr`

    # if missing_in_pr:
    #     raise ValueError(f"❌ ERROR: The following columns from `base` are missing in `pr`: {missing_in_pr}. Process aborted!")

    # print(f"✅ All required columns from `base` are present in `pr`.")
    # print(f"🔍 Found {len(base_columns)} matching columns between `base` and `pr`.")
    if INSURED_NAME_COLUMN not in df_pr.columns:
        df_pr[INSURED_NAME_COLUMN] = None

    if VECHICAL_SEGMENT not in df_pr.columns:
        df_pr[VECHICAL_SEGMENT] = None

    common_columns = set(df_base.columns).intersection(set(df_pr.columns))
    print(f"🔍 Found {len(common_columns)} common columns between base and pr.")
    # original_columns = set(df_pr.columns)
    # mapped_columns = set(common_columns.keys()).intersection(original_columns)
    # updated_columns = {col: common_columns[col] for col in mapped_columns}

    # print(f"🔄 common_columns: {len(mapped_columns)} .")
    # for old_col, new_col in updated_columns.items():
    #     print(f"   🔹 `{old_col}` → `{new_col}`")

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
    print(f"📌 Appended `base` and `pr` data. Total records: {len(df)}")
    
    removed_nop = df[df[CHASSIS_COLUMN] =='']  # Capture rows to be removed
    df = df[df[CHASSIS_COLUMN] != '']  # Keep only rows where `nop` is 1
    removed_nop["removal_reason"] = "chassis no is blank"

    print(f"📌 Removed {len(removed_nop)} rows where `nop` != 1.")

    # ✅ Step 2: Ensure Chassis & Engine Columns are Strings
    df[CHASSIS_COLUMN] = df[CHASSIS_COLUMN].astype(str).fillna("")
    df[ENGINE_COLUMN] = df[ENGINE_COLUMN].astype(str).fillna("")

    # ✅ Step 3: Create Lookup Tables for Chassis & Engine Numbers
    print("🔍 Creating lookup dictionaries for faster updates...")

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

    print("✅ Chassis & Engine numbers updated successfully (excluding 'new' vehicles).")

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

    print("📝 Correcting insured names using fuzzy matching...")

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

    print("✅ Name correction process completed.")
    # ✅ Initialize Previous Values
    prev_chassis = None
    prev_name = None

    corrected_chassis_numbers = []
    similarity_scores = []

    print("🔍 Correcting chassis numbers using fuzzy logic...")
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

    print("✅ Chassis number correction process completed.")

    df["chassis_key"] = df[CORRECTED_CHASSIS_ENGINE_NO].astype(str) + "_" + df[POLICY_START_COLUMN].astype(str) + "_" + df[POLICY_END_COLUMN].astype(str)

# ✅ **Step: Order Data Before Processing**
    df = df.sort_values(by=["chassis_key", POLICY_START_COLUMN], ascending=[True, True])

# ✅ Remove Duplicates by `chassis_key`, keeping latest
    before_dedup = len(df)
    duplicate_chassis = df[df.duplicated(subset=["chassis_key"], keep="first")]
    df = df.drop_duplicates(subset=["chassis_key"], keep="first")
    removed_chassis_count = before_dedup - len(df)
    print(f"📊 Removed {removed_chassis_count} duplicate chassis, keeping latest month.")


    # ✅ Remove Duplicates by `policy_key`, keeping latest
    before_dedup = len(df)
    duplicate_policies = df[df.duplicated(subset=["policy_key"], keep="first")]
    df = df.drop_duplicates(subset=["policy_key"], keep="first")
    removed_count = before_dedup - len(df)
    print(f"📊 Removed {removed_count} duplicate policies, keeping latest month.")

    # ✅ Log Removed Duplicates
    removed_duplicates = pd.concat([duplicate_policies, duplicate_chassis])
    if not removed_duplicates.empty:
        safe_to_sql_log(removed_duplicates, LOG_TABLE, log_schema, engine)
        print(f"⚠️ Logged {len(removed_duplicates)} removed duplicates into `{log_schema}.{LOG_TABLE}`.")
    removed_rows = len(removed_duplicates)
    
 # ✅ Load Cleaned Data into Target Table
    df.to_sql(name=TARGET_TABLE, schema=TARGET_SCHEMA, con=engine, if_exists="replace", index=False)
    row_count = len(df)
    print(f"✅ Appended data successfully loaded into `{TARGET_SCHEMA}.{TARGET_TABLE}`.")
    update_metadata(engine, BASE_TABLE,TARGET_TABLE, row_count=row_count,removed_count=removed_rows)

    update_metadata(engine, PR_TABLE,TARGET_TABLE, row_count=row_count,  removed_count=removed_rows)
    

# ✅ Define DAG
# with DAG(
#     dag_id="bap",
#     default_args={"owner": "airflow", "start_date": datetime(2024, 2, 10)},
#     schedule_interval=None,
#     catchup=False
# ) as dag:

#     append_task = PythonOperator(task_id="append_base_pr", python_callable=append_base_pr)
   

#     append_task 