import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from sqlalchemy import text
from fuzzywuzzy import fuzz
from sqlalchemy.engine import reflection
from airflow.models import Variable
from cryptography.fernet import Fernet
import json
import base64
import gc
import warnings
import re
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import OperationalError, PendingRollbackError
import time

# ✅ Define Source & Target Tables
SOURCE_SCHEMA = "pip_bi_dwh" # both source and target has same schema name
TARGET_SCHEMA = "pip_bi_dwh" # both source and target has same schema name
TARGET_TABLE = "final_renewed_policies_test"
LOG_TABLE = "removed_duplicate_policies"
log_schema= "pip_log"
META_TABLE = "etl_metadata_logs"
# ✅ Define Column Names
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
def get_latest_source_table(engine):
    query = f"""
        SELECT appended_table_name 
        FROM {log_schema}.{META_TABLE}
        WHERE is_basepr_appended = 'YES'
        ORDER BY last_updated_ts DESC
        LIMIT 1;
    """
    result = engine.execute(text(query)).fetchone()
    return result[0] if result else None


def update_claim_merge_table(engine, target_table,row_count=None):
    query = f"""
        UPDATE {log_schema}.{META_TABLE}
        SET renewal_policy_table = :final_table,
            renewal_policy_count = :cnt
        WHERE appended_table_name = (
            SELECT appended_table_name 
            FROM {log_schema}.{META_TABLE}
            WHERE is_basepr_appended = 'YES'
            ORDER BY last_updated_ts DESC
            LIMIT 1
        );
    """
    with engine.begin() as conn:
        conn.execute(
            text(query),
            {"final_table": target_table, "cnt": row_count}
        )


def fuzzy_matching():
        
 
    postgres_hook = PostgresHook(postgres_conn_id="postgres_cloud_prochurn")
    engine = postgres_hook.get_sqlalchemy_engine()
    SOURCE_TABLE = get_latest_source_table(engine)
    if not SOURCE_TABLE:
        print("no source table found")
        return
    
    query = f"""
        SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} 
        ORDER BY corrected_chassis_no, corrected_name, policy_start_date
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print("⚠️ No records found!")
        return

    print(f"📂 Loaded {len(df)} records from `{SOURCE_SCHEMA}.{SOURCE_TABLE}`.")


    # ✅ Convert Date Columns to Datetime Format
    df[POLICY_START_COLUMN] = pd.to_datetime(df[POLICY_START_COLUMN], errors="coerce")
    df[POLICY_END_COLUMN] = pd.to_datetime(df[POLICY_END_COLUMN], errors="coerce")

    # ✅ Step 1: Generate Customer ID
    df["customer_id_Base"] = df[CORRECT_INSURANCE_NAME].astype(str) + "_" + df[BRANCH_COLUMN].astype(str)
    df["customer_id"] = (df.groupby("customer_id_Base").ngroup() + 1000001).astype(str)

    # Convert dates to datetime
    df["policy_start_date"] = pd.to_datetime(df["policy_start_date"], errors="coerce")
    df["policy_end_date"] = pd.to_datetime(df["policy_end_date"], errors="coerce")

    # Sort DataFrame
    df = df.sort_values(
        by=[CORRECTED_CHASSIS_ENGINE_NO, POLICY_START_COLUMN, POLICY_END_COLUMN,CORRECT_INSURANCE_NAME], 
        ascending=[True, True, True, True]
    )


    # ✅ Shift previous row values for comparison
    df["prev_chassis"] = df[CORRECTED_CHASSIS_ENGINE_NO].shift(1)
    df["prev_name"] = df[CORRECT_INSURANCE_NAME].shift(1)
    df["prev_start"] = df[POLICY_START_COLUMN].shift(1)
    df["prev_end"] = df[POLICY_END_COLUMN].shift(1)
    df["prev_customer_id"] = df["customer_id"].shift(1)

    # ✅ Mask where records need correction
    mask = (
        (df[CORRECTED_CHASSIS_ENGINE_NO] == df["prev_chassis"]) &  # Same chassis
        (df[POLICY_START_COLUMN] == df["prev_start"]) &  # Same start date
        (df[POLICY_END_COLUMN] != df["prev_end"])  # Different end date
    )

    # ✅ Propagate `customer_id` and `corrected_name`
    df.loc[mask, "customer_id"] = df.loc[mask, "prev_customer_id"]
    df.loc[mask, CORRECT_INSURANCE_NAME] = df.loc[mask, "prev_name"]  # Fixed name update

    # ✅ Drop temporary columns
    df.drop(columns=["prev_chassis", "prev_name", "prev_start", "prev_end", "prev_customer_id"], inplace=True)

    print("✅ Customer ID & Name successfully propagated for duplicate policies!")


    # ✅ Step 1: Sort the Data (If Not Sorted Already)
    df = df.sort_values(by=["corrected_chassis_no", "corrected_name"]).reset_index(drop=True)

    # ✅ Step 2: Forward Fill Customer ID Where `corrected_chassis_no` & `corrected_name` Match
    df["customer_id"] = df.groupby(["corrected_chassis_no", "corrected_name"])["customer_id"].transform("first")

    print("✅ Customer ID successfully propagated!")

    # ✅ Sort Data for Correct Processing
    df = df.sort_values(
        by=[CORRECTED_CHASSIS_ENGINE_NO, CORRECT_INSURANCE_NAME, POLICY_START_COLUMN, POLICY_END_COLUMN], 
        ascending=[True, True, True, True]
    )

    # ✅ Initialize Columns
    df["renewed_flag"] = 0
    df["renewed_policy_date"] = None
    df["policy_renew_days_difference"] = None
    df["old_policy_no"] = None
    df["initial_policy_no"] = None

    # ✅ Identify Renewed Policies & Mark Previous One
    for _, group in df.groupby([CORRECTED_CHASSIS_ENGINE_NO, CORRECT_INSURANCE_NAME]):
        previous_index = None
        previous_end_date = None
        initial_policy_no = None

        for index, row in group.iterrows():
            start_date = row[POLICY_START_COLUMN]
            end_date = row[POLICY_END_COLUMN]

            # ✅ Check if policy is still active (end date in the future)
            today = pd.Timestamp.today()
            if end_date > today:
                df.at[index, "renewed_flag"] = 2  # ✅ Mark as "Open Renewal"
            else:
                df.at[index, "renewed_flag"] = 0  # ✅ Default: Not Renewed

            # ✅ If there's a previous policy, check renewal condition
            if previous_end_date is not None:
                days_difference = (start_date - previous_end_date).days
                
                if 0 <= days_difference <= 60:  # ✅ Renewed within 5 days
                    df.at[previous_index, "renewed_flag"] = 1  # ✅ Mark previous policy as renewed
                    df.at[previous_index, "renewed_policy_date"] = start_date
                    df.at[previous_index, "policy_renew_days_difference"] = days_difference

                    # ✅ Assign old policy number
                    df.at[index, "old_policy_no"] = df.at[previous_index, POLICY_NUMBER_COLUMN]

                    # ✅ Assign initial policy number (first policy in renewal chain)
                    if initial_policy_no is None:
                        initial_policy_no = df.at[previous_index, POLICY_NUMBER_COLUMN]
                    df.at[index, "initial_policy_no"] = initial_policy_no
                else:
                    df.at[index, "initial_policy_no"] = row[POLICY_NUMBER_COLUMN]

            # ✅ Update previous policy details for next iteration
            previous_index = index
            previous_end_date = end_date

            # ✅ Ensure the first policy in a chain has its own initial policy number
            if initial_policy_no is None:
                df.at[index, "initial_policy_no"] = row[POLICY_NUMBER_COLUMN]

    # ✅ Remove values for policies marked as "Open" (renewed_flag = 2)
    df.loc[df["renewed_flag"] == 2, ["renewed_policy_date", "policy_renew_days_difference"]] = None

    print("✅ Policy renewal identification completed.")

    # ✅ Step 3: Calculate policy_tenure
    df["policy_tenure_month"] = ((df[POLICY_END_COLUMN].dt.year - df[POLICY_START_COLUMN].dt.year) * 12 +
                                (df[POLICY_END_COLUMN].dt.month - df[POLICY_START_COLUMN].dt.month))

    df["policy_tenure"] = (df["policy_tenure_month"] / 12).round(0)

    # ✅ Step 4: Extract start_year & end_year
    df["start_year"] = df[POLICY_START_COLUMN].dt.year
    df["end_year"] = df[POLICY_END_COLUMN].dt.year

    # ✅ Step 5: Calculate Yearly & Cumulative Tenure
    yearly_tenure = (
        df.groupby(["customer_id", "start_year"])
        .agg({POLICY_START_COLUMN: "min", POLICY_END_COLUMN: "max"})
        .reset_index()
    )

    yearly_tenure["yearly_tenure_months"] = (
        (yearly_tenure[POLICY_END_COLUMN].dt.year - yearly_tenure[POLICY_START_COLUMN].dt.year) * 12 +
        (yearly_tenure[POLICY_END_COLUMN].dt.month - yearly_tenure[POLICY_START_COLUMN].dt.month)
    )

    yearly_tenure["cumulative_tenure_months"] = (
        yearly_tenure.groupby("customer_id")["yearly_tenure_months"]
        .cumsum()
    )

    yearly_tenure["tenure_decimal"] = yearly_tenure["cumulative_tenure_months"] / 12
    yearly_tenure["customer_tenure"] = yearly_tenure["tenure_decimal"].round(0)

    df = df.drop(columns=["cumulative_tenure_months", "customer_tenure", "tenure_decimal"], errors="ignore")

    # ✅ Step 6: Merge Tenure Data Back to Main DataFrame
    tenure_mapping = yearly_tenure[["customer_id", "start_year", "cumulative_tenure_months", "tenure_decimal", "customer_tenure"]]
    df = df.merge(tenure_mapping, on=["customer_id", "start_year"], how="left")

    # ✅ Step 7: Identify New Customers
    df["firstyearpolicy"] = df.groupby("customer_id")["start_year"].transform("min")
    df["new_customer"] = df.apply(
        lambda row: f"{row['firstyearpolicy']}_{row['customer_id']}" if row["start_year"] == row["firstyearpolicy"] else "",
        axis=1
    )
    df["New Customers"] = df["new_customer"].apply(lambda x: "Yes" if x else "No")

    print("✅ customer_tenure & renewal status calculated.")


    try:
        # Dispose the old possibly broken connection pool
        engine.dispose()
        time.sleep(2)

        # Recreate a fresh engine
        hook = PostgresHook(postgres_conn_id="postgres_cloud_prochurn")
        fresh_engine = hook.get_sqlalchemy_engine()

        with fresh_engine.begin() as conn:
            df.to_sql(
                name=TARGET_TABLE,
                schema=TARGET_SCHEMA,
                con=conn,
                if_exists="replace",
                index=False
            )

        print(f"✅ Data successfully loaded into `{TARGET_SCHEMA}.{TARGET_TABLE}`.")
        row_cnt = len(df)

        update_claim_merge_table(fresh_engine, TARGET_TABLE, row_count=row_cnt)
        print("✅ Metadata updated with claim merge table name.")

    except (PendingRollbackError, OperationalError) as e:
        print(f"❌ Transaction issue encountered: {e}. Retrying after rollback...")
        fresh_engine.dispose()
        time.sleep(5)
        # retry once with new connection
        retry_engine = hook.get_sqlalchemy_engine()
        with retry_engine.begin() as conn:
            df.to_sql(
                name=TARGET_TABLE,
                schema=TARGET_SCHEMA,
                con=conn,
                if_exists="replace",
                index=False
            )
        print("✅ Retry successful.")
    finally:
        # Always close connections
        fresh_engine.dispose()
        gc.collect()

# ✅ Define DAG
# with DAG(
#     dag_id="fuzzy_match",
#     default_args={"owner": "airflow", "start_date": datetime(2024, 2, 10)},
#     schedule_interval=None,
#     catchup=False
# ) as dag:

#     fuzzy = PythonOperator(task_id="fuzzy_match", python_callable=fuzzy_matching)

#     fuzzy