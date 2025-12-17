from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import pandas as pd

from schema_table_config import get_log_tables, get_schema


# --------------------------------------------------------------------
# Source / Target Tables
# --------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")

SOURCE_TABLE_1 = "future_prediction"
SOURCE_TABLE_2 = "final_policy_features"

TARGET_TABLE_1 = "future_prediction_with_notrenewal_reason"
TARGET_TABLE_2 = "final_policy_features_with_notrenewed_reason_only"

SOURCE_SCHEMA = get_schema("bi_dwh", META_JSON)
TARGET_SCHEMA = get_schema("da/ml", META_JSON)

LOG_SCHEMA = get_schema("log", META_JSON)
FEATURE_ENG_LOG = get_log_tables("featurelog", META_JSON)

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
OUTER_CHUNK = 100000
INNER_CHUNK = 50000


# --------------------------------------------------------------------
# Update Metadata
# --------------------------------------------------------------------
def update_reason_metadata(engine, count_rows):
    """
    Update metadata for reason generation step.
    """
    sql = f"""
        UPDATE {LOG_SCHEMA}.{FEATURE_ENG_LOG}
        SET
            reason_done = 'YES',
            removal_reason_cnt = {count_rows},
            timestamp = NOW()
        WHERE last_run_date = (
            SELECT last_run_date
            FROM {LOG_SCHEMA}.{FEATURE_ENG_LOG}
            WHERE reason_done = 'NO'
            ORDER BY timestamp DESC
            LIMIT 1
        );
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(
        f"✅ Updated metadata: reason_done=YES, "
        f"removal_reason_cnt={count_rows}"
    )


# --------------------------------------------------------------------
# Load Data To Postgres (chunked)
# --------------------------------------------------------------------
def load_chunked(df, table_name, schema):
    total_rows = len(df)
    print(
        f"\n🚀 Loading → {schema}.{table_name} "
        f"({total_rows} rows)"
    )

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    first = True

    for start in range(0, total_rows, OUTER_CHUNK):
        end = min(start + OUTER_CHUNK, total_rows)
        sub_df = df.iloc[start:end]

        mode = "replace" if first else "append"

        with engine.begin() as conn:
            sub_df.to_sql(
                name=table_name,
                schema=schema,
                con=conn,
                if_exists=mode,
                index=False,
                chunksize=INNER_CHUNK,
                method="multi",
            )

        first = False
        print(f"Loaded rows {start} → {end}")

    print(f"✔ Load COMPLETE → {schema}.{table_name}\n")


# --------------------------------------------------------------------
# Reason logic for churn (row-wise)
# --------------------------------------------------------------------
def reason_for_churn(row, status_col):
    if row[status_col] == "Not Renewed":
        reasons = []

        # vehicle_age
        if row["vehicle_age"] <= 1.0038:
            reasons.append("Young Vehicle Age")
        if row["vehicle_age"] > 6.4663:
            reasons.append("Old Vehicle Age")

        # Vehicle IDV
        if row["vehicle_idv"] <= 405003.0:
            reasons.append("Low Vehicle IDV")

        # Add-on Premium
        if row["before_gst_add_on_gwp"] > 6944.5:
            reasons.append("High Add-On Premium")

        # Own-Damage Premium
        if row["total_od_premium"] > 7790.5:
            reasons.append("High Own-Damage Premium")

        # Third-Party Premium
        if row["total_tp_premium"] > 8268.5:
            reasons.append("High Third-Party Premium")

        # NCB Percentage
        if row["previous_year_ncb_percentage"] <= 10.0:
            reasons.append("Low No Claim Bonus Percentage")

        # Discount with NCB
        if row["applicable_discount_with_ncb"] <= 64.5:
            reasons.append("Low Discount with NCB")

        # number_of_claims
        if row["number_of_claims"] > 1.5:
            reasons.append("Multiple Claims on Record")

        # Claim Happened
        if row.get("claim_happened_not") == "Yes":
            reasons.append("Claims Happened")

        # Policy-wise Purchase
        if row["policy_wise_purchase"] <= 2.5:
            reasons.append("Minimal Policies Purchased")

        # policy_tenure
        if row["policy_tenure"] > 2.0:
            reasons.append("Policy Tenure Exceeds 2 Years")

        # tie_ups (grouped)
        if row.get("tie_up") in [
            "MARUTI",
            "MIBL OEM",
            "HYUNDAI",
            "Non-OEM",
        ]:
            reasons.append(f"Tie Up with {row['tie_up']}")

        return ", ".join(reasons) if reasons else "Organic Churn"

    return ""


# --------------------------------------------------------------------
# Apply reason generation on predicted data
# --------------------------------------------------------------------
def call_pred_data_def():
    print("🔗 Connecting to PostgreSQL...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # ---------------------------
    # Step 1: Load Data
    # ---------------------------
    query = (
        f"SELECT * FROM {TARGET_SCHEMA}.{SOURCE_TABLE_1};"
    )
    df = pd.read_sql(query, con=engine)
    print(f"✅ Loaded {len(df)} rows from {SOURCE_TABLE_1}")

    # Apply the function
    df["Not Renewed Reasons"] = df.apply(
        lambda r: reason_for_churn(r, "predicted_status"),
        axis=1,
    )
    print("functions applied successfully")

    # Normalize column names before writing to DB
    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_")
    )
    print("📝 Normalized column names in df")

    load_chunked(df, TARGET_TABLE_1, TARGET_SCHEMA)

    print(
        f"loaded data successfully into db with total records of: "
        f"{len(df)} in {TARGET_TABLE_1}"
    )


# --------------------------------------------------------------------
# Apply reason generation on historic data
# --------------------------------------------------------------------
def call_historic_data_def():
    print("🔗 Connecting to PostgreSQL...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # ---------------------------
    # Step 1: Load Data
    # ---------------------------
    query = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE_2} WHERE policy_status = 'Not Renewed';"
    df = pd.read_sql(query, con=engine)

    print(f"✅ Loaded {len(df)} rows from {SOURCE_TABLE_2}")

    # Step 2: Ensure numeric columns are numeric
    numeric_columns = [
        "before_gst_add_on_gwp",
        "total_od_premium",
        "total_tp_premium",
        "total_premium_payable",
        "previous_year_ncb_percentage",
        "applicable_discount_with_ncb",
        "vehicle_age",
        "vehicle_idv",
    ]

    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Apply the function
    df["Not Renewed Reasons"] = df.apply(
        lambda r: reason_for_churn(r, "policy_status"),
        axis=1,
    )

    print("functions applied successfully")

    # Normalize column names before writing to DB
    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_")
    )
    load_chunked(df, TARGET_TABLE_2, TARGET_SCHEMA)

    print(
        f"loaded data successfully into db with total records of: "
        f"{len(df)} in {TARGET_TABLE_2}"
    )

    # # ⭐ Update metadata for prediction reason table
    # update_reason_metadata(engine, len(df))


# # --------------------------------------------------------------------
# # DAG
# # --------------------------------------------------------------------
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2024, 11, 1),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=3),
# }

# with DAG(
#     dag_id="not_renewed_reason_generator",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["reasons", "churn", "liberty"],
# ) as dag:

#     task_model_prediction = PythonOperator(
#         task_id="reason_for_prediction_table",
#         python_callable=call_pred_data_def,
#     )

#     task_policy_status = PythonOperator(
#         task_id="reason_for_policy_status_table",
#         python_callable=call_historic_data_def,
#     )

#     [task_model_prediction, task_policy_status]
