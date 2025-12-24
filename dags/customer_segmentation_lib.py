from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import pandas as pd

from config.crypto_utils import get_fernet, encrypt_value, decrypt_value
from config.config_loader import load_sensitive_columns
from schema_table_config import get_log_tables, get_schema


# ----------------------------------------------------------
# Source / Target Tables
# ----------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")

SOURCE_TABLE_1 = "future_predition_with_top3_reason"
TARGET_TABLE = "customer_segmentation_on_future_prediction"

SOURCE_SCHEMA = get_schema("da/ml", META_JSON)
TARGET_SCHEMA = get_schema("da/ml", META_JSON)

LOG_SCHEMA = get_schema("log", META_JSON)
FEATURE_ENG_LOG = get_log_tables("featurelog", META_JSON)

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
OUTER_CHUNK = 100000
INNER_CHUNK = 50000


# ----------------------------------------------------------
# Update Metadata
# ----------------------------------------------------------
def update_segment_metadata(engine):
    """
    Updates feature_eng_log after customer segmentation.
    """
    # Count rows from final segmentation table
    after_cnt = pd.read_sql(
        text(
            f'''SELECT COUNT(*) AS cnt 
                 FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"'''
        ),
        con=engine,
    )["cnt"][0]

    sql = f"""
        UPDATE {LOG_SCHEMA}.{FEATURE_ENG_LOG}
        SET
            segmentation = '{TARGET_TABLE}',
            segmentation_count = {after_cnt}
        WHERE date = (
            SELECT date
            FROM {LOG_SCHEMA}.{FEATURE_ENG_LOG}
            ORDER BY date DESC
            LIMIT 1
        );
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(f"✅ Segment metadata updated — rows={after_cnt}")


# ----------------------------------------------------------
# Chunk Load
# ----------------------------------------------------------
def load_chunked(df, table_name, schema):
    total_rows = len(df)
    print(
        f"\n🚀 Loading → {schema}.{table_name} ({total_rows} rows)"
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


# ----------------------------------------------------------
# CUSTOMER SEGMENTATION GENERATION
# ----------------------------------------------------------
def cus_segmentation():
    print("🔗 Connecting to PostgreSQL...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    fernet = get_fernet()
    sensitive_cols = load_sensitive_columns()
    print("🔐 Fernet initialized & sensitive columns loaded")
    # ---------------------------
    # STEP 1: Load Prediction Table
    # ---------------------------
    query_hist = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE_1};"
    data = pd.read_sql(query_hist, con=engine)
    # 🔓 Decrypt sensitive columns before cleaning
    for col in data.columns:
        if col in sensitive_cols:
            data[col] = data[col].apply(
                    lambda x: decrypt_value(x, fernet)
            )

    print(f"🔓 Decrypted sensitive columns for {SOURCE_TABLE_1}")    
    print(f"📌 Loaded TOP 3 REASON FILE: {len(data)} rows")

    high_discount_threshold = data[
        "applicable_discount_with_ncb"
    ].quantile(0.75)
    mid_discount_threshold = data[
        "applicable_discount_with_ncb"
    ].quantile(0.5)
    low_discount_threshold = data[
        "applicable_discount_with_ncb"
    ].quantile(0.25)

    high_clv_threshold = data["clv"].quantile(0.75)
    mid_clv_threshold = data["clv"].quantile(0.5)
    low_clv_threshold = data["clv"].quantile(0.25)

    high_churn_probability_threshold = 0.80
    mid_churn_probability_threshold = 0.60
    low_churn_probability_threshold = 0.50

    # Assign CLV, Payment, Discount, and Churn categories
    data["clv_category"] = data["clv"].apply(
        lambda x: "High"
        if x > high_clv_threshold
        else ("Mid" if x > mid_clv_threshold else "Low")
    )
    data["discount_category"] = data[
        "applicable_discount_with_ncb"
    ].apply(
        lambda x: "High"
        if x > high_discount_threshold
        else ("Mid" if x > mid_discount_threshold else "Low")
    )
    data["churn_category"] = data["churn_probability"].apply(
        lambda x: "High"
        if x > high_churn_probability_threshold
        else ("Mid" if x > mid_churn_probability_threshold else "Low")
    )

    # Function to segment customers based on the new criteria
    def segment_policy(row):
        if row["predicted_status"] == "Not Renewed":
            if (
                row["churn_category"] == "Mid"
                and row["discount_category"] in ["Mid", "Low"]
                and row["clv_category"] in ["High", "Mid"]
            ):
                return "Platinum"
            if (
                row["churn_category"] == "Low"
                and row["discount_category"] in ["Mid", "Low"]
                and row["clv_category"] in ["High", "Mid"]
            ):
                return "Gold"
            if (
                row["churn_category"] == "Mid"
                and row["discount_category"]
                in ["High", "Mid", "Low"]
                and row["clv_category"]
                in ["High", "Mid", "Low"]
            ):
                return "Gold"
            if (
                row["churn_category"] == "Low"
                and row["discount_category"]
                in ["High", "Mid", "Low"]
                and row["clv_category"]
                in ["High", "Mid", "Low"]
            ):
                return "Gold"
            if (
                row["churn_category"] == "High"
                and row["discount_category"]
                in ["High", "Mid", "Low"]
                and row["clv_category"]
                in ["High", "Mid", "Low"]
            ):
                return "Sliver"
        return None

    # Apply the segmentation function
    data["Customer Segment"] = data.apply(segment_policy, axis=1)

    # ---------------------------
    # STEP 8: Save Final Output Back to Postgres
    # ---------------------------
    data.columns = (
        data.columns.str.strip().str.lower().str.replace(" ", "_")
    )
    print("normalization for columns applied")
    # 🔐 Re-encrypt sensitive columns before loading
    for col in data.columns:
        if col in sensitive_cols:
            data[col] = data[col].apply(lambda x: encrypt_value(x, fernet))

    print(f"🔐 Re-encrypted sensitive columns before loading {TARGET_SCHEMA}.{TARGET_TABLE}")
    load_chunked(data, TARGET_TABLE, TARGET_SCHEMA)

    print("🎉 FINAL OUTPUT SAVED SUCCESSFULLY")
    # ⭐ Update segmentation metadata
    update_segment_metadata(engine)

    # Print threshold values
    print("Threshold Values:")
    print(
        f"applicable_discount_with_ncb - High: "
        f"{high_discount_threshold}, Mid: {mid_discount_threshold}, "
        f"Low: {low_discount_threshold}"
    )
    print(
        f"CLV - High: {high_clv_threshold}, Mid: "
        f"{mid_clv_threshold}, Low: {low_clv_threshold}"
    )
    print(
        f"churn_probability - High: {high_churn_probability_threshold}, "
        f"Mid: {mid_churn_probability_threshold}, Low: "
        f"{low_churn_probability_threshold}"
    )


# ----------------------------------------------------------
# DAG
# ----------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="segment_customer_basedon_top_3_reason",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["segmentation", "liberty"],
) as dag:

    segmentation_task = PythonOperator(
        task_id="customer_segmenatation",
        python_callable=cus_segmentation,
    )

    segmentation_task
