from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

# ----------------------------------------------------------
# Source / Target Tables
# ----------------------------------------------------------
SOURCE_TABLE_1 = "gbm1_predictions_jfmamj_final_top3_reasons"

TARGET_TABLE = "GBM1_predictions_jasond_final_reason_segment"

SOURCE_SCHEMA = "test_bi_dwh"
TARGET_SCHEMA = "test_bi_dwh"

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
OUTER_CHUNK = 30000
INNER_CHUNK = 15000

# ----------------------------------------------------------
# Chunk Load
# ----------------------------------------------------------
def load_chunked(df, table_name, schema):
    total_rows = len(df)
    print(f"\n🚀 Loading → {schema}.{table_name} ({total_rows} rows)")

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
                method="multi"
            )

        first = False
        print(f"Loaded rows {start} → {end}")

    print(f"✔ Load COMPLETE → {schema}.{table_name}\n")

# ----------------------------------------------------------
# CUSTOMER SEGEMNTATION GENERATION
# ----------------------------------------------------------
def cus_segmentation():

    print("🔗 Connecting to PostgreSQL...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # ---------------------------
    # STEP 1: Load Historical Table
    # ---------------------------
    query_hist = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE_1};"
    data = pd.read_sql(query_hist, con=engine)

    print(f"📌 Loaded TOP 3 RESON FILE: {len(data)} rows")
    high_discount_threshold = data['applicable_discount_with_ncb'].quantile(0.75)
    mid_discount_threshold = data['applicable_discount_with_ncb'].quantile(0.5)
    low_discount_threshold = data['applicable_discount_with_ncb'].quantile(0.25)

    high_clv_threshold = data['clv'].quantile(0.75)
    mid_clv_threshold = data['clv'].quantile(0.5)
    low_clv_threshold = data['clv'].quantile(0.25)

    high_churn_probability_threshold = 0.80
    mid_churn_probability_threshold = 0.65
    low_churn_probability_threshold = 0.50

    # Assign CLV, Payment, Discount, and Churn categories
    data['clv_category'] = data['clv'].apply(
        lambda x: 'High' if x > high_clv_threshold else ('Mid' if x > mid_clv_threshold else 'Low')
    )
    data['discount_category'] = data['applicable_discount_with_ncb'].apply(
        lambda x: 'High' if x > high_discount_threshold else ('Mid' if x > mid_discount_threshold else 'Low')
    )
    data['churn_category'] = data['churn_probability'].apply(
        lambda x: 'High' if x > high_churn_probability_threshold else ('Mid' if x > mid_churn_probability_threshold else 'Low')
    )

    # Function to segment customers based on the new criteria
    def segment_policy(row):
        if row['predicted_status'] == 'Not Renewed':  # Process only those who have at least one Not Renewed policy
            if row['churn_category'] == 'Mid' and row['discount_category'] in ['Mid', 'Low'] and row['clv_category'] in ['High', 'Mid']:
                return 'Elite Retainers'
            elif row['churn_category'] == 'Low' and row['discount_category'] in ['Mid', 'Low'] and row['clv_category'] in ['High', 'Mid']:
                return 'Low Value Customers'
            elif row['churn_category'] == 'Mid' and row['discount_category'] in ['High', 'Mid', 'Low'] and row['clv_category'] in ['High', 'Mid', 'Low']:
                return 'Potential Customers'
            elif row['churn_category'] == 'Low' and row['discount_category'] in ['High', 'Mid', 'Low'] and row['clv_category'] in ['High', 'Mid', 'Low']:
                return 'Potential Customers'
            elif row['churn_category'] == 'High' and row['discount_category'] in ['High', 'Mid', 'Low'] and row['clv_category'] in ['High', 'Mid', 'Low']:
                return 'Low Value Customers'
        return None

    # Apply the segmentation function
    data['Customer Segment'] = data.apply(segment_policy, axis=1)

    # ---------------------------
    # STEP 8: Save Final Output Back to Postgres
    # ---------------------------
    # 🔽 Normalize column names before writing to DB
    data.columns = (
        data.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    print("normalization for columns applied")
    load_chunked(data, TARGET_TABLE, TARGET_SCHEMA)

    print("🎉 FINAL OUTPUT SAVED SUCCESSFULLY")

    # Print threshold values
    print("Threshold Values:")
    print(f"applicable_discount_with_ncb - High: {high_discount_threshold}, Mid: {mid_discount_threshold}, Low: {low_discount_threshold}")
    print(f"CLV - High: {high_clv_threshold}, Mid: {mid_clv_threshold}, Low: {low_clv_threshold}")
    print(f"churn_probability - High: {high_churn_probability_threshold}, Mid: {mid_churn_probability_threshold}, Low: {low_churn_probability_threshold}")

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
    schedule_interval=None,      # Run manually
    catchup=False,
    tags=["segmentation", "liberty"],
) as dag:

    segmentation_task = PythonOperator(
        task_id="customer_segmenatation",
        python_callable=cus_segmentation
    )

    segmentation_task