from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import pandas as pd

from schema_table_config import get_log_tables, get_schema
from customer_segmentation_lib import cus_segmentation

# ----------------------------------------------------------
# Source / Target Tables
# ----------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")

SOURCE_TABLE_1 = "future_prediction_with_notrenewal_reason"
SOURCE_TABLE_2 = "final_policy_features_with_notrenewed_reason_only"

TARGET_TABLE = "future_predition_with_top3_reason"

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
def update_top3_metadata(engine):
    """Mark top-3 reason step completed in feature log."""
    after_cnt = pd.read_sql(
        text(
            f'''SELECT COUNT(*) AS cnt FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"'''
        ),
        con=engine,
    )["cnt"][0]

    sql = f"""
        UPDATE {LOG_SCHEMA}.{FEATURE_ENG_LOG}
        SET
            top_3_reason = {TARGET_TABLE},
            top_3_reason_cnt = {after_cnt},
        WHERE date = (
            SELECT date
            FROM {LOG_SCHEMA}.{FEATURE_ENG_LOG}
            ORDER BY date DESC
            LIMIT 1
        );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

    print(f"✅ Top-3 metadata updated. rows={after_cnt}")


# ----------------------------------------------------------
# Chunk Load
# ----------------------------------------------------------
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


# ----------------------------------------------------------
# TOP 3 REASON GENERATION
# ----------------------------------------------------------
def top_3_reason():
    print("🔗 Connecting to PostgreSQL...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # ---------------------------
    # STEP 1: Load Historical Table
    # ---------------------------
    query_hist = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE_2};"
    df_hist = pd.read_sql(query_hist, con=engine)

    print(f"📌 Loaded Historical Table: {len(df_hist)} rows")

    # Preprocess
    df_hist["not_renewed_reasons"] = (
        df_hist["not_renewed_reasons"].fillna("").astype(str)
    )

    # Step 2: Explode historical reason counts
    all_reasons = (
        df_hist["not_renewed_reasons"]
        .str.split(",")
        .explode()
        .str.strip()
    )
    reason_counts = all_reasons.value_counts().reset_index()
    reason_counts.columns = ["Reason", "Count"]
    print("exploded historical reason count")

    # Step 3: Rank by frequency
    reason_counts["Rank"] = reason_counts["Count"].rank(
        method="dense", ascending=False
    ).astype(int)
    reason_counts = reason_counts.sort_values(by="Rank")

    # Rank dictionary
    rank_dict = reason_counts.set_index("Reason")["Rank"].to_dict()
    print("ranking historical reason")

    # Step 4: Preference order
    preference_order = [
        "Low Discount with NCB",
        "Low Vehicle IDV",
        "High Own-Damage Premium",
        "High Third-Party Premium",
        "High Add-On Premium",
        "Claims Happened",
        "Young Vehicle Age",
        "Tie Up with Non-OEM",
        "Tie Up with HYUNDAI",
        "Tie Up with MIBL OEM",
        "Tie Up with MARUTI",
        "Low No Claim Bonus Percentage",
        "Multiple Claims on Record",
        "Old Vehicle Age",
        "Minimal Policies Purchased",
        "Policy Tenure Exceeds 2 Years",
    ]

    preference_dict = {reason: i for i, reason in enumerate(preference_order, 1)}

    # ---------------------------
    # STEP 5: Load NEW Prediction Table
    # ---------------------------
    query_pred = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE_1};"
    df = pd.read_sql(query_pred, con=engine)

    print(f"📌 Loaded Prediction Table: {len(df)} rows")

    df["not_renewed_reasons"] = (
        df["not_renewed_reasons"].fillna("").astype(str)
    )
    print(f"cleaned {SOURCE_TABLE_1} not renewed reason")

    # ---------------------------
    # STEP 6: Define Top-3 Logic
    # ---------------------------
    def get_top_3_reasons(reason_string):
        if not reason_string.strip():
            return None

        reasons = [r.strip() for r in reason_string.split(",")]

        reason_tuples = []
        for r in reasons:
            pref = preference_dict.get(r, float("inf"))
            rank = rank_dict.get(r, float("inf"))
            reason_tuples.append((r, pref, rank))

        reason_tuples.sort(key=lambda x: (x[1], x[2]))

        top_reasons = [t[0] for t in reason_tuples[:3]]

        if len(top_reasons) == 1:
            return top_reasons[0]
        if len(top_reasons) == 2:
            return f"{top_reasons[0]} and {top_reasons[1]}"
        return (
            f"{top_reasons[0]}, {top_reasons[1]} and "
            f"{top_reasons[2]}"
        )

    # ---------------------------
    # STEP 7: Apply on Prediction File
    # ---------------------------
    df["Top 3 Reasons"] = df["not_renewed_reasons"].apply(
        get_top_3_reasons
    )

    print("✔ Top 3 reasons generated successfully")

    # ---------------------------
    # STEP 8: Save Final Output Back to Postgres
    # ---------------------------
    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_")
    )
    print("normalization for columns applied")
    load_chunked(df, TARGET_TABLE, TARGET_SCHEMA)

    print("🎉 FINAL OUTPUT SAVED SUCCESSFULLY")

    # ---------- update metadata ----------
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    update_top3_metadata(engine)


# # ----------------------------------------------------------
# # DAG
# # ----------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="top_3_reason",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["reasons", "churn", "liberty"],
):
    task_top3 = PythonOperator(
        task_id="generate_top_3_reasons",
        python_callable=top_3_reason,
    )
    segmentation_task = PythonOperator(
        task_id="customer_segmenatation",
        python_callable=cus_segmentation,
    )

    task_top3 >> segmentation_task
