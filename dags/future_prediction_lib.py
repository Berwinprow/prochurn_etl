from datetime import datetime, timedelta
from pathlib import Path
import re

import joblib
import matplotlib.pyplot as plt
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from sqlalchemy import create_engine

from schema_table_config import get_log_tables, get_schema
from config.crypto_utils import get_fernet, encrypt_value, decrypt_value
from config.config_loader import load_sensitive_columns

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
WEIGHTS_DIR = DAGS_DIR / "weights"
META_JSON = str(
    DAGS_DIR / "config" / "schema_metadata_config.json"
)

POSTGRES_CONN_ID = "postgres_cloud_prochurn"

SOURCE_SCHEMA = get_schema("bi_dwh", META_JSON)
SOURCE_TABLE = "final_policy_features"

TARGET_SCHEMA = get_schema("da/ml", META_JSON)
TARGET_TABLE = "future_prediction"

LOG_SCHEMA = get_schema("log", META_JSON)
FEATURE_ENG_LOG = get_log_tables("featurelog", META_JSON)

model = joblib.load(WEIGHTS_DIR / "gbm_model.pkl")
label_encoders = joblib.load(
    WEIGHTS_DIR / "label_encoders_gbm.pkl"
)
features = joblib.load(WEIGHTS_DIR / "model_features_gbm.pkl")

OUTER_CHUNK = 100000
INNER_CHUNK = 50000


# ---------------------------------------------------------------------
# Update metadata
# ---------------------------------------------------------------------
def update_prediction_metadata(engine):
    """
    Updates ONLY two metadata fields:
      - future_pred  (table name)
      - count_of_future_pred (table count)
    """

    # get table count
    after_cnt = pd.read_sql(
        text(f'''SELECT COUNT(*) AS cnt
                 FROM "{TARGET_SCHEMA}"."{TARGET_TABLE}"'''),
        con=engine,
    )["cnt"][0]

    # UPDATE metadata
    sql = f"""
        UPDATE {LOG_SCHEMA}.{FEATURE_ENG_LOG}
        SET
            future_pred = '{TARGET_TABLE}',
            count_of_future_pred = {after_cnt}
        WHERE date = (
            SELECT date
            FROM {LOG_SCHEMA}.{FEATURE_ENG_LOG}
            ORDER BY date DESC
            LIMIT 1
        );
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(f"✔ Metadata updated → future_pred='{TARGET_TABLE}', count={after_cnt}")

# ---------------------------------------------------------------------
# Chunked DB load
# ---------------------------------------------------------------------
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


# ---------------------------------------------------------------------
# Future prediction
# ---------------------------------------------------------------------
def future_prediction():
    print("\n==============================")
    print("model file gen started")
    print("\n==============================")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    read_engine = pg_hook.get_sqlalchemy_engine()
    fernet = get_fernet()
    sensitive_cols = load_sensitive_columns()
    print("🔐 Fernet initialized & sensitive columns loaded")
    with read_engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))
    df = pd.read_sql(
        text(
            f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} "
            "WHERE policy_status = 'Open';"
        ),
        con=read_engine,
    )

    print(f"📊 Loaded rows = {len(df)}")
    # 🔓 Decrypt sensitive columns
    for col in df.columns:
        if col in sensitive_cols:
            df[col] = df[col].apply(lambda x: decrypt_value(x, fernet))
    print(f"🔓 Decrypted sensitive columns for {SOURCE_TABLE}")

    selected_columns = ['add_on_adoption', 'vehicle_age', 'applicable_discount_with_ncb', 'approved', 'avg_premium_hist', 'before_gst_add_on_gwp',
                        'business_type', 'claim_happened_flag', 'claim_approval_rate', 'cleaned_new_branch_name', 'cleaned_chassis_no', 'cleaned_engine_no', 
                        'cleaned_veh_reg_no', 'state', 'zone', 'clv', 'corrected_name', 'customer_id', 
                        'customer_apf', 'customer_apv', 'days_between_renewals', 'days_gap_prev_end_to_curr_start', 'firstyearpolicy', 'fuel_type_risk_factor',
                        'gst', 'idv_premium_ratio', 'lag_1_ncb', 'lag_1_od_premium', 'lag_1_premium', 'lag_1_tp_premium',
                        'make_clean', 'manufacturer_risk_rate', 'model_clean', 'previous_year_ncb_percentage', 'number_of_claims', 'od_tp_ratio', 
                        'policy_no', 'policy_tenure', 'policy_end_date', 'policy_start_date', 'policy_status', 'policy_wise_purchase', 
                        'previous_year_premium_ratio', 'product_name', 'retention_rate_pct', 'retention_streak', 'rto_risk_factor', 'segment_risk_score',
                        'state_risk_score', 'tie_up', 'total_od_premium', 'total_od_premium_max', 'total_od_premium_mean', 'total_od_premium_min',
                        'total_premium_payable', 'total_tp_premium', 'total_tp_premium_max', 'total_tp_premium_mean', 'total_tp_premium_min',
                        'variant', 'vehicle_idv']

    data = df[selected_columns]

    # Convert policy_end_date to datetime
    data["policy_end_date"] = pd.to_datetime(
        data["policy_end_date"], errors="coerce"
    )

    # Filter open customers (Jan - March 2025)
    open_customers = data[
        (data["policy_status"] == "Open")
        & (data["policy_end_date"].dt.year == 2025)
    ].copy()
    print("filtering open customer")

    # Extract date features
    for col in ["policy_start_date", "policy_end_date"]:
        open_customers[col] = pd.to_datetime(
            open_customers[col], errors="coerce"
        )

    open_customers_new_date_cols = {
        f"{col}_YEAR": open_customers[col].dt.year
        for col in ["policy_start_date", "policy_end_date"]
    }
    open_customers_new_date_cols.update(
        {
            f"{col}_MONTH": open_customers[col].dt.month
            for col in ["policy_start_date", "policy_end_date"]
        }
    )
    open_customers_new_date_cols.update(
        {
            f"{col}_DAY": open_customers[col].dt.day
            for col in ["policy_start_date", "policy_end_date"]
        }
    )

    open_customers = pd.concat(
        [open_customers, pd.DataFrame(open_customers_new_date_cols)],
        axis=1,
    )
    open_customers = open_customers.drop(
        columns=["policy_start_date", "policy_end_date"]
    )

    print("year month day extraction from polciy start and end date done")

    # Handle missing values
    for column in open_customers.columns:
        if open_customers[column].dtype == "object":
            open_customers[column] = open_customers[column].fillna(
                "none"
            )
        else:
            open_customers[column] = open_customers[column].fillna(0)

    # Label Encoding for open customers using dynamic mapping
    open_customers_encoded = open_customers.copy()

    for column in open_customers_encoded.columns:
        if column in label_encoders:
            encoder = label_encoders[column]

            mapping_dict = {
                label: i for i, label in enumerate(encoder.classes_)
            }
            next_unique_value = [max(mapping_dict.values()) + 1]

            def encode_test_value(value):
                if value in mapping_dict:
                    return mapping_dict[value]
                mapping_dict[value] = next_unique_value[0]
                next_unique_value[0] += 1
                return mapping_dict[value]

            open_customers_encoded[column] = open_customers_encoded[
                column
            ].apply(encode_test_value)

    print("encoding done")

    # Predict
    X_open_customers = open_customers_encoded[features]
    y_open_pred = model.predict(X_open_customers)
    y_open_pred_proba = model.predict_proba(X_open_customers)[:, 1]
    print("prediction done")

    open_customers["Predicted Status"] = [
        "Not Renewed" if pred == 1 else "Renewed"
        for pred in y_open_pred
    ]
    open_customers["Churn Probability"] = y_open_pred_proba

    # Normalize column names before writing to DB
    open_customers.columns = (
        open_customers.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    print("📝 Normalized column names in df")
    # 🔐 Re-encrypt sensitive columns
    for col in open_customers.columns:
        if col in sensitive_cols:
            open_customers[col] = open_customers[col].apply(lambda x: encrypt_value(x, fernet))
    print(f"🔐 Re-encrypted sensitive columns before loading {TARGET_SCHEMA}.{SOURCE_TABLE}")
    # Save predictions
    load_chunked(open_customers, TARGET_TABLE, TARGET_SCHEMA)
    print(
        f"Predictions saved in {TARGET_SCHEMA}.{TARGET_TABLE} "
        f"with total records of: {len(open_customers)} "
    )

    print(f"Predicted Renewed: {(y_open_pred == 0).sum()}")
    print(f"Predicted Not Renewed: {(y_open_pred == 1).sum()}")

    # ⭐ Update metadata
    update_prediction_metadata(read_engine)


# ---------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="future_prediction",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["weights", "model"],
) as dag:

    prediction_task = PythonOperator(
        task_id="renewed_notrenewed_pred",
        python_callable=future_prediction,
    )

    prediction_task
