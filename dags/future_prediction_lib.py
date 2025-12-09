from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import LabelEncoder
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import joblib
import re
from pathlib import Path


DAGS_DIR = Path(__file__).resolve().parent
WEIGHTS_DIR = DAGS_DIR / "weights"

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
SOURCE_SCHEMA = "test_aggregation"
SOURCE_TABLE = "policydata_with_fb_cc_pc_newfea_opti_correct"
TARGET_SCHEMA = "test_bi_dwh"
TARGET_TABLE = "gbm1_prediction_jfmamj_final"

model = joblib.load(WEIGHTS_DIR / "gbm_model.pkl")
label_encoders = joblib.load(WEIGHTS_DIR / "label_encoders_gbm.pkl")
features = joblib.load(WEIGHTS_DIR / "model_features_gbm.pkl")

OUTER_CHUNK = 10000
INNER_CHUNK = 5000
# ---------------------------------------------------------------------
# Load Data To Postgres
# ----------------------------------------------------------------------
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
# ---------------------------------------------------------------------
# Future prediction 
# ----------------------------------------------------------------------
def future_prediction():
    print(f"\n==============================")
    print(f"model file gen started")
    print(f"\n==============================")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    read_engine = pg_hook.get_sqlalchemy_engine()

    with read_engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))

    df = pd.read_sql(
        text(f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} WHERE policy_staus = 'Open';"),
        con=read_engine
    )

    print(f"📊 Loaded rows = {len(df)}")

    selected_columns = ['add_on_adoption', 'vehicle_age', 'applicable_discount_with_ncb', 'approved', 'avg_premium_hist', 'before_gst_add_on_gwp',
                        'business_type', 'claim_happened_not', 'claim_approval_rate', 'cleaned_branch_name_2', 'cleaned_chassis_number', 'cleaned_engine_number', 
                        'cleaned_reg_no', 'cleaned_state_2', 'cleaned_zone_2', 'clv', 'corrected_name', 'customerid', 
                        'customer_apf', 'customer_apv', 'days_between_renewals', 'days_gap_prev_end_to_curr_start', 'firstpolicyyear', 'fuel_type_risk_factor',
                        'gst', 'idv_premium_ratio', 'lag_1_ncb', 'lag_1_od_premium', 'lag_1_premium', 'lag_1_tp_premium',
                        'make_clean', 'manufacturer_risk_rate', 'model_clean', 'previous_year_ncb_percentage', 'number_of_claims', 'od_tp_ratio', 
                        'policy_no', 'policy_tenure', 'policy_end_date', 'policy_start_date', 'policy_status', 'policy_wise_purchase', 
                        'previous_year_premium_ratio', 'product_name', 'retention_rate_pct', 'retention_streak', 'rto_risk_factor', 'segment_risk_score',
                        'state_risk_score', 'tie_up', 'total_od_premium', 'total_od_premium_max', 'total_od_premium_mean', 'total_od_premium_min',
                        'total_premium_payable', 'total_tp_premium', 'total_tp_premium_max', 'total_tp_premium_mean', 'total_tp_premium_min',
                        'variant', 'vehicle_idv']

    data = df[selected_columns]

    # Convert policy_end_date to datetime
    data['policy_end_date'] = pd.to_datetime(data['policy_end_date'], errors='coerce')

    # Filter open customers (Jan - March 2025)
    open_customers = data[
        (data['policy_status'] == 'Open') & 
        (data['policy_end_date'].dt.year == 2025)].copy()
    print("filtering open customer")
    # Extract date features
    for col in ['policy_start_date', 'policy_end_date']:
        open_customers[col] = pd.to_datetime(open_customers[col], errors='coerce')

    open_customers_new_date_cols = {
        f'{col}_YEAR': open_customers[col].dt.year for col in ['policy_start_date', 'policy_end_date']
    }
    open_customers_new_date_cols.update({
        f'{col}_MONTH': open_customers[col].dt.month for col in ['policy_start_date', 'policy_end_date']
    })
    open_customers_new_date_cols.update({
        f'{col}_DAY': open_customers[col].dt.day for col in ['policy_start_date', 'policy_end_date']
    })

    open_customers = pd.concat([open_customers, pd.DataFrame(open_customers_new_date_cols)], axis=1)
    open_customers = open_customers.drop(columns=['policy_start_date', 'policy_end_date'])

    print("year month day extraction from polciy start and end date done")

    # Handle missing values
    for column in open_customers.columns:
        if open_customers[column].dtype == 'object':
            open_customers[column] = open_customers[column].fillna('none')
        else:
            open_customers[column] = open_customers[column].fillna(0)

    # Label Encoding for open customers using dynamic mapping
    open_customers_encoded = open_customers.copy()

    for column in open_customers_encoded.columns:
        if column in label_encoders:  
            encoder = label_encoders[column]

            # Get existing mapping from the trained encoder
            mapping_dict = {label: i for i, label in enumerate(encoder.classes_)}
            next_unique_value = [max(mapping_dict.values()) + 1]  

            # Function to encode new values dynamically
            def encode_test_value(value):
                if value in mapping_dict:
                    return mapping_dict[value]
                else:
                    mapping_dict[value] = next_unique_value[0]
                    next_unique_value[0] += 1
                    return mapping_dict[value]
            
            open_customers_encoded[column] = open_customers_encoded[column].apply(encode_test_value)
    print("encoding done")
    # Predict
    X_open_customers = open_customers_encoded[features]
    y_open_pred = model.predict(X_open_customers)
    y_open_pred_proba = model.predict_proba(X_open_customers)[:, 1]
    print("prediction done")
    open_customers['Predicted Status'] = ['Not Renewed' if pred == 1 else 'Renewed' for pred in y_open_pred]
    open_customers['Churn Probability'] = y_open_pred_proba
    # 🔽 Normalize column names before writing to DB
    open_customers.columns = (
        open_customers.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    print("📝 Normalized column names in df")
    # Save predictions
    load_chunked(open_customers,TARGET_TABLE,TARGET_SCHEMA)
    print(f"Predictions saved in {TARGET_SCHEMA}.{TARGET_TABLE} with total records of: {len(open_customers)} ")

    print(f"Predicted Renewed: {(y_open_pred == 0).sum()}")
    print(f"Predicted Not Renewed: {(y_open_pred == 1).sum()}")

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
    schedule_interval=None,      # Run manually
    catchup=False,
    tags=["weights", "model"],
) as dag:

    prediction_task = PythonOperator(
        task_id="renewed_notrenewed_pred",
        python_callable=future_prediction
    )
 
    prediction_task 