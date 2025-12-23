from datetime import datetime, timedelta
from pathlib import Path
import re

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from imblearn.over_sampling import RandomOverSampler
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    log_loss,
    roc_auc_score,
    roc_curve,
)
from sklearn.preprocessing import LabelEncoder
from sqlalchemy import create_engine, text

from schema_table_config import get_log_tables, get_schema


# --------------------------------------------------------------------
# Paths / constants
# --------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")
WEIGHTS_DIR = DAGS_DIR / "weights"
WEIGHTS_DIR.mkdir(exist_ok=True)

# Path to save files
model_file_path = WEIGHTS_DIR / "gbm_model.pkl"
label_file_path = WEIGHTS_DIR / "label_encoders_gbm.pkl"
feature_file_path = WEIGHTS_DIR / "model_features_gbm.pkl"

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
SOURCE_SCHEMA = get_schema("bi_dwh", META_JSON)
SOURCE_TABLE = "final_policy_features"


# --------------------------------------------------------------------
# Model generation
# --------------------------------------------------------------------
def model_file_generation():
    print("\n==============================")
    print("model file gen started")
    print("\n==============================")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    read_engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        text(f'SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE};'),
        con=read_engine,
    )

    print(f"📊 Loaded rows = {len(df)}")

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

    # Filter the main dataset for customers whose policy_end_date
    # is <= July 2024
    data = data[data["policy_status"].isin(["Renewed", "Not Renewed"])]

    # Map policy_status to binary for the filtered dataset
    data["policy_status"] = data["policy_status"].apply(
        lambda x: 1 if x == "Not Renewed" else 0
    )

    # Handle missing values
    for column in data.columns:
        if data[column].dtype == "object":
            data[column] = data[column].fillna("none")
        else:
            data[column] = data[column].fillna(0)

    # Extract year, month, and day from date columns
    date_columns = ["policy_start_date", "policy_end_date"]
    for col in date_columns:
        data[col] = pd.to_datetime(data[col], errors="coerce")

    new_date_cols = {}
    for col in date_columns:
        new_date_cols[f"{col}_YEAR"] = data[col].dt.year
        new_date_cols[f"{col}_MONTH"] = data[col].dt.month
        new_date_cols[f"{col}_DAY"] = data[col].dt.day

    data = pd.concat([data, pd.DataFrame(new_date_cols)], axis=1)

    # Drop original date columns
    data = data.drop(columns=date_columns)

    # Separate features and target variable for training
    features = [col for col in data.columns if col != "policy_status"]
    X = data[features]
    y = data["policy_status"]

    # Initialize RandomOverSampler
    ros = RandomOverSampler(random_state=42)

    # Apply Random Oversampling to the training data
    X, y = ros.fit_resample(X, y)

    # label encoding for the actual data
    label_encoders = {}
    for column in X.columns:
        if X[column].dtype == "object":
            label_encoder = LabelEncoder()
            X[column] = label_encoder.fit_transform(
                X[column].astype(str)
            )
            label_encoders[column] = label_encoder

    # XGBoost model
    model = GradientBoostingClassifier(
        max_depth=6,
        learning_rate=0.1,
        n_estimators=100,
        random_state=42,
    )

    # Fit the model
    model.fit(X, y)

    # Save the trained model
    joblib.dump(model, model_file_path)
    print(f"✅ Model saved at: {model_file_path}")

    joblib.dump(label_encoders, label_file_path)
    print(f"✅ Label encoders saved at: {label_file_path}")

    joblib.dump(features, feature_file_path)
    print(f"✅ Feature list saved at: {feature_file_path}")

    y_pred = model.predict(X)
    y_pred_proba = model.predict_proba(X)[:, 1]

    # Evaluate the model on training data
    train_accuracy = accuracy_score(y, y_pred)
    train_log_loss = log_loss(y, y_pred_proba)
    train_roc_auc = roc_auc_score(y, y_pred_proba)
    train_report = classification_report(y, y_pred)

    conf_matrix_train = confusion_matrix(y, y_pred)
    class_0_accuracy_train = (
        conf_matrix_train[0, 0] / conf_matrix_train[0].sum()
    )
    class_1_accuracy_train = (
        conf_matrix_train[1, 1] / conf_matrix_train[1].sum()
    )

    # Print the metrics
    print(f"Train Accuracy: {train_accuracy}")
    print(f"Train Log Loss: {train_log_loss}")
    print(f"Train ROC AUC: {train_roc_auc}")
    print(f"Train Classification Report:\n{train_report}")
    print(f"Class 0 Train Accuracy: {class_0_accuracy_train}")
    print(f"Class 1 Train Accuracy: {class_1_accuracy_train}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weight_file_generator",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["weights", "model"],
) as dag:

    generate_weights_task = PythonOperator(
        task_id="generate_weight_file",
        python_callable=model_file_generation,
    )

    generate_weights_task
