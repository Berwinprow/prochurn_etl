import pandas as pd
import numpy as np
import io
from psycopg2 import Binary
import joblib
from datetime import datetime
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import roc_curve, auc, roc_auc_score
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import LabelEncoder
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, text
from sklearn.metrics import (
    accuracy_score, log_loss, roc_auc_score,
    classification_report, confusion_matrix, roc_curve
)
from imblearn.over_sampling import RandomOverSampler
from pathlib import Path


DAGS_DIR = Path(__file__).resolve().parent
WEIGHTS_DIR = DAGS_DIR / "weights"
POSTGRES_CONN_ID = "postgres_cloud_prochurn"

SOURCE_SCHEMA = "test_aggregation"
SOURCE_TABLE = "policydata_with_fb_cc_pc_newfea_opti_correct"

TARGET_SCHEMA = "health_monitoring"
roc_table_name = 'model_roc_curve_data'
model_performance_table_name = 'model_performance_metrics'
prediction_table = "training_set_predictions_with_probs"

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
# Load Data To Postgres
# ----------------------------------------------------------------------   
def save_image_to_db(fig, name, engine, schema=TARGET_SCHEMA, file_type="png"):
    # Ensure table exists
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.artifacts (
        id SERIAL PRIMARY KEY,
        name TEXT,
        file_type TEXT,
        content BYTEA,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

    # Convert fig to bytes
    buf = io.BytesIO()
    fig.savefig(buf, format=file_type, bbox_inches='tight')
    buf.seek(0)
    img_bytes = buf.read()

    # Insert into DB
    raw = engine.raw_connection()
    cur = raw.cursor()
    cur.execute(
        f"INSERT INTO {schema}.artifacts (name, file_type, content) VALUES (%s, %s, %s)",
        (name, file_type, Binary(img_bytes))
    )
    raw.commit()
    cur.close()
    raw.close()

    print(f"Saved image '{name}' into {schema}.artifacts")
# ---------------------------------------------------------------------
#Health Monitoring
# ----------------------------------------------------------------------
def Monitoring():
    print(f"\n==============================")
    print(f"model health monitoring started")
    print(f"\n==============================")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))

    df = pd.read_sql(
        text(f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE};"),
        con=engine
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
    

    df = df[selected_columns].copy()

    # SAME PREPROCESSING AS TRAINING
    # Convert date column(s)
    df['policy_end_date'] = pd.to_datetime(df['policy_end_date'], errors='coerce')
    df['policy_start_date'] = pd.to_datetime(df['policy_start_date'], errors='coerce')

    # Filter the main dataset to the same subset training used originally:
    df = df[df['policy_status'].isin(['Renewed', 'Not Renewed'])].copy()

    # Map policy_status to binary exactly as training did
    df['policy_status'] = df['policy_status'].apply(lambda x: 1 if x == 'Not Renewed' else 0)

    # Fill missing values exactly as training did
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].fillna('none')
        else:
            df[column] = df[column].fillna(0)

    # Create date-derived features (YEAR, MONTH, DAY) and drop original date cols
    date_cols = ['policy_start_date', 'policy_end_date']
    new_date_cols = {}
    for col in date_cols:
        new_date_cols[f'{col}_YEAR'] = df[col].dt.year
        new_date_cols[f'{col}_MONTH'] = df[col].dt.month
        new_date_cols[f'{col}_DAY'] = df[col].dt.day

    df = pd.concat([df, pd.DataFrame(new_date_cols)], axis=1)
    df = df.drop(columns=date_cols)

    # Separate features/target
    X = df[[c for c in df.columns if c != 'policy_status']].copy()
    y = df['policy_status'].copy()

    # APPLY THE SAME OVERSAMPLING USED IN TRAINING
    ros = RandomOverSampler(random_state=42)
    X_res, y_res = ros.fit_resample(X, y)

    # ENCODE CATEGORICALS USING SAVED ENCODERS
    X_enc = X_res.copy()

    for column in X_enc.columns:
        if column in label_encoders:
            
            encoder = label_encoders[column]

            # Build Version-1 mapping using encoder classes
            mapping_dict = {label: i for i, label in enumerate(encoder.classes_)}

            # Version-1 behavior
            next_unique_value = [max(mapping_dict.values()) + 1]

            # Dynamic encoder (Version-1 style)
            def encode_value(value):
                key = str(value)  
                if key in mapping_dict:
                    return mapping_dict[key]
                else:
                    mapping_dict[key] = next_unique_value[0]
                    next_unique_value[0] += 1
                    return mapping_dict[key]

            X_enc[column] = X_enc[column].astype(str).apply(encode_value)


    # For any remaining object dtype columns that DID NOT have saved encoders, convert to strings
    # and map to integers to ensure the model receives numeric data.
    for col in X_enc.columns:
        if X_enc[col].dtype == 'object':
            X_enc[col] = X_enc[col].astype(str).factorize()[0]

    # Ensure column order matches features list (and handle missing features gracefully)
    missing_feats = [f for f in features if f not in X_enc.columns]
    if missing_feats:
        print("Warning: missing features added with zeros:", missing_feats)
        for feat in missing_feats:
            X_enc[feat] = 0

    X_enc = X_enc[features]  

    # PREDICT
    y_pred = model.predict(X_enc)
    y_pred_proba = model.predict_proba(X_enc)[:, 1]

    # clip probabilities to avoid numerical issues in log_loss
    y_pred_proba = np.clip(y_pred_proba, 1e-6, 1 - 1e-6)

    # ==================== ROC CURVE CALCULATION ====================
    # Calculate ROC curve points
    fpr, tpr, thresholds = roc_curve(y_res, y_pred_proba)

    # # Print ROC curve points to console
    # print("\n" + "="*70)
    # print("ROC CURVE DATA POINTS")
    # print("="*70)
    # print(f"{'False Positive Rate':<25} | {'True Positive Rate':<25} | {'Threshold':<15}")
    # print("-" * 70)
    # for i in range(len(fpr)):
    #     print(f"{fpr[i]:<25.6f} | {tpr[i]:<25.6f} | {thresholds[i]:<15.6f}")
    # print("="*70 + "\n")

    # Create ROC curve dataframe
    roc_df = pd.DataFrame({
        'run_id': datetime.utcnow().strftime('%Y%m%d_%H%M%S'),
        'model_name': 'gbm_model',
        'run_timestamp_utc': datetime.utcnow(),
        'dataset': 'training_oversampled',
        'point_index': range(len(fpr)),
        'false_positive_rate': fpr,
        'true_positive_rate': tpr,
        'threshold': thresholds
    })

    with engine.begin() as conn:
        create_roc_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{roc_table_name} (
            run_id TEXT,
            model_name TEXT,
            run_timestamp_utc TIMESTAMP,
            dataset TEXT,
            point_index INTEGER,
            false_positive_rate DOUBLE PRECISION,
            true_positive_rate DOUBLE PRECISION,
            threshold DOUBLE PRECISION
        );
        """
        conn.execute(text(create_roc_table_sql))
    print(f"table {roc_table_name} created sucessfully")

    load_chunked(roc_df,roc_table_name,TARGET_SCHEMA)
    print(f"ROC curve data saved to PostgreSQL table: {roc_table_name}")
    print(f"Total ROC points saved: {len(roc_df)}\n")

    # METRICS
    train_accuracy = accuracy_score(y_res, y_pred)
    train_log_loss = log_loss(y_res, y_pred_proba)
    train_roc_auc = roc_auc_score(y_res, y_pred_proba)

    train_report = classification_report(y_res, y_pred, zero_division=0)
    conf_matrix_train = confusion_matrix(y_res, y_pred)

    # Class-wise accuracy (guard for shapes)
    class_0_accuracy_train = conf_matrix_train[0, 0] / conf_matrix_train[0].sum() if conf_matrix_train.shape[0] > 0 and conf_matrix_train[0].sum() > 0 else np.nan
    class_1_accuracy_train = conf_matrix_train[1, 1] / conf_matrix_train[1].sum() if conf_matrix_train.shape[0] > 1 and conf_matrix_train[1].sum() > 0 else np.nan

    # Per-class precision, recall, f1
    report_dict = classification_report(y_res, y_pred, output_dict=True, zero_division=0)

    precision_0 = report_dict.get("0", {}).get("precision", np.nan)
    recall_0    = report_dict.get("0", {}).get("recall", np.nan)
    f1_0        = report_dict.get("0", {}).get("f1-score", np.nan)

    precision_1 = report_dict.get("1", {}).get("precision", np.nan)
    recall_1    = report_dict.get("1", {}).get("recall", np.nan)
    f1_1        = report_dict.get("1", {}).get("f1-score", np.nan)

    # Macro avg
    precision_macro = report_dict.get("macro avg", {}).get("precision", np.nan)
    recall_macro    = report_dict.get("macro avg", {}).get("recall", np.nan)
    f1_macro        = report_dict.get("macro avg", {}).get("f1-score", np.nan)

    # Extract confusion matrix values
    tn = int(conf_matrix_train[0, 0]) if conf_matrix_train.shape[0] > 0 else 0
    fp = int(conf_matrix_train[0, 1]) if conf_matrix_train.shape[0] > 0 and conf_matrix_train.shape[1] > 1 else 0
    fn = int(conf_matrix_train[1, 0]) if conf_matrix_train.shape[0] > 1 else 0
    tp = int(conf_matrix_train[1, 1]) if conf_matrix_train.shape[0] > 1 and conf_matrix_train.shape[1] > 1 else 0


    # Compose metrics row (use the train_* variables)
    metrics_row = {
        'run_id': datetime.utcnow().strftime('%Y%m%d_%H%M%S'),
        'model_name': 'gbm_model',
        'run_timestamp_utc': datetime.utcnow(),
        'dataset': 'training_oversampled',
        'n_samples': int(len(y_res)),
        'accuracy': float(train_accuracy),
        'log_loss': float(train_log_loss),
        'roc_auc': float(train_roc_auc) if not np.isnan(train_roc_auc) else None,
        'class_0_accuracy': float(class_0_accuracy_train) if not np.isnan(class_0_accuracy_train) else None,
        'class_1_accuracy': float(class_1_accuracy_train) if not np.isnan(class_1_accuracy_train) else None,

        # Confusion Matrix Values
        'true_negative': tn,
        'false_positive': fp,
        'false_negative': fn,
        'true_positive': tp,

        # Per-class
        'precision_0': float(precision_0) if not np.isnan(precision_0) else None,
        'recall_0': float(recall_0) if not np.isnan(recall_0) else None,
        'f1_0': float(f1_0) if not np.isnan(f1_0) else None,

        'precision_1': float(precision_1) if not np.isnan(precision_1) else None,
        'recall_1': float(recall_1) if not np.isnan(recall_1) else None,
        'f1_1': float(f1_1) if not np.isnan(f1_1) else None,

        # Macro
        'precision_macro': float(precision_macro) if not np.isnan(precision_macro) else None,
        'recall_macro': float(recall_macro) if not np.isnan(recall_macro) else None,
        'f1_macro': float(f1_macro) if not np.isnan(f1_macro) else None
    }

    metrics_df = pd.DataFrame([metrics_row])

    with engine.begin() as conn:
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{model_performance_table_name} (
            run_id TEXT,
            model_name TEXT,
            run_timestamp_utc TIMESTAMP,
            dataset TEXT,
            n_samples INTEGER,
            accuracy DOUBLE PRECISION,
            log_loss DOUBLE PRECISION,
            roc_auc DOUBLE PRECISION,
            class_0_accuracy DOUBLE PRECISION,
            class_1_accuracy DOUBLE PRECISION,
            true_negative INTEGER,
            false_positive INTEGER,
            false_negative INTEGER,
            true_positive INTEGER,
            precision_0 DOUBLE PRECISION,
            recall_0 DOUBLE PRECISION,
            f1_0 DOUBLE PRECISION,
            precision_1 DOUBLE PRECISION,
            recall_1 DOUBLE PRECISION,
            f1_1 DOUBLE PRECISION,
            precision_macro DOUBLE PRECISION,
            recall_macro DOUBLE PRECISION,
            f1_macro DOUBLE PRECISION
        );
        """
        conn.execute(text(create_sql))

    print(f"table {model_performance_table_name} created sucessfully")
    
    # append the metrics row
    load_chunked(metrics_df,model_performance_table_name,TARGET_SCHEMA)
    

    pred_out = X_res.copy()
    pred_out['actual'] = y_res
    pred_out['predicted'] = y_pred
    pred_out['predicted_proba'] = y_pred_proba
    load_chunked(pred_out,prediction_table,TARGET_SCHEMA)

    # PRINT SUMMARY
    print(f"Saved metrics to Postgres table: {TARGET_SCHEMA}.{model_performance_table_name}")
    print(metrics_df.to_string(index=False))
    print(f"Saved full predictions db to: {TARGET_SCHEMA}.{prediction_table}")

    # --- Ensure ROC values exist (compute if not present) ---
    # Uses training variables from your run: y_res (true), y_pred_proba (predicted probs), conf_matrix_train (confusion matrix)

    fpr_train, tpr_train, _ = roc_curve(y_res, y_pred_proba)
    train_roc_auc = roc_auc_score(y_res, y_pred_proba)

    # --- Plot ROC curve for training data (green style) ---
    fig_roc = plt.figure(figsize=(8, 6))
    plt.plot(fpr_train, tpr_train, color='green', lw=2,
            label=f'ROC curve (train) (area = {train_roc_auc:.2f})')
    plt.plot([0, 1], [0, 1], color='gray', lw=2, linestyle='--')
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver Operating Characteristic (ROC) Curve - Training Data')
    plt.legend(loc="lower right")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    save_image_to_db(fig_roc, "roc_curve", engine)
    plt.close(fig_roc)

    # --- Plot Confusion Matrix - Training Data (green heatmap) ---
    fig_cm = plt.figure(figsize=(6, 5))
    sns.heatmap(conf_matrix_train, annot=True, fmt='d', cmap='Greens',
                xticklabels=['Not Churn (0)', 'Churn (1)'],
                yticklabels=['Not Churn (0)', 'Churn (1)'],
                cbar=False)
    plt.xlabel('Predicted')
    plt.ylabel('Actual')
    plt.title('Confusion Matrix - Training Data (GBM)')
    plt.tight_layout()
    save_image_to_db(fig_cm, "confusion_matrix", engine)
    plt.close(fig_cm)
 
    engine.dispose()
    

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="health_monitoring",
    default_args=default_args,
    schedule_interval=None,      # Run manually
    catchup=False,
    tags=["weights", "model"],
) as dag:

    monitoring_task = PythonOperator(
        task_id="model_health_monitoring",
        python_callable= Monitoring
    )
 
    monitoring_task 