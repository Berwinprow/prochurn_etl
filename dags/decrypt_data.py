
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import text
import pandas as pd
import logging
import json

from crypto.crypto_utils import get_fernet, decrypt_value
from utils.config_loader import load_sensitive_columns

# ------------------------------------------------------------
# Constants
# ------------------------------------------------------------
POSTGRES_CONN_ID = "postgres_cloud_prochurn"

# ------------------------------------------------------------
# Main decrypt function
# ------------------------------------------------------------
def decrypt_any_table_for_ui(**context):
    conf = context["dag_run"].conf or {}

    schema = conf.get("schema")
    table_name = conf.get("table_name")
    filter_column = conf.get("filter_column")
    filter_values = conf.get("filter_values")

    if not schema or not table_name:
        raise ValueError("schema and table_name are required")

    if not filter_column or not filter_values:
        raise ValueError("filter_column and filter_values are required")

    logging.info(f"Schema      : {schema}")
    logging.info(f"Table       : {table_name}")
    logging.info(f"Filter col  : {filter_column}")
    logging.info(f"Filter vals : {filter_values}")

    # DB connection
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # -------------------------------
    # Dynamic IN clause preparation
    # -------------------------------
    placeholders = ", ".join([f":val{i}" for i in range(len(filter_values))])
    params = {f"val{i}": v for i, v in enumerate(filter_values)}

    query = text(f"""
        SELECT *
        FROM "{schema}"."{table_name}"
        WHERE {filter_column} IN ({placeholders})
    """)

    df = pd.read_sql(query, engine, params=params)

    if df.empty:
        logging.info("No data found for given filters")
        return []

    # 🔐 Decrypt setup
    fernet = get_fernet()
    sensitive_cols = load_sensitive_columns()

    logging.info("Decrypting sensitive columns")

    for col in df.columns:
        if col in sensitive_cols:
            df[col] = df[col].apply(
                lambda x: decrypt_value(x, fernet) if x is not None else None
            )

    # 🔧 FIX: Convert Timestamp to string (XCom safe)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)

    result = df.to_dict(orient="records")
    logging.info(f"Returning {len(result)} rows")

    return json.dumps(result)


# ------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------
with DAG(
    dag_id="test_decrypt_any_table_for_ui",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "decrypt", "ui"],
) as dag:

    decrypt_task = PythonOperator(
        task_id="decrypt_any_table",
        python_callable=decrypt_any_table_for_ui,
        provide_context=True,
    )
