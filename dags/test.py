from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import Variable
from datetime import datetime
from airflow.models import Variable
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from crypto.crypto_utils import get_postgres_password
import logging

def test_blob_connection():
    # Read from Airflow Variables
    container_name = Variable.get("AZURE_RAW_CONTAINER")

    # Use Azure Blob connection (Managed Identity)
    hook = WasbHook(wasb_conn_id="azure_blob_mi")

    blobs = hook.get_blobs_list(container_name=container_name)

    print("Blobs found in container:")
    for blob in blobs:
        print(blob)

def test_schema_access():
    # --- Non-secret config from Airflow Variables ---
    pg_host = Variable.get("PG_HOST")
    pg_port = Variable.get("PG_PORT")
    pg_db   = Variable.get("PG_DB")
    pg_user = Variable.get("PG_USER")

    # --- Secret from Key Vault ---
    pg_password = get_postgres_password()
    safe_password = quote_plus(pg_password)

    # --- Build connection ---
    pg_url = (
        f"postgresql+psycopg2://{pg_user}:{safe_password}"
        f"@{pg_host}:{pg_port}/{pg_db}"
    )

    engine = create_engine(pg_url)

    # --- Query schema which airflow_user may NOT have access to ---
    query = text("select * from lib_log.claim_etl_log limit 1")

    try:
        with engine.connect() as conn:
            result = conn.execute(query).fetchall()
            logging.info("Query succeeded. Rows: %s", result)

    except Exception as e:
        logging.error("❌ Permission test failed as expected")
        logging.error(str(e))
        raise

with DAG(
    dag_id="test_azure_blob_managed_identity",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "azure", "blob"],
) as dag:

    test_blob = PythonOperator(
        task_id="testing",
        python_callable=test_schema_access
    )