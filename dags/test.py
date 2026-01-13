from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import Variable
from datetime import datetime

def test_blob_connection():
    # Read from Airflow Variables
    container_name = Variable.get("AZURE_RAW_CONTAINER")

    # Use Azure Blob connection (Managed Identity)
    hook = WasbHook(wasb_conn_id="azure_blob_mi")

    blobs = hook.get_blobs_list(container_name=container_name)

    print("Blobs found in container:")
    for blob in blobs:
        print(blob)

with DAG(
    dag_id="test_azure_blob_managed_identity",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "azure", "blob"],
) as dag:

    test_blob = PythonOperator(
        task_id="test_blob_access",
        python_callable=test_blob_connection
    )
