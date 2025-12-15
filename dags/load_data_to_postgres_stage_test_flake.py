# ============================================================
# 📦 Airflow ETL: Azure Blob → PostgreSQL (PEP8 + Flake8 Clean)
# ============================================================

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import io
import json
import logging
import re
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import TEXT, Integer, Float, DateTime
from azure.storage.blob import BlobServiceClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from pathlib import Path
from schema_table_config import get_schema, get_log_tables



# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------
POSTGRES_CONN_ID = "postgres_cloud_prochurn"
AZURE_BLOB_CONN_ID = "azure_blob"
DAGS_DIR = Path(__file__).resolve().parent
JSON_PATH = str(DAGS_DIR/"config"/"schema_metadata_config.json")
SCHEMA_NAME_VAR = get_schema("stage",JSON_PATH)
BATCH_SIZE = 1000
LOG_SCHEMA = get_schema("log",JSON_PATH)


# ---------------------------------------------------------------------
# 🧹 Clean column names
# ---------------------------------------------------------------------
def clean_column_names(df):
    """Clean and standardize DataFrame column names."""
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace(" ", "_", regex=True)
        .str.replace(r"[()\[\]{}]", "", regex=True)
        .str.lower()
    )

    seen = set()
    new_columns = []
    for col in df.columns:
        new_col = col
        count = 1
        while new_col in seen:
            new_col = f"{col}_{count}"
            count += 1
        seen.add(new_col)
        new_columns.append(new_col)
    df.columns = new_columns
    return df

# ---------------------------------------------------------------------
# 🏷️ Normalize table names
# ---------------------------------------------------------------------
def normalize_table_name(file_name: str, sheet_name: str = "Sheet1"):
    """Generate normalized table name based on file and sheet names."""
    name = file_name.split('.')[0].lower().replace('-', '_').replace(' ', '_')
    year_match = re.search(r'(?<!\d)(\d{2,4})(?!\d)', name)
    year = year_match.group(1) if year_match else 'unknown'
    if len(year) == 2:
        year = '20' + year

    

    if 'base' in name:
        return f'base_{year}'
    if 'pr' in name:
        return f'pr_{year}'
    if 'claim' in name:
        part_match = re.search(r'part[_]?(\d+)', name)
        part = part_match.group(1) if part_match else '1'
        return f'claim_{year}_part_{part}'
    return f'unknown_{year}'


# ---------------------------------------------------------------------
# 🗃️ Update metadata logs
# ---------------------------------------------------------------------
def update_metadata(table_name, step, status=True, row_count=None):
    """Update metadata log tables with load status."""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    

    # Select metadata table
    meta_table = (
        get_log_tables("metadata",JSON_PATH)
        if any(x in table_name for x in ["base", "pr"])
        else get_log_tables("claimlog",JSON_PATH)
    )

    year_match = re.search(r'\d{4}', table_name)
    year = int(year_match.group(0)) if year_match else None

    if "base_" in table_name:
        table_rnk = 1
    elif "pr_" in table_name:
        table_rnk = 2
    else:
        table_rnk = 3

    status_val = "YES" if status in [True, "YES", "1"] else "NO"
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO "{LOG_SCHEMA}"."{meta_table}"
                (table_name, table_rnk, year, {step}, stage_count, last_updated_ts)
                VALUES (:table_name, :table_rnk, :year, :status, :row_count, :ts)
                ON CONFLICT (table_name)
                DO UPDATE SET
                    table_rnk = :table_rnk,
                    year = :year,
                    {step} = :status,
                    stage_count = :row_count,
                    last_updated_ts = :ts
            """
            ),
            {
                "table_name": table_name,
                "table_rnk": table_rnk,
                "year": year,
                "status": status_val,
                "row_count": row_count or 0,
                "ts": datetime.utcnow(),
            },
        )

# ---------------------------------------------------------------------
# 📥 Process and load file to Postgres
# ---------------------------------------------------------------------
def process_file_bytes_to_postgres(file_bytes, file_name, engine, schema_name):
    """Process and load a file from Azure Blob into PostgreSQL."""
    logging.info(f"[process_file_bytes_to_postgres] Processing file: {file_name}")

    try:
        if file_name.lower().endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_bytes))
            df_dict = {"Sheet1": df}
        else:
            df_dict = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None, engine="openpyxl")

        for sheet_name, df in df_dict.items():
            table_name = normalize_table_name(file_name, sheet_name)
            df = clean_column_names(df)
            df["last_runned_date"] = datetime.now()

            if df.empty:
                logging.warning(f"Skipping empty '{sheet_name}' in '{file_name}'.")
                continue

            dtype_mapping = {}
            for col in df.columns:
                col_dtype = df[col].dtype
                if pd.api.types.is_integer_dtype(col_dtype):
                    dtype_mapping[col] = Integer
                elif pd.api.types.is_float_dtype(col_dtype):
                    dtype_mapping[col] = Float
                elif pd.api.types.is_bool_dtype(col_dtype):
                    dtype_mapping[col] = TEXT
                elif pd.api.types.is_datetime64_any_dtype(col_dtype):
                    dtype_mapping[col] = DateTime
                else:
                    dtype_mapping[col] = TEXT

            meta_table = (
                get_log_tables("metadata",JSON_PATH)
                if any(x in file_name for x in ["base", "pr"])
                else get_log_tables("claimlog",JSON_PATH)
            )

            with engine.begin() as conn:
                result = conn.execute(
                    text(
                        f"""
                        SELECT stage_loaded
                        FROM "{LOG_SCHEMA}"."{meta_table}"
                        WHERE table_name = :table_name
                    """
                    ),
                    {"table_name": table_name},
                ).fetchone()

                if result and result[0] == "YES":
                    logging.info(f"Skipping {table_name}, already loaded.")
                    return

            df.to_sql(
                table_name,
                engine,
                schema=schema_name,
                if_exists="replace",
                index=False,
                dtype=dtype_mapping,
                chunksize=BATCH_SIZE,
                method="multi",
            )

            row_count = len(df)
            update_metadata(table_name, "stage_loaded", True, row_count)

    except Exception as e:
        logging.error(f"[process_file_bytes_to_postgres] Error processing {file_name}: {e}", exc_info=True)
        raise
# ---------------------------------------------------------------------
# ☁️ Batch process files from Azure Blob
# ---------------------------------------------------------------------
def batch_process_from_blob(**context):
    """Process multiple files from Azure Blob Storage."""
    logging.info("[batch_process_from_blob] Starting batch process.")

    azure_conn = BaseHook.get_connection(AZURE_BLOB_CONN_ID)
    azure_extra = json.loads(azure_conn.extra)
    schema_name = SCHEMA_NAME_VAR

    connection_string = azure_extra["connection_string"]
    container_name = azure_extra["container"]

    dag_run_conf = context.get("dag_run").conf or {}
    selected_files = dag_run_conf.get("selected_files")

    if not selected_files:
        selected_files = Variable.get("selected_files", default_var="[]")
        try:
            selected_files = json.loads(selected_files)
        except Exception:
            selected_files = []

    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service.get_container_client(container_name)

    all_blobs = [
        b for b in container_client.list_blobs()
        if b.name.lower().endswith((".csv", ".xlsx"))
    ]
    blob_list = (
        [b for b in all_blobs if b.name in selected_files]
        if selected_files else all_blobs
    )

    if not blob_list:
        raise Exception(f"No matching files found for: {selected_files}")

    errors = []
    for i, blob in enumerate(blob_list, start=1):
        file_name = blob.name
        try:
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            pg_url = pg_hook.get_uri()

            engine = create_engine(
                pg_url,
                pool_pre_ping=True,
                pool_recycle=1800,
                connect_args={
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                },
            )

            blob_client = container_client.get_blob_client(file_name)
            file_bytes = blob_client.download_blob().readall()

            logging.info(f"Processing file {i}/{len(blob_list)}: {file_name}")
            process_file_bytes_to_postgres(file_bytes, file_name, engine, schema_name)
            logging.info(f"✅ Successfully processed: {file_name}")

        except Exception as e:
            logging.error(f"❌ Error with {file_name}: {e}", exc_info=True)
            errors.append(f"{file_name}: {str(e)}")

    if errors:
        raise Exception(f"Some files failed: {errors}")

    logging.info("[batch_process_from_blob] ✅ All files processed.")


# ---------------------------------------------------------------------
# 🚀 Airflow Task Entry
# ---------------------------------------------------------------------
def load_data_to_postgres_stage(**context):
    """Entry task for Airflow DAG."""
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    batch_process_from_blob(**context)
    logging.info("Batch processing from Azure Blob completed successfully.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="azure_blob_to_postgres_etl_pg_hook_v3",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["azure", "postgres", "etl"],
) as dag:

    etl_task = PythonOperator(
        task_id="process_azure_blob_to_postgres",
        python_callable=load_data_to_postgres_stage,
        provide_context=True,
    )

    etl_task