from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import sys
import json
import os
import io
import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy.types import TEXT, Integer, Float, DateTime, TEXT
from cryptography.fernet import Fernet
import base64
from azure.storage.blob import BlobServiceClient
import re   

# Constants
POSTGRES_CONN_ID = "postgres_cloud_prochurn"
AZURE_BLOB_CONN_ID = "azure_blob"
SCHEMA_NAME_VAR = "pip_stage"
BATCH_SIZE = 1000  # Reduced to prevent long transaction timeout
LOG_SCHEMA = "pip_log"
# META_TABLE = "etl_metadata_logs"

def clean_column_names(df):
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.replace(" ", "_", regex=True)
        .str.replace("[()\[\]{}]", "", regex=True)
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

# ✅ Ensure claim_logs table exists with proper columns
def ensure_claim_log_table(engine):
    query = f"""
        CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.claim_logs (
        table_name TEXT PRIMARY KEY,
        table_rnk INTEGER,
        year INTEGER,
        stage_loaded TEXT DEFAULT 'NO',
        stage_count BIGINT DEFAULT 0,
        is_appended TEXT DEFAULT 'NO',
        appended_count integer,
        is_merged TEXT DEFAULT 'NO',
        merged_count integer,
        is_basepr_claim_merged TEXT DEFAULT 'NO',
        cnt integer,
        last_updated_ts TIMESTAMP DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(query))

def ensure_meta_log_table(engine):
    query = f"""
            CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};
            CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.etl_metadata_logs (
            table_name TEXT PRIMARY KEY,
            table_rnk integer,
            year integer,  -- Added year column
            stage_loaded TEXT DEFAULT 'NO',
            stage_count BIGINT DEFAULT 0,
            is_base_cleaned TEXT DEFAULT 'NO',
            dwh_loaded_cnt_base BIGINT DEFAULT 0,
            is_pr_cleaned TEXT DEFAULT 'NO',
            dwh_loaded_cnt_pr BIGINT DEFAULT 0,
            is_basepr_appended TEXT DEFAULT 'NO',
            basepr_count TEXT DEFAULT 'NO',
            appended_table_name TEXT,
            renewal_policy_table TEXT,
            renewal_policy_count BIGINT DEFAULT 0,
            last_updated_ts TIMESTAMP DEFAULT now()
            );
    """
    with engine.begin() as conn:
        conn.execute(text(query))

def normalize_table_name(file_name: str, sheet_name: str = "Sheet1"):
    # Always prefer file name
    name = file_name.split('.')[0].lower().replace('-', '_').replace(' ', '_')
    year_match = re.search(r'(?<!\d)(\d{2,4})(?!\d)', name)
    year = year_match.group(1) if year_match else 'unknown'
    if len(year) == 2:
        year = '20' + year

    if 'base' in name:
        return f'base_{year}'
    elif 'pr' in name:
        return f'pr_{year}'
    elif 'claim' in name:
        part_match = re.search(r'part[_]?(\d+)', name)
        part = part_match.group(1) if part_match else '1'
        return f'claim_{year}_part_{part}'
    else:
        return f'unknown_{year}'
    
def update_metadata(table_name, step, status=True, row_count=None):
    pg_hook = PostgresHook(postgres_conn_id="postgres_cloud_prochurn")
    engine = pg_hook.get_sqlalchemy_engine()
    ensure_claim_log_table(engine)
    # Check if file is 'base' or 'pr'
    if 'base' in table_name or 'pr' in table_name:
        meta_table = "etl_metadata_logs"  # Metadata table for base or pr
    else:
        meta_table = "claim_logs"  # Use claim_logs for others

    # Extract the year from the table_name (e.g., base_2022 -> 2022, pr_2023 -> 2023)
    year_match = re.search(r'\d{4}', table_name)  # Find a 4-digit year in the table name
    year = int(year_match.group(0)) if year_match else None
    
    # Check if table is base or pr and assign rank accordingly
    if 'base_' in table_name:
        table_rnk = 1  # Rank 1 for base tables
    elif 'pr_' in table_name:
        table_rnk = 2  # Rank 2 for pr tables
    else:
        table_rnk = 3  # Default rank for any other tables

    # Create schema and metadata table if not exists
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};
            CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.{meta_table} (
                table_name TEXT PRIMARY KEY,
                table_rnk integer,
                year integer,  -- Added year column
                stage_loaded TEXT DEFAULT 'NO',
                stage_count BIGINT DEFAULT 0,
                is_base_cleaned TEXT DEFAULT 'NO',
                dwh_loaded_cnt_base BIGINT DEFAULT 0,
                is_pr_cleaned TEXT DEFAULT 'NO',
                dwh_loaded_cnt_pr BIGINT DEFAULT 0,
                is_basepr_appended TEXT DEFAULT 'NO',
                basepr_count TEXT DEFAULT 'NO',
                appended_table_name TEXT,
                renewal_policy_table TEXT,
                renewal_policy_count BIGINT DEFAULT 0,
                last_updated_ts TIMESTAMP DEFAULT now()
            );
        """))
        
        # Convert True/False to YES/NO
        status_val = "YES" if status in [True, "YES", "1"] else "NO"
        
        # Insert or update metadata
        conn.execute(text(f"""
            INSERT INTO {LOG_SCHEMA}.{meta_table} (table_name, table_rnk, year, {step}, stage_count, last_updated_ts)
            VALUES (:table_name, :table_rnk, :year, :status, :row_count, :ts)
            ON CONFLICT (table_name)
            DO UPDATE SET table_rnk = :table_rnk,
                          year = :year,
                          {step} = :status,
                          stage_count = :row_count,
                          last_updated_ts = :ts
        """), {
            "table_name": table_name,
            "table_rnk": table_rnk,
            "year": year,  # Add year here
            "status": status_val,
            "row_count": row_count if row_count is not None else 0,
            "ts": datetime.utcnow()
        })


def process_file_bytes_to_postgres(file_bytes, file_name, engine, schema_name, fernet=None, columns_to_encrypt=None):
    logging.info(f"[process_file_bytes_to_postgres] Processing file: {file_name}")
    try:
        if file_name.lower().endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_bytes))
            df_dict = {"Sheet1": df}
        elif file_name.lower().endswith(".xlsb"):
            try:
                import pyxlsb  # noqa: F401
            except ImportError:
                msg = "Missing optional dependency 'pyxlsb'. Use pip or conda to install pyxlsb."
                logging.error(msg)
                raise ImportError(msg)
            df_dict = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None, engine="pyxlsb")
        else:
            try:
                import openpyxl  # noqa: F401
            except ImportError:
                msg = "Missing optional dependency 'openpyxl'. Use pip or conda to install openpyxl."
                logging.error(msg)
                raise ImportError(msg)
            df_dict = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None, engine="openpyxl")

        for sheet_name, df in df_dict.items():
            table_name = normalize_table_name(file_name,sheet_name)

            df = clean_column_names(df)
            # df = df.head(1000)
            logging.info(f"[process_file_bytes_to_postgres] Columns detected in '{sheet_name}': {list(df.columns)}")

            if df.empty:
                logging.warning(f"[process_file_bytes_to_postgres] Skipping empty '{sheet_name}' in '{file_name}'.")
                continue

            # if columns_to_encrypt and fernet:
            #     df = encrypt_columns(df, columns_to_encrypt, fernet)

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

            logging.info(f"[process_file_bytes_to_postgres] Uploading '{sheet_name}' to table '{schema_name}.{table_name}' in batches of {BATCH_SIZE}...")
            ensure_claim_log_table(engine)
            ensure_meta_log_table(engine)
            # Decide metadata table name (base/pr → etl_metadata_logs, else → claim_logs)
            if 'base' in file_name or 'pr' in file_name:
                meta_table = "etl_metadata_logs"
            else:
                meta_table = "claim_logs"

            with engine.begin() as conn:
                # Check if table already stage_loaded = YES
                result = conn.execute(text(f"""
                    SELECT stage_loaded 
                    FROM {LOG_SCHEMA}.{meta_table}
                    WHERE table_name = :table_name
                """), {"table_name": table_name}).fetchone()

                if result and result[0] == "YES":
                    logging.info(f"[process_file_bytes_to_postgres] Skipping {table_name}, already stage_loaded=YES.")
                    return
                
            with engine.begin() as connection:
                df.to_sql(
                    table_name,
                    connection,
                    schema=schema_name,
                    if_exists="replace",
                    index=False,
                    dtype=dtype_mapping,
                    chunksize=BATCH_SIZE,
                    method="multi"
                )

            logging.info(f"[process_file_bytes_to_postgres] Table '{schema_name}.{table_name}' created/updated with {len(df)} rows.")
            row_count = len(df)
            logging.info(f"[process_file_bytes_to_postgres] Table '{schema_name}.{table_name}' created/updated with {row_count} rows.")
            
            # Decide which metadata table to update based on file name
            if 'base' in file_name or 'pr' in file_name:
                update_metadata(table_name, "stage_loaded", True, row_count=row_count)
            else:
                update_metadata(table_name, "stage_loaded", True, row_count=row_count)
            

    except Exception as e:
        logging.error(f"[process_file_bytes_to_postgres] Error processing file {file_name}: {e}", exc_info=True)
        raise


def batch_process_from_blob(**context):
    logging.info("[batch_process_from_blob] Starting batch process.")

    azure_conn = BaseHook.get_connection(AZURE_BLOB_CONN_ID)
    azure_extra = json.loads(azure_conn.extra)
    SCHEMA_NAME = Variable.get(SCHEMA_NAME_VAR, default_var= "pip_stage")


    AZURE_STORAGE_CONNECTION_STRING = azure_extra["connection_string"]
    AZURE_BLOB_CONTAINER = azure_extra["container"]
   

    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(AZURE_BLOB_CONTAINER)
    blob_list = [blob for blob in container_client.list_blobs() if blob.name.lower().endswith((".csv", ".xlsx", ".xlsb"))]
    total_files = len(blob_list)
    logging.info(f"[batch_process_from_blob] Found {total_files} files to process in Azure Blob Storage.")

    errors = []

    for i, blob in enumerate(blob_list, start=1):
        file_name = blob.name
        try:
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            pg_url = pg_hook.get_uri()

            # 🔥 Improved engine with keepalive and reconnect settings
            engine = create_engine(
                pg_url,
                pool_pre_ping=True,
                pool_recycle=1800,
                connect_args={"keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 5}
            )

            with engine.connect() as conn:
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")
                logging.info(f"[batch_process_from_blob] Ensured schema '{SCHEMA_NAME}' exists.")

            blob_client = container_client.get_blob_client(file_name)
            file_bytes = blob_client.download_blob().readall()
            logging.info(f"[batch_process_from_blob] Processing file {i} of {total_files}: {file_name}")

            process_file_bytes_to_postgres(
                file_bytes, file_name, engine, SCHEMA_NAME, fernet=None, columns_to_encrypt=None
            )

            logging.info(f"[batch_process_from_blob] Finished processing file {i} of {total_files}: {file_name}")

        except Exception as e:
            logging.error(f"[batch_process_from_blob] Error during processing of {file_name}: {e}", exc_info=True)
            errors.append(f"{file_name}: {str(e)}")

    if errors:
        raise Exception(f"Some files failed to load: {errors}")


def load_data_to_postgres_stage(**context):
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    batch_process_from_blob(**context)
    logging.info("[azure_blob_to_postgres_task] Batch processing from Azure Blob Storage completed successfully.")


# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2024, 6, 1),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="azure_blob_to_postgres_etl_pg_hook_v3",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["azure", "postgres", "etl"],
# ) as dag:

#     etl_task = PythonOperator(
#         task_id="process_azure_blob_to_postgres",
#         python_callable=load_data_to_postgres_stage,
#         provide_context=True,
#     )

#     etl_task