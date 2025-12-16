from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

import pandas as pd
import re
from fuzzywuzzy import fuzz

from schema_table_config import get_schema, get_log_tables


# ---------------------------------------------------------
# Constants / Paths
# ---------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")

POSTGRES_CONN_ID = "postgres_cloud_prochurn"

SOURCE_TABLE_1 = "overallcleaned_chessis_engine"
TARGET_TABLE_1 = "customer_null_case_handeling"
TARGET_TABLE_2 = "corrected_customer_name_fuzzy_match"

TARGET_SCHEMA = get_schema("agg", META_JSON)
SCHEMA = get_schema("agg", META_JSON)
LOG_SCHEMA = get_schema("log", META_JSON)
ARCHIVE_SCHEMA = get_schema("archive_log", META_JSON)

FEATURE_ENG_LOG = get_log_tables("featurelog", META_JSON)
NULL_LOG_TABLE = "corrected_name_null_log"
FUZZY_LOG_TABLE = "corrected_name_fuzzy_log"

OUTER_CHUNK = 10000
INNER_CHUNK = 5000


# ----------------------------------------------------------------------
# Archive old log tables
# ----------------------------------------------------------------------
def archive_log(log_table, **context):
    """
    Archive log_table safely. 
    Does NOT enforce schema because write_log() writes FULL rows from source.
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    today = datetime.utcnow().date()
    archive_table = f"{log_table}_{today:%Y%m%d}"

    with engine.begin() as conn:

     
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LOG_SCHEMA}"'))
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{ARCHIVE_SCHEMA}"'))

        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{LOG_SCHEMA}"."{log_table}" ();
        '''))
        
        count = conn.execute(
            text(f'SELECT COUNT(*) FROM "{LOG_SCHEMA}"."{log_table}"')
        ).scalar() or 0

        print(f"📝 Existing rows in {log_table} = {count}")

       
        if count > 0:
            conn.execute(text(f'''
                CREATE TABLE IF NOT EXISTS "{ARCHIVE_SCHEMA}"."{archive_table}" AS
                SELECT * FROM "{LOG_SCHEMA}"."{log_table}";
            '''))

            conn.execute(text(
                f'TRUNCATE TABLE "{LOG_SCHEMA}"."{log_table}"'
            ))

            print(f"📦 Archived {count} rows → {ARCHIVE_SCHEMA}.{archive_table}")

        else:
            print(f"ℹ No rows to archive for {log_table}")


# ----------------------------------------------------------------------
# Load Data To Postgres (chunked)
# ----------------------------------------------------------------------
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


# ---------------------------------------------------------
# Removed rows Log
# ---------------------------------------------------------
def write_log(df, log_table):
    """
    Generic log writer: appends DataFrame to given log table.
    """
    if df.empty:
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        df.to_sql(
            name=log_table,
            schema=LOG_SCHEMA,
            con=conn,
            if_exists="append",
            index=False,
        )
    print(f"📝 Logged {len(df)} rows → {LOG_SCHEMA}.{log_table}")


# ---------------------------------------------------------
# Update Meta Log
# ---------------------------------------------------------
def update_corrected_name_metadata(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    before_count = pd.read_sql(
        text(
            f'SELECT COUNT(*) AS cnt FROM "{SCHEMA}"."{SOURCE_TABLE_1}"'
        ),
        con=engine,
    )["cnt"][0]

    after_count = pd.read_sql(
        text(
            f'SELECT COUNT(*) AS cnt FROM "{SCHEMA}"."{TARGET_TABLE_2}"'
        ),
        con=engine,
    )["cnt"][0]

    removed_count = before_count - after_count

    with engine.begin() as conn:
        conn.execute(
            text(
                f'''
            UPDATE {LOG_SCHEMA}.{FEATURE_ENG_LOG}
            SET 
                is_inusred_name_corrected = 'YES',
                corrected_table_name = '{TARGET_TABLE_2}',
                corrected_name_cnt = {after_count},
                corrected_name_removed_cnt = {removed_count},
                timestamp = NOW()
            WHERE last_run_date = (
                SELECT last_run_date
                FROM {LOG_SCHEMA}.{FEATURE_ENG_LOG}
                WHERE is_inusred_name_corrected = 'NO'
                ORDER BY timestamp DESC
                LIMIT 1
            );
            '''
            )
        )


# ---------------------------------------------------------
# Correct null in customer name (SQL)
# ---------------------------------------------------------
SQL_QUERY = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TARGET_TABLE_1} AS
SELECT 
    a.*,
    CASE 
        WHEN (a."cleaned_insured_name" IS NULL 
              OR a."cleaned_insured_name" = '' 
              OR lower(a."cleaned_insured_name") = 'none')
        THEN b.lookup_name
        ELSE a."cleaned_insured_name"
    END AS "cleaned_insured_name_filled"
FROM {SCHEMA}.{SOURCE_TABLE_1} a
LEFT JOIN (
    SELECT 
        "policy_no", 
        MAX("cleaned_insured_name") AS lookup_name
    FROM {SCHEMA}.{SOURCE_TABLE_1}
    WHERE "cleaned_insured_name" IS NOT NULL 
      AND "cleaned_insured_name" <> '' 
      AND lower("cleaned_insured_name") <> 'none'
    GROUP BY "policy_no"
) b ON a."previous_policy" = b."policy_no";
"""


def corrected_name_null_cases(**context):
    print("▶ customer null case handling started")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(
            text(
                f'DROP TABLE IF EXISTS "{LOG_SCHEMA}"."corrected_name_null_log"'
            )
        )
        print(
            "droped the corrected_name_removal_reason_lib to create "
            "with new LOG TABLE"
        )

        conn.execute(
            text(f'DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{TARGET_TABLE_1}"')
        )
        print(f"droped the {TARGET_TABLE_1} to create with new data")
        conn.execute(text(SQL_QUERY))
        print("✅ Table created/updated.")

    print("🎉 Process completed.")

    removal_logs = []

    removed_df = pd.read_sql(
        text(
            f'''
            SELECT *
            FROM "{SCHEMA}"."{SOURCE_TABLE_1}"
            WHERE "cleaned_insured_name" IS NULL
            OR "cleaned_insured_name" = ''
            OR lower("cleaned_insured_name") = 'none'
            '''
        ),
        con=engine,
    )

    if not removed_df.empty:
        removed_df["removal_reason"] = (
            "Null/Empty insured name corrected using lookup"
        )
        removal_logs.append(removed_df)

    if removal_logs:
        log_df = pd.concat(removal_logs, ignore_index=True)
        write_log(log_df, NULL_LOG_TABLE)


# ---------------------------------------------------------
# Cleaning corrected customer names (fuzzy)
# ---------------------------------------------------------
def clean_corrected_name_fuzzy(**context):
    print("\n==============================")
    print(f"▶ Cleaning: {TARGET_TABLE_1}")
    print("==============================\n")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    read_engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        text(f'SELECT * FROM "{SCHEMA}"."{TARGET_TABLE_1}"'),
        con=read_engine,
    )

    print(f"📊 Loaded rows = {len(df)}")
    if df.empty:
        print("⚠ No data found — skipping.")
        return

    df["chassis_engine_key"] = (
        df["cleaned_chassis_number"].astype(str)
        + "_"
        + df["cleaned_engine_number"].astype(str)
    )

    df.sort_values(
        ["chassis_engine_key", "policy_start_date"],
        inplace=True,
    )

    prev_name = None
    prev_chassis = None

    corrected_names = []
    similarity_scores = []

    for _, row in df.iterrows():
        current_name = row["cleaned_insured_name_filled"]
        chassis_engine_key = row["chassis_engine_key"]

        if pd.isnull(current_name):
            current_name = ""

        if prev_name is not None and prev_chassis == chassis_engine_key:
            similarity = fuzz.ratio(prev_name, current_name)
            if similarity >= 80:
                corrected_names.append(prev_name)
            else:
                corrected_names.append(current_name)
        else:
            corrected_names.append(current_name)

        similarity_scores.append(
            fuzz.ratio(corrected_names[-1], current_name)
        )

        prev_name = corrected_names[-1]
        prev_chassis = chassis_engine_key

    df["corrected_name"] = corrected_names
    df["name_similarity"] = similarity_scores

    removal_logs = []

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    print("📝 Normalized column names in final_df")

    load_chunked(df, TARGET_TABLE_2, SCHEMA)

    changed_rows = df[
        df["corrected_name"] != df["cleaned_insured_name_filled"]
    ].copy()

    if not changed_rows.empty:
        changed_rows["removal_reason"] = (
            "Corrected by fuzzy-matching (similarity < 80 "
            "threshold logic)"
        )
        removal_logs.append(changed_rows)

    if removal_logs:
        log_df = pd.concat(removal_logs, ignore_index=True)
        write_log(log_df, FUZZY_LOG_TABLE)

    update_corrected_name_metadata()

    print(
        "🎉 Completed cleaning for correct insured names"
    )


# --------------------------------------------------------------------
# (Optional) DAG (commented out in source)
# --------------------------------------------------------------------
# default_args = {
#     "owner": "airflow",
#     "start_date": datetime(2024, 11, 1),
#     "retries": 0,
#     "retry_delay": timedelta(minutes=2),
# }
#
# with DAG(
#     dag_id="corrected_names_cleaning",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["cleanup", "chassis_engine"],
# ) as dag:
#     archive_null_log_task = PythonOperator(
#         task_id="archive_null_log",
#         python_callable=archive_log,
#         op_kwargs={"log_table": "corrected_name_null_log"},
#         provide_context=True,
#     )
#
#     archive_fuzzy_log_task = PythonOperator(
#         task_id="archive_fuzzy_log",
#         python_callable=archive_log,
#         op_kwargs={"log_table": "corrected_name_fuzzy_log"},
#         provide_context=True,
#     )
#
#     null_case_task = PythonOperator(
#         task_id="null_case_handeling",
#         python_callable=corrected_name_null_cases,
#         provide_context=True,
#     )
#
#     furzzy_task = PythonOperator(
#         task_id="furzzy_match",
#         python_callable=clean_corrected_name_fuzzy,
#         provide_context=True,
#     )
#
#     null_case_task >> furzzy_task
