from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import pandas as pd

from schema_table_config import get_schema, get_log_tables


# ---------------------------------------------------------
# Constants / Paths
# ---------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")

SOURCE_SCHEMA = get_schema("agg", META_JSON)
SOURCE_TABLE = "handled_bookedcase_base_pr"

TARGET_SCHEMA = get_schema("agg", META_JSON)
TARGET_TABLE = "mapping_old_policy"

POSTGRES_CONN_ID = "postgres_cloud_prochurn"

FEATURE_ENG_LOG = get_log_tables("featurelog", META_JSON)
LOG_SCHEMA = get_schema("log", META_JSON)


# ---------------------------------------------------------
# Metadata update
# ---------------------------------------------------------
def update_old_policy_metadata(engine):
    # count rows in final mapped table
    count_final = pd.read_sql(
        text(
            f'''SELECT COUNT(*) AS cnt 
                 FROM "{SOURCE_SCHEMA}"."{TARGET_TABLE}"'''
        ),
        con=engine,
    )["cnt"][0]

    sql = f"""
        UPDATE {LOG_SCHEMA}.{FEATURE_ENG_LOG}
        SET 
            is_old_policy_mapped = 'YES',
            old_policy_mapped_name = '{TARGET_SCHEMA}.{TARGET_TABLE}',
            count_old_policy = {count_final},
            timestamp = NOW()
        WHERE last_run_date = (
            SELECT last_run_date
            FROM {LOG_SCHEMA}.{FEATURE_ENG_LOG}
            WHERE is_old_policy_mapped = 'NO'
            ORDER BY timestamp DESC
            LIMIT 1
        );
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("✅ Metadata updated for old policy mapping.")


# ---------------------------------------------------------
# Old policy mapping
# ---------------------------------------------------------
def old_policy_mapping():
    sql = f""" CREATE TABLE {SOURCE_SCHEMA}.{TARGET_TABLE} AS
    SELECT *,
       CASE 
         WHEN "previous_policy" IS NULL
              AND LAG(upd_booked) OVER (
                    PARTITION BY "cleaned_chassis_number", 
                                 "cleaned_engine_number", 
                                 "corrected_name"
                    ORDER BY "policy_start_date"
                  ) IN ('1.0', '1')
              AND "policy_start_date" >= LAG("policy_end_date") OVER (
                    PARTITION BY "cleaned_chassis_number", 
                                 "cleaned_engine_number", 
                                 "corrected_name"
                    ORDER BY "policy_start_date"
                  ) + INTERVAL '1 day'
         THEN LAG("policy_no") OVER (
                PARTITION BY "cleaned_chassis_number", 
                             "cleaned_engine_number", 
                             "corrected_name"
                ORDER BY "policy_start_date"
              )
         ELSE "previous_policy"
       END AS updated_previous_policy
    FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
    ORDER BY "cleaned_chassis_number", "cleaned_engine_number", 
             "corrected_name", "policy_start_date";
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(
            text(f"DROP TABLE IF EXISTS {SOURCE_SCHEMA}.{TARGET_TABLE}")
        )
        conn.execute(text(sql))
        print(" old policy mapping query execution completed")

    # count the len of rows inserted
    with engine.begin() as conn:
        row_cnt = conn.execute(
            text(f"SELECT COUNT(*) FROM {SOURCE_SCHEMA}.{TARGET_TABLE}")
        ).scalar()

    print(
        f"number of rows inserted into {SOURCE_SCHEMA}.{TARGET_TABLE} is : "
        f"{row_cnt}"
    )
    print("✅ old policy mapping table created.")

    # ⭐ Update metadata
    update_old_policy_metadata(engine)


# default_args = {
#     "owner": "prochurn",
#     "retries": 0,
# }

# with DAG(
#     dag_id="old_policy_mapping",
#     default_args=default_args,
#     start_date=datetime(2025, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:

#     policy_mapping_task = PythonOperator(
#         task_id="Mapping_Old_Policy",
#         python_callable=old_policy_mapping,
#     )

#     policy_mapping_task
