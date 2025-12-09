from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from datetime import datetime

SOURCE_SCHEMA = "test_aggregation"
SOURCE_TABLE = "handled_bookedcase_base_pr"

TARGET_SCHEMA = "test_aggregation"
TARGET_TABLE = "mapoldpolicy_handled_bookedcase_base_pr"

POSTGRES_CONN_ID = "postgres_cloud_prochurn"

def old_policy_mapping():

    sql = f""" CREATE TABLE IF NOT EXISTS {SOURCE_SCHEMA}.{TARGET_TABLE} AS
    SELECT *,
       CASE 
         WHEN "previous_policy" IS NULL
              AND LAG(upd_booked) OVER (
                    PARTITION BY "cleaned_chassis_number", "cleaned_engine_number", "corrected_name"
                    ORDER BY "policy_start_date"
                  ) IN ('1.0', '1')
              AND "policy_start_date" >= LAG("policy_end_date") OVER (
                    PARTITION BY "cleaned_chassis_number", "cleaned_engine_number", "corrected_name"
                    ORDER BY "policy_start_date"
                  ) + INTERVAL '1 day'
         THEN LAG("policy_no") OVER (
                PARTITION BY "cleaned_chassis_number", "cleaned_engine_number", "corrected_name"
                ORDER BY "policy_start_date"
              )
         ELSE "previous_policy"
       END AS updated_previous_policy
    FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
    ORDER BY "cleaned_chassis_number", "cleaned_engine_number", "corrected_name", "policy_start_date";
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {SOURCE_SCHEMA}.{TARGET_TABLE}"))
        conn.execute(text(sql))
        print(f" old policy mapping query execution completed")
    # count the len of rows inserted 
    with engine.begin() as conn:
        row_cnt = conn.execute(text(f"SELECT COUNT(*) FROM {SOURCE_SCHEMA}.{TARGET_TABLE}")).scalar()
    print(f"number of rows inserted into {SOURCE_SCHEMA}.{TARGET_TABLE} is : {row_cnt}")

    print("✅ old policy mapping table created.")

default_args = {
    "owner": "prochurn",
    "retries": 0,
}

with DAG(
    dag_id="old_policy_mapping",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    policy_mapping_task = PythonOperator(
        task_id="Mapping_Old_Policy",
        python_callable=old_policy_mapping,
    )

    policy_mapping_task