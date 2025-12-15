from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import pandas as pd
import re

from schema_table_config import get_schema, get_log_tables


# ---------------------------------------------------------
# Constants / Paths
# ---------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
SOURCE_TABLE = "corrected_customer_name_fuzzy_match"

SOURCE_SCHEMA = get_schema("agg", META_JSON)
TARGET_SCHEMA = get_schema("agg", META_JSON)
LOG_SCHEMA = get_schema("log", META_JSON)

TARGET_TABLE_1 = "corrected_customer_name_fuzzy_match_column_modified"
TARGET_TABLE_2 = "handled_bookedcase_base_pr"

FEATURE_ENG_LOG = get_log_tables("featurelog", META_JSON)

# ---------------------------------------------------------
# UPDATE METADATA
# ---------------------------------------------------------


def update_booked_metadata_internal(engine):
    # count before trim → rows in TARGET_TABLE_1
    before_count = pd.read_sql(
        text(
            f'''SELECT COUNT(*) AS cnt 
                 FROM "{SOURCE_SCHEMA}"."{TARGET_TABLE_1}"'''
        ),
        con=engine,
    )["cnt"][0]

    # count after booked-case handling → rows in TARGET_TABLE_2
    after_count = pd.read_sql(
        text(
            f'''SELECT COUNT(*) AS cnt 
                 FROM "{SOURCE_SCHEMA}"."{TARGET_TABLE_2}"'''
        ),
        con=engine,
    )["cnt"][0]

    removed_count = before_count - after_count

    sql = f"""
       UPDATE {LOG_SCHEMA}.{FEATURE_ENG_LOG}
        SET 
            is_booked_cleaned = 'YES',
            booked_table_name = '{TARGET_TABLE_2}',
            booked_clean_cnt = {after_count},
            timestamp = NOW()
        WHERE last_run_date = (
            SELECT last_run_date
            FROM {LOG_SCHEMA}.{FEATURE_ENG_LOG}
            WHERE is_booked_cleaned = 'NO'
            ORDER BY timestamp DESC
            LIMIT 1
        );
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("✅ Metadata updated for booked case cleaning")


# ---------------------------------------------------------
# COLUMN LIST
# ---------------------------------------------------------


WANTED_COLUMNS = [
    "applicable_discount_with_ncb",
    "before_gst_add_on_gwp",
    "booked",
    "booked_date",
    "booked_month",
    "business_type",
    "chassis_engine_key",
    "chassis_no",
    "cleaned_branch_name_2",
    "cleaned_chassis_number",
    "cleaned_engine_number",
    "cleaned_insured_name",
    "cleaned_insured_name_filled",
    "cleaned_reg_no",
    "cleaned_state_2",
    "cleaned_zone_2",
    "corrected_name",
    "current_year_ncb_%",
    "data",
    "detariff_disc_amount",
    "engine_no",
    "fuel_type",
    "gst",
    "insured_name",
    "last_year_ncb",
    "manufacturer",
    "model",
    "model_variant",
    "model_with_fuel",
    "name_similarity",
    "ncb_amount",
    "net_premium",
    "new_branch_name_2",
    "new_vertical",
    "policy_end_date",
    "policy_issue_date",
    "policy_no",
    "policy_start_date",
    "policy_tenure(check)",
    "premium_after_discount",
    "previous_policy",
    "previous_year_ncb_amount",
    "previous_year_ncb_percentage",
    "product_name",
    "renewal_type",
    "rto_location",
    "state",
    "tie_up",
    "total_add-on_with_gst",
    "total_od_premium",
    "total_premium_payable",
    "total_tp_premium",
    "veh_reg_no",
    "vehicle_age",
    "vehicle_idv",
    "vehicle_segment",
    "zone",
]

COLUMN_LIST_SQL = ", ".join([f'"{col}"' for col in WANTED_COLUMNS])


# ---------------------------------------------------------
# TRIM COLUMNS
# ---------------------------------------------------------


def trim_columns():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    print("creating new table with wanted columns")
    sql = f"""
        
        DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{TARGET_TABLE_1}";

        
        CREATE TABLE "{TARGET_SCHEMA}"."{TARGET_TABLE_1}" AS
        SELECT {COLUMN_LIST_SQL}
        FROM "{SOURCE_SCHEMA}"."{SOURCE_TABLE}";
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(
        f"🎯 Trimmed table created with {len(WANTED_COLUMNS)} "
        f"columns → {TARGET_SCHEMA}.{TARGET_TABLE_1}"
    )


# ---------------------------------------------------------
# UPDATE BOOKED STATUS
# ---------------------------------------------------------


def update_booked_status():
    print("booked case handling started")
    sql = f"""
    CREATE TABLE IF NOT EXISTS {SOURCE_SCHEMA}.{TARGET_TABLE_2} AS
    WITH ordered AS (
      SELECT *,

        LEAD("policy_start_date") OVER (
          PARTITION BY "cleaned_chassis_number", 
                       "cleaned_engine_number", 
                       "corrected_name"
          ORDER BY "policy_start_date"
        ) AS next_policy_start_date

      FROM {SOURCE_SCHEMA}.{TARGET_TABLE_1}
    )
    SELECT *
    FROM (
      SELECT *,
        CASE 
          WHEN booked IS NULL
               AND next_policy_start_date IS NOT NULL
               AND next_policy_start_date >= "policy_end_date" + INTERVAL '1 day'
            THEN '1.0'
          WHEN booked IS NULL
               AND next_policy_start_date IS NULL
               AND "policy_end_date" >= '2025-01-01'
            THEN '2.0'
          WHEN booked IS NULL
               AND next_policy_start_date IS NULL
               AND "policy_end_date" < '2025-01-01'
            THEN '0.0'
          WHEN booked IS NULL
               AND next_policy_start_date IS NOT NULL
               AND "policy_end_date" < '2025-01-01'
            THEN '0.0'
          WHEN booked IS NULL
               AND next_policy_start_date IS NOT NULL
               AND "policy_end_date" >= '2025-01-01'
            THEN '2.0'
          ELSE booked::text
        END AS upd_booked
      FROM ordered
      ORDER BY 
          "cleaned_chassis_number",
          "cleaned_engine_number",
          "corrected_name",
          "policy_start_date",
          "policy_end_date"
    ) a;
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(
            text(
                f"DROP TABLE IF EXISTS {SOURCE_SCHEMA}.{TARGET_TABLE_2}"
            )
        )
        conn.execute(text(sql))

    print(
        f"✅ booked status updated table created on"
        f"{SOURCE_SCHEMA}.{TARGET_TABLE_2}"
    )
    update_booked_metadata_internal(engine)


# ---------------------------------------------------------
# (Optional) DAG (commented out in source)
# ---------------------------------------------------------
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2024, 11, 1),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=3),
# }
#
# with DAG(
#     dag_id="booked_cases_handling",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["booked", "liberty"],
# ) as dag:
#
#     trim_task = PythonOperator(
#         task_id="unwanted_column_removal",
#         python_callable=trim_columns,
#     )
#
#     booked_task = PythonOperator(
#         task_id="booked_cases",
#         python_callable=update_booked_status,
#         provide_context=True,
#     )
#
#     trim_task >> booked_task
