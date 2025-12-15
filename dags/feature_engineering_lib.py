from datetime import datetime, timedelta
from pathlib import Path
import re

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import pandas as pd

from schema_table_config import (
    get_log_tables,
    get_schema,
    get_column_mapping,
)


# ---------------------------------------------------------------------
# Config / constants
# ---------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
SOURCE_TABLE = "overall_appended_basepr_claim"
SOURCE_SCHEMA = get_schema("agg", META_JSON)
LOG_SCHEMA = get_schema("log", META_JSON)

TARGET_SCHEMA = get_schema("agg", META_JSON)
TARGET_TABLE_1 = "anomalies_detection_on_overall_appended_baseprclaim"
TARGET_TABLE_2 = "policy_chain_feature"

OUTER_CHUNK = 10000
INNER_CHUNK = 5000

COLUMN_JSON = str(DAGS_DIR / "config" / "column_mapping.json")
ZONE_TABLE = get_column_mapping("zonemapping", COLUMN_JSON)
FEATURE_ENG_LOG = get_log_tables("featurelog", META_JSON)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
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


def update_feature_eng_metadata(engine):
    before_count = pd.read_sql(
        text(
            f'''SELECT COUNT(*) AS cnt 
                 FROM "{TARGET_SCHEMA}"."{TARGET_TABLE_1}"'''
        ),
        con=engine,
    )["cnt"][0]

    after_count = pd.read_sql(
        text(
            f'''SELECT COUNT(*) AS cnt 
                 FROM "{TARGET_SCHEMA}"."{TARGET_TABLE_2}"'''
        ),
        con=engine,
    )["cnt"][0]

    print(f"before={before_count}, after={after_count}")

    sql = f"""
        UPDATE {LOG_SCHEMA}.{FEATURE_ENG_LOG}
        SET 
            feature_eng_completed = 'YES',
            feature_eng_name = '{TARGET_SCHEMA}.{TARGET_TABLE_2}',
            feature_eng_cnt = {after_count},
            timestamp = NOW()
        WHERE last_run_date = (
            SELECT last_run_date
            FROM {LOG_SCHEMA}.{FEATURE_ENG_LOG}
            WHERE feature_eng_completed = 'NO'
            ORDER BY timestamp DESC
            LIMIT 1
        );
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("✅ Metadata updated for feature engineering.")


# ---------------------------------------------------------------------
# Main: anomalies / tenure / churn / new-customer logic
# ---------------------------------------------------------------------
def anomalies_claimerge_external_factors():
    print("🔗 Connecting to PostgreSQL...")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    query = f'SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE};'
    df = pd.read_sql(query, con=engine)
    print(f"fetched {len(df)} from {SOURCE_TABLE}")

    df["customerid_base"] = (
        df["corrected_name"].astype(str)
        + "_"
        + df["cleaned_branch_name_2"].astype(str)
    )
    df["customerid"] = (
        df.groupby("customerid_base").ngroup() + 1000001
    ).astype(str)

    print("created customer id")

    df["policy_start_date"] = pd.to_datetime(
        df["policy_start_date"], errors="coerce"
    )
    df["policy_end_date"] = pd.to_datetime(
        df["policy_end_date"], errors="coerce"
    )

    df["zone"] = df.apply(
        lambda row: ZONE_TABLE.get(str(row["state"]).upper(), row["zone"])
        if pd.isna(row["zone"])
        else row["zone"],
        axis=1,
    )
    print(f"✅ Applied zone mapping from {ZONE_TABLE}")

    df["Policy Status"] = df["upd_booked"].apply(
        lambda x: (
            "Renewed"
            if x in ["1", "1.0"]
            else "Not Renewed"
            if x in ["0", "0.0"]
            else "Open"
            if x == "2.0"
            else "Unknown"
        )
    )

    df["Policy Tenure Month"] = (
        (df["policy_end_date"].dt.year - df["policy_start_date"].dt.year)
        * 12
        + (df["policy_end_date"].dt.month - df["policy_start_date"].dt.month)
    )
    print("policy tenure (Months) calculation done")

    df["Policy Tenure"] = (df["Policy Tenure Month"] / 12).round(0)
    df["Start Year"] = df["policy_start_date"].dt.year
    df["End Year"] = df["policy_end_date"].dt.year

    yearly_tenure = (
        df.groupby(["customerid", "Start Year"])
        .agg(
            {
                "policy_start_date": "min",
                "policy_end_date": "max",
            }
        )
        .reset_index()
    )

    yearly_tenure["Yearly Tenure (Months)"] = (
        (yearly_tenure["policy_end_date"].dt.year
         - yearly_tenure["policy_start_date"].dt.year)
        * 12
        + (yearly_tenure["policy_end_date"].dt.month
           - yearly_tenure["policy_start_date"].dt.month)
    )
    print("policy tenure (year) calculation done")

    yearly_tenure["Cumulative Tenure (Months)"] = (
        yearly_tenure.groupby("customerid")[
            "Yearly Tenure (Months)"
        ].cumsum()
    )

    yearly_tenure["Tenure Decimal"] = (
        yearly_tenure["Cumulative Tenure (Months)"] / 12
    )
    yearly_tenure["Customer Tenure"] = (
        yearly_tenure["Tenure Decimal"].round(0)
    )

    tenure_mapping = yearly_tenure[
        [
            "customerid",
            "Start Year",
            "Cumulative Tenure (Months)",
            "Tenure Decimal",
            "Customer Tenure",
        ]
    ]
    print("policy tenure mapping done")

    df = df.merge(
        tenure_mapping,
        on=["customerid", "Start Year"],
        how="left",
    )

    df["FirstPolicyYear"] = df.groupby("customerid")["Start Year"].transform(
        "min"
    )
    df["New_Customer_ID"] = df.apply(
        lambda row: f"{row['FirstPolicyYear']}_{row['customerid']}"
        if row["Start Year"] == row["FirstPolicyYear"]
        else "",
        axis=1,
    )
    df["New Customers"] = df["New_Customer_ID"].apply(
        lambda x: "Yes" if x else "No"
    )
    print("adding new customer column done")

    def calculate_churn_status(group):
        unique_statuses = group.unique()
        if len(unique_statuses) == 1 and unique_statuses[0] == "Not Renewed":
            return "Yes"
        return "No"

    df["Churn Label"] = df.groupby(["customerid", "End Year"])[
        "Policy Status"
    ].transform(lambda x: calculate_churn_status(x))
    print("churn label done")

    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_")
    )

    print("📝 Normalized column names in final_df")
    load_chunked(df, TARGET_TABLE_1, TARGET_SCHEMA)
    print(f"🎉 Completed cleaning for {SOURCE_SCHEMA}.{TARGET_TABLE_1}\n")

    print("🛠 Adding & updating 'claim_happened_not' column...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    alter_sql = f"""
    ALTER TABLE {SOURCE_SCHEMA}.{TARGET_TABLE_1}
    ADD COLUMN IF NOT EXISTS claim_happened_not VARCHAR;
    """

    update_sql = f"""
    UPDATE {SOURCE_SCHEMA}.{TARGET_TABLE_1}
    SET claim_happened_not = CASE 
        WHEN claim_no IS NULL THEN 'No'
        ELSE 'Yes'
    END;
    """

    with engine.begin() as conn:
        conn.execute(text(alter_sql))
        conn.execute(text(update_sql))

    print("✅ Column 'claim_happened_not' added & updated successfully.")


# ---------------------------------------------------------------------
# Policy chain / feature engineering (SQL heavy)
# ---------------------------------------------------------------------
def add_policy_chain_features():
    print("🔗 Connecting to PostgreSQL...")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    sql = f"""
    WITH base AS (
      SELECT *,
             CASE 
               WHEN LAG(policy_start_date) OVER (
                      PARTITION BY cleaned_chassis_number, cleaned_engine_number, corrected_name
                      ORDER BY policy_start_date
                    ) IS NULL 
               THEN 1
               WHEN NOT (
                      LAG(upd_booked) OVER (
                        PARTITION BY cleaned_chassis_number, cleaned_engine_number, corrected_name
                        ORDER BY policy_start_date
                      ) IN ('1.0', '1')
                      AND policy_start_date >= LAG(policy_end_date) OVER (
                        PARTITION BY cleaned_chassis_number, cleaned_engine_number, corrected_name
                        ORDER BY policy_start_date
                      ) + INTERVAL '1 day'
                    )
               THEN 1
               ELSE 0
             END AS new_chain_flag
      FROM {SOURCE_SCHEMA}.{TARGET_TABLE_1}
    ),
    grouped AS (
      SELECT *,
             SUM(new_chain_flag) OVER (
               PARTITION BY cleaned_chassis_number, cleaned_engine_number, corrected_name
               ORDER BY policy_start_date
               ROWS UNBOUNDED PRECEDING
             ) AS chain_group
      FROM base
    ),
    first_policy AS (
      SELECT *,
             FIRST_VALUE(policy_no) OVER (
               PARTITION BY cleaned_chassis_number, cleaned_engine_number, corrected_name, chain_group
               ORDER BY policy_start_date
             ) AS first_initial_policy_no
      FROM grouped
    )
    SELECT *
    FROM (
      SELECT 
        *,

        ROW_NUMBER() OVER (
          PARTITION BY cleaned_chassis_number, cleaned_engine_number, corrected_name, chain_group
          ORDER BY policy_start_date
        ) AS policy_wise_purchase
      FROM first_policy
    ) AS final_result
    ORDER BY cleaned_chassis_number, cleaned_engine_number, corrected_name, policy_start_date;
    """

    print("🚀 Executing SQL to generate enriched dataset...")

    df = pd.read_sql(sql, con=engine)
    print(f"📥 Loaded {len(df)} rows with new chain columns")

    print("📝 Writing updated data back into overall_cleaned_base_and_pr_ef...")
    load_chunked(df, TARGET_TABLE_2, TARGET_SCHEMA)

    print("🎉 Successfully updated policy-chain feature table:")
    print("    • new_chain_flag")
    print("    • chain_group")
    print("    • first_initial_policy_no")
    print("    • policy_wise_purchase")

    update_feature_eng_metadata(engine)


# ---------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2024, 11, 1),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=3),
# }

# with DAG(
#     dag_id="external_factors_addon",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["external", "liberty"],
# ) as dag:

#     anomali_task = PythonOperator(
#         task_id="external_factors_addons",
#         python_callable=anomalies_claimerge_external_factors,
#     )

#     sql_task = PythonOperator(
#         task_id="sql_column_addon",
#         python_callable=add_policy_chain_features,
#     )

#     anomali_task >> sql_task
