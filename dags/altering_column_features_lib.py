from datetime import datetime
import time
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

from schema_table_config import (
    get_schema)


# --------------------------------------------------------------------
# Config / constants
# --------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
TABLE = "policy_chain_feature"
SCHEMA = get_schema("agg", META_JSON)

OUTER_CHUNK = 10000
INNER_CHUNK = 5000


# --------------------------------------------------------------------
# Load Data To Postgres (chunked)
# --------------------------------------------------------------------
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


# --------------------------------------------------------------------
# Simple step logger
# --------------------------------------------------------------------
def step_log(label, func):
    print("\n-----------------------------")
    print(f"⏳ STARTING: {label}")
    start = time.time()

    func()

    end = time.time()
    elapsed = int((end - start) // 60)
    sec = int((end - start) % 60)
    print(
        f"✅ COMPLETED: {label} in {elapsed} min {sec} sec"
    )
    print("-----------------------------\n")


# --------------------------------------------------------------------
# CLEAN + RELOAD
# --------------------------------------------------------------------
def convert_and_reload():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    print("\n🚀 Loading table from Postgres...")
    df = pd.read_sql(f'SELECT * FROM {SCHEMA}.{TABLE}', engine)
    print(f"🔹 Loaded {len(df)} rows\n")

    # ----------------------------------------------------------
    # cleaned_new_vertical
    # ----------------------------------------------------------
    def clean_new_vertical():
        print("   Cleaning using REGEXP + LOWER...")
        df["cleaned_new_vertical"] = (
            df["new_vertical"]
            .astype(str)
            .str.replace(r"[^a-zA-Z0-9]", "", regex=True)
            .str.lower()
        )
        print(f"   Sample before: {df['new_vertical'].iloc[0]}")
        print(
            f"   Sample after:  "
            f"{df['cleaned_new_vertical'].iloc[0]}"
        )

    step_log("cleaned_new_vertical", clean_new_vertical)

    # ----------------------------------------------------------
    # tie_up
    # ----------------------------------------------------------
    def clean_tie_up():
        before = df["tie_up"].isna().sum()
        df["tie_up"] = df["tie_up"].fillna("Non-OEM")
        after = df["tie_up"].isna().sum()
        print(f"   NULL before: {before}")
        print(f"   NULL after:  {after}")

    step_log("tie_up cleanup", clean_tie_up)

    # ----------------------------------------------------------
    # fuel_type
    # ----------------------------------------------------------
    def clean_fuel_type():
        before_blank = (df["fuel_type"].isin(["-", "(blank)"])).sum()
        before_null = df["fuel_type"].isna().sum()

        df["fuel_type"] = df["fuel_type"].replace(
            ["-", "(blank)"], pd.NA
        )
        df["fuel_type"] = df["fuel_type"].fillna("Petrol")

        after_null = df["fuel_type"].isna().sum()
        print(f"   '-' or '(blank)' count: {before_blank}")
        print(f"   NULL before: {before_null}")
        print(f"   NULL after: {after_null}")

    step_log("fuel_type cleanup", clean_fuel_type)

    # ----------------------------------------------------------
    # vehicle_idv
    # ----------------------------------------------------------
    def clean_vehicle_idv():
        before_invalid = (df["vehicle_idv"] == "(blank)").sum()

        df["vehicle_idv"] = df["vehicle_idv"].replace(
            "(blank)", None
        )
        df["vehicle_idv"] = pd.to_numeric(
            df["vehicle_idv"], errors="coerce"
        )
        df["vehicle_idv"] = df["vehicle_idv"].fillna(0)
        df["vehicle_idv"] = df["vehicle_idv"].round().astype(float)

        null_after = df["vehicle_idv"].isna().sum()

        print(f"   '(blank)' rows: {before_invalid}")
        print(f"   NULL after conversion: {null_after}")

    step_log("vehicle_idv cleanup", clean_vehicle_idv)

    # ----------------------------------------------------------
    # previous_year_ncb_percentage
    # ----------------------------------------------------------
    def clean_previous_ncb():
        before_blank = (
            df["previous_year_ncb_percentage"] == "(blank)"
        ).sum()

        df["previous_year_ncb_percentage"] = df[
            "previous_year_ncb_percentage"
        ].replace("(blank)", None)
        df["previous_year_ncb_percentage"] = pd.to_numeric(
            df["previous_year_ncb_percentage"], errors="coerce"
        )
        df["previous_year_ncb_percentage"] = (
            df["previous_year_ncb_percentage"].fillna(0)
        )
        df["previous_year_ncb_percentage"] = (
            df["previous_year_ncb_percentage"]
            .round()
            .astype(float)
        )

        after_null = df["previous_year_ncb_percentage"].isna().sum()
        print(f"   '(blank)' rows: {before_blank}")
        print(f"   NULL after: {after_null}")

    step_log("previous_year_ncb_percentage", clean_previous_ncb)

    # ----------------------------------------------------------
    # applicable_discount_with_ncb
    # ----------------------------------------------------------
    def clean_discount_ncb():
        before_blank = (
            df["applicable_discount_with_ncb"] == "(blank)"
        ).sum()

        df["applicable_discount_with_ncb"] = df[
            "applicable_discount_with_ncb"
        ].replace("(blank)", None)
        df["applicable_discount_with_ncb"] = pd.to_numeric(
            df["applicable_discount_with_ncb"], errors="coerce"
        )
        df["applicable_discount_with_ncb"] = (
            df["applicable_discount_with_ncb"].fillna(0)
        )
        df["applicable_discount_with_ncb"] = (
            df["applicable_discount_with_ncb"].round()
            .astype(float)
        )

        after_null = df["applicable_discount_with_ncb"].isna().sum()
        print(f"   '(blank)' rows: {before_blank}")
        print(f"   NULL after: {after_null}")

    step_log("applicable_discount_with_ncb", clean_discount_ncb)

    # ----------------------------------------------------------
    # ncb_amount
    # ----------------------------------------------------------
    def clean_ncb_amount():
        before_blank = (df["ncb_amount"] == "(blank)").sum()

        df["ncb_amount"] = df["ncb_amount"].replace(
            "(blank)", None
        )
        df["ncb_amount"] = pd.to_numeric(
            df["ncb_amount"], errors="coerce"
        )
        df["ncb_amount"] = df["ncb_amount"].round().astype(float)

        after_null = df["ncb_amount"].isna().sum()
        print(f"   '(blank)' rows: {before_blank}")
        print(f"   NULL after: {after_null}")

    step_log("ncb_amount", clean_ncb_amount)

    # ----------------------------------------------------------
    # before_gst_add_on_gwp
    # ----------------------------------------------------------
    def clean_before_gst():
        before_blank = (
            df["before_gst_add_on_gwp"] == "(blank)"
        ).sum()

        df["before_gst_add_on_gwp"] = df[
            "before_gst_add_on_gwp"
        ].replace("(blank)", None)
        df["before_gst_add_on_gwp"] = pd.to_numeric(
            df["before_gst_add_on_gwp"], errors="coerce"
        )
        df["before_gst_add_on_gwp"] = (
            df["before_gst_add_on_gwp"].round().astype(float)
        )

        after_null = df["before_gst_add_on_gwp"].isna().sum()
        print(f"   '(blank)' rows: {before_blank}")
        print(f"   NULL after: {after_null}")

    step_log("before_gst_add_on_gwp", clean_before_gst)

    # ----------------------------------------------------------
    # vehicle_age
    # ----------------------------------------------------------
    def clean_vehicle_age():
        before_blank = (df["vehicle_age"] == "(blank)").sum()

        df["vehicle_age"] = df["vehicle_age"].replace("(blank)", None)
        df["vehicle_age"] = pd.to_numeric(
            df["vehicle_age"], errors="coerce"
        )
        df["vehicle_age"] = df["vehicle_age"].round().astype(float)

        after_null = df["vehicle_age"].isna().sum()
        print(f"   '(blank)' rows: {before_blank}")
        print(f"   NULL after: {after_null}")

    step_log("vehicle_age", clean_vehicle_age)

    # ----------------------------------------------------------
    # Write back to Postgres
    # ----------------------------------------------------------
    def write_back():
        load_chunked(df, TABLE, SCHEMA)

    step_log("write to Postgres", write_back)


# --------------------------------------------------------------------
# Update overall churned via SQL
# --------------------------------------------------------------------
def update_overall_churned():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    print("\n🚀 Running overall churned update...")

    sql = f"""
    ALTER TABLE {SCHEMA}.{TABLE}
    ADD COLUMN IF NOT EXISTS overall_churned TEXT;

    DROP TABLE IF EXISTS temp_latest_churn;
    CREATE TEMP TABLE temp_latest_churn AS
    SELECT customerid, churn_label
    FROM (
        SELECT
            customerid,
            churn_label,
            ROW_NUMBER() OVER (PARTITION BY customerid ORDER BY end_year DESC) AS rn
        FROM {SCHEMA}.{TABLE}
    ) x
    WHERE rn = 1;

    UPDATE {SCHEMA}.{TABLE} t
    SET overall_churned = tmp.churn_label
    FROM temp_latest_churn tmp
    WHERE t.customerid = tmp.customerid;
    """

    cur.execute(sql)
    conn.commit()
    cur.close()

    print("✅ Completed: overall churned update\n")


# --------------------------------------------------------------------
# Update renewal rate status via SQL
# --------------------------------------------------------------------
def update_renewal_rate_status():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    print("\n🚀 Running renewal rate status update...")

    sql = f"""
    ALTER TABLE {SCHEMA}.{TABLE}
    ADD COLUMN IF NOT EXISTS renewal_rate_status TEXT;

    DROP TABLE IF EXISTS temp_renewal_rate;
    CREATE TEMP TABLE temp_renewal_rate AS
    SELECT
        cleaned_chassis_number,
        cleaned_engine_number,
        corrected_name,
        policy_start_date,
        CASE 
            WHEN LAG(policy_end_date) OVER (
                PARTITION BY cleaned_chassis_number, cleaned_engine_number, corrected_name
                ORDER BY policy_start_date
            ) IS NULL THEN 'Null'
            WHEN policy_start_date <
                 LAG(policy_end_date) OVER (
                     PARTITION BY cleaned_chassis_number, cleaned_engine_number, corrected_name
                     ORDER BY policy_start_date
                 ) + INTERVAL '1 day' THEN 'Null'
            ELSE CASE
                WHEN ROUND(total_premium_payable::numeric,0) >
                     LAG(ROUND(total_premium_payable::numeric,0)) OVER (
                         PARTITION BY cleaned_chassis_number, cleaned_engine_number, corrected_name
                         ORDER BY policy_start_date
                     )
                THEN 'Increase'
                WHEN ROUND(total_premium_payable::numeric,0) <
                     LAG(ROUND(total_premium_payable::numeric,0)) OVER (
                         PARTITION BY cleaned_chassis_number, cleaned_engine_number, corrected_name
                         ORDER BY policy_start_date
                     )
                THEN 'Decrease'
                ELSE 'No Change'
            END
        END AS renewal_status
    FROM {SCHEMA}.{TABLE};

    UPDATE {SCHEMA}.{TABLE} t
    SET renewal_rate_status = tmp.renewal_status
    FROM temp_renewal_rate tmp
    WHERE t.cleaned_chassis_number = tmp.cleaned_chassis_number
      AND t.cleaned_engine_number = tmp.cleaned_engine_number
      AND t.corrected_name = tmp.corrected_name
      AND t.policy_start_date = tmp.policy_start_date;
    """

    cur.execute(sql)
    conn.commit()
    cur.close()

    print("✅ Completed: renewal rate status update\n")


# --------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------
# with DAG(
#     "alter_columns_py",
#     start_date=datetime(2024, 11, 1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:

#     t1 = PythonOperator(
#         task_id="convert_datatypes_and_reload",
#         python_callable=convert_and_reload,
#     )

#     t2 = PythonOperator(
#         task_id="update_overall_churned",
#         python_callable=update_overall_churned,
#     )

#     t3 = PythonOperator(
#         task_id="update_renewal_rate_status",
#         python_callable=update_renewal_rate_status,
#     )

#     t1 >> t2 >> t3
