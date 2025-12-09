from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import re
from sqlalchemy import text
import pandas as pd
from fuzzywuzzy import fuzz



# ---------------------------------------------------------
# 🔧 Constants / Paths
# ---------------------------------------------------------


POSTGRES_CONN_ID = "postgres_cloud_prochurn"

SOURCE_TABLE_1 = "overallcleaned_chessis_engine"
TARGET_TABLE_1 = "cleancus_overallcleaned_chessis_engine"
TARGET_TABLE_2 = "corrected_cleancus_overallcleaned_chessis_engine"
TARGET_SCHEMA = "test_aggregation"
SCHEMA = "test_aggregation"
OUTER_CHUNK = 10000
INNER_CHUNK = 5000
LOG_SCHEMA = "pip_log"
# ----------------------------------------------------------------------
# Load Data To Postgres
# ----------------------------------------------------------------------
def load_chunked(df, table_name, schema):
    total_rows = len(df)
    print(f"\n🚀 Loading → {schema}.{table_name} ({total_rows} rows)")

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
                method="multi"
            )

        first = False
        print(f"Loaded rows {start} → {end}")

    print(f"✔ Load COMPLETE → {schema}.{table_name}\n")
# ---------------------------------------------------------
# Log Table
# ---------------------------------------------------------
def load_log(df):
    if df.empty:
        return
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        df.to_sql(
            name="corrected_name_removal_reason_lib",
            schema=LOG_SCHEMA,
            con=conn,
            if_exists="append",
            index=False
        )

# ---------------------------------------------------------
#correcting null in customer name
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
        conn.execute(text(f'DROP TABLE IF EXISTS "{LOG_SCHEMA}"."corrected_name_removal_reason_lib"'))
        print(f"droped the corrected_name_removal_reason_lib to create with new LOG TABLE")

        conn.execute(text(f'DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{TARGET_TABLE_1}"'))
        print(f"droped the {TARGET_TABLE_1} to create with new data")
        conn.execute(text(SQL_QUERY))
        print("✅ Table created/updated.")
    print("🎉 Process completed.")
    
    # 🔍 LOG REMOVED ROWS FOR NULL/EMPTY INSURED NAME
    removed_df = pd.read_sql(
        text(f'''
            SELECT *
            FROM "{SCHEMA}"."{SOURCE_TABLE_1}"
            WHERE "cleaned_insured_name" IS NULL
            OR "cleaned_insured_name" = ''
            OR lower("cleaned_insured_name") = 'none'
        '''), 
        con=engine
    )

    if not removed_df.empty:
        removed_df["removal_reason"] = "Null/Empty insured name corrected using lookup"
        load_log(removed_df)


# ---------------------------------------------------------
# cleaning corrrected customer names
# ---------------------------------------------------------
def clean_corrected_name_fuzzy(**context):

    print(f"\n==============================")
    print(f"▶ Cleaning: {TARGET_TABLE_1}")
    print(f"==============================\n")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    read_engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        text(f'SELECT * FROM "{SCHEMA}"."{TARGET_TABLE_1}"'),
        con=read_engine
    )

    print(f"📊 Loaded rows = {len(df)}")
    if df.empty:
        print("⚠ No data found — skipping.")
        return
    # Create chassis_engine_key by concatenating "cleaned_chassis_number" and "cleaned_engine_number"
    df['chassis_engine_key'] = df['cleaned_chassis_number'].astype(str) + '_' + df['cleaned_engine_number'].astype(str)

    # Sort the DataFrame by chassis_engine_key and policy_start_date to ensure sequential processing
    df.sort_values(["chassis_engine_key", "policy_start_date"], inplace=True)

    # ---------------------------
    # Step 2: Sequential Name Correction
    # ---------------------------
    # Initialize previous name tracker
    prev_name = None
    prev_chassis = None

    corrected_names = []
    similarity_scores = []

    # Iterate over rows sequentially
    for index, row in df.iterrows():
        # Use the column "cleaned_insured_name_filled"
        current_name = row["cleaned_insured_name_filled"]
        chassis_engine_key = row["chassis_engine_key"]

        # Handle potential null values by converting them to an empty string
        if pd.isnull(current_name):
            current_name = ""
        
        # Check if we are still within the same chassis_engine_key group
        if prev_name is not None and prev_chassis == chassis_engine_key:
            similarity = fuzz.ratio(prev_name, current_name)
            if similarity >= 80:  # If similar, use the previous (corrected) name
                corrected_names.append(prev_name)
            else:
                corrected_names.append(current_name)
        else:
            # First record for this chassis_engine_key; keep the original name
            corrected_names.append(current_name)

        # Calculate the similarity score between the corrected name and the current name
        similarity_scores.append(fuzz.ratio(corrected_names[-1], current_name))
        
        # Update previous values
        prev_name = corrected_names[-1]
        prev_chassis = chassis_engine_key

    # Add the new columns to the DataFrame
    df["corrected_name"] = corrected_names
    df["name_similarity"] = similarity_scores
    
    # 🔽 Normalize column names before writing to DB
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    print("📝 Normalized column names in final_df")
    load_chunked(df, TARGET_TABLE_2, SCHEMA)
    # # 🔍 LOG REMOVED / CHANGED NAMES BY FUZZY MATCH
    # changed_rows = df[df["corrected_name"] != df["cleaned_insured_name_filled"]].copy()

    # if not changed_rows.empty:
    #     changed_rows["removal_reason"] = (
    #         "Corrected by fuzzy-matching (similarity < 80 threshold logic)"
    #     )
    #     load_log(changed_rows)

    # print(f"🎉 Completed cleaning for correct insured names ")



default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 11, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="corrected_names_cleaning",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["cleanup", "chassis_engine"],
) as dag:

    null_case_task = PythonOperator(
        task_id="null_case_handeling",
        python_callable=corrected_name_null_cases,
        provide_context=True,
    )

    furzzy_task = PythonOperator(
        task_id="furzzy_match",
        python_callable=clean_corrected_name_fuzzy,
        provide_context=True,
    )
    

    null_case_task >> furzzy_task 