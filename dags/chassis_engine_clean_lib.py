from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import pandas as pd
import re



# ---------------------------------------------------------
# 🔧 Constants / Paths
# ---------------------------------------------------------

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
TARGET_SCHEMA = "test_aggregation"
STAGE_SCHEMA = "test_aggregation"
BACKUP_TABLE = "basiccleaned_appended_base_and_pr"
SOURCE_TABLE_1 = "working_basiccleaned_appended_base_and_pr"
TARGET_TABLE_1 = "cleanchassisengine_basiccleaned_appended_base_and_pr"
DUP_CLEAN_TARGET_1 ="dupclean_cleanchassisengine_basiccleaned_appended_base_and_pr"

SOURCE_TABLE_2 = "samechassisno_differregno"
TARGET_TABLE_2 = "cleanchassisengine_samechassisno_differregno"
DUP_CLEAN_TARGET_2 = "dupclean_cleanchassisengine_samechassisno_differregno"

FINAL_TABLE = "overallcleaned_chessis_engine"
OUTER_CHUNK = 10000
INNER_CHUNK = 5000
LOG_SCHEMA = "pip_log"

# ---------------------------------------------------------
# SQL QUERY HANDELING
# ---------------------------------------------------------
CREATE_TABLE_SQL = f"""
DROP TABLE IF EXISTS {TARGET_SCHEMA}.{SOURCE_TABLE_2};
CREATE TABLE {TARGET_SCHEMA}.{SOURCE_TABLE_2} AS 
SELECT * 
FROM {TARGET_SCHEMA}.{SOURCE_TABLE_1}
WHERE "cleaned_chassis_number" IN (
    SELECT "cleaned_chassis_number"
    FROM {STAGE_SCHEMA}.{SOURCE_TABLE_1}
    GROUP BY "cleaned_chassis_number"
    HAVING COUNT(DISTINCT "cleaned_reg_no") > 1
);
"""

DELETE_SQL = f"""
WITH bad_keys AS (
    SELECT "cleaned_chassis_number"
    FROM {TARGET_SCHEMA}.{SOURCE_TABLE_1}
    GROUP BY "cleaned_chassis_number"
    HAVING COUNT(DISTINCT "cleaned_reg_no") > 1
)
DELETE FROM {TARGET_SCHEMA}.{SOURCE_TABLE_1}
WHERE "cleaned_chassis_number" IN (
    SELECT "cleaned_chassis_number" FROM bad_keys
);
"""
UNION_QUERY = f"""
DROP TABLE IF EXISTS {TARGET_SCHEMA}.{FINAL_TABLE};
CREATE TABLE {TARGET_SCHEMA}.{FINAL_TABLE} AS 
SELECT * 
FROM {TARGET_SCHEMA}.{DUP_CLEAN_TARGET_1}
UNION ALL
SELECT * 
FROM {TARGET_SCHEMA}.{DUP_CLEAN_TARGET_2};
"""
# ---------------------------------------------------------
# SAME CHASSIS DIFFERENT ENGINE NO TABLE CREATION
# ---------------------------------------------------------
def run_samechassis_process(**context):
    print("▶ Starting Same-Chassis Different-RegNo process...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        # 1️⃣ TRUNCATE working table (if exists)
        print("🔄 Truncating working table...")
        conn.execute(text(f"""
            DROP TABLE IF EXISTS {TARGET_SCHEMA}.{SOURCE_TABLE_1};
            CREATE TABLE {TARGET_SCHEMA}.{SOURCE_TABLE_1} AS
            SELECT * FROM {TARGET_SCHEMA}.{BACKUP_TABLE};
        """))
        print("✅ Working table refreshed from backup")
        print("📌 Creating table samechassisno_differregno ...")
        conn.execute(text(CREATE_TABLE_SQL))
        print("✅ Table created/updated.")

        print("🧹 Removing matching rows from main table ...")
        conn.execute(text(DELETE_SQL))
        print("✅ Removed rows successfully.")

    print("🎉 Process completed.")


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
            name="chassis_removal_reason_lib",
            schema=LOG_SCHEMA,
            con=conn,
            if_exists="append",
            index=False
        )

# ---------------------------------------------------------
# Cleaning the Chassis Engine No for Both Table
# ---------------------------------------------------------
def clean_chassis_engine_no(source_table, target_table):

    print(f"\n==============================")
    print(f"▶ Cleaning: {source_table}")
    print(f"==============================\n")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    read_engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        text(f'SELECT * FROM "{TARGET_SCHEMA}"."{source_table}"'),
        con=read_engine
    )

    print(f"📊 Loaded rows = {len(df)}")
    if df.empty:
        print("⚠ No data found — skipping.")
        return
    # ---------------------------
    # Step 1.1: Separate Data into Valid and Invalid Groups
    # ---------------------------
    REG_NO_COLUMN = "cleaned_reg_no"
    CHASSIS_COLUMN = "cleaned_chassis_number"
    ENGINE_COLUMN = "cleaned_engine_number"
    MODEL_COLUMN = "model"

    with read_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {LOG_SCHEMA}.chassis_removal_reason_lib"))
    print("log table dropped sucessfully")

    # Separate rows where cleaned_reg_no is NOT null and does NOT contain 'new' (valid rows)
    valid_df = df[df[REG_NO_COLUMN].notnull() & ~df[REG_NO_COLUMN].str.contains("new", case=False, na=False)].copy()

    # The invalid rows (where cleaned_reg_no is null or contains 'new') remain unchanged
    invalid_df = df[~(df[REG_NO_COLUMN].notnull() & ~df[REG_NO_COLUMN].str.contains("new", case=False, na=False))].copy()
    invalid_df["removal_reason"] = "invalid cleaned_reg_no"
    load_log(invalid_df)
    print(f"Valid records for cleaning: {len(valid_df)}")
    print(f"Invalid records (unchanged): {len(invalid_df)}")

    # ---------------------------
    # Step 2: Clean Valid Data
    # ---------------------------
    # Ensure chassis & engine columns in the valid subset are strings and fill NaNs with empty strings.
    valid_df[CHASSIS_COLUMN] = valid_df[CHASSIS_COLUMN].astype(str).fillna("")
    valid_df[ENGINE_COLUMN] = valid_df[ENGINE_COLUMN].astype(str).fillna("")

    print("Creating lookup dictionaries for chassis & engine numbers (valid records only)...")

    # Create lookup dictionaries for valid rows by grouping on cleaned_reg_no and model.
    chassis_lookup = (
        valid_df.groupby([REG_NO_COLUMN, MODEL_COLUMN])[CHASSIS_COLUMN]
        .apply(lambda x: max(x, key=len))  # Get the longest chassis number in each group.
        .to_dict()
    )
    engine_lookup = (
        valid_df.groupby([REG_NO_COLUMN, MODEL_COLUMN])[ENGINE_COLUMN]
        .apply(lambda x: max(x, key=len))  # Get the longest engine number in each group.
        .to_dict()
    )

    # Update the chassis and engine numbers in the valid subset using the lookup dictionaries.
    valid_df[CHASSIS_COLUMN] = valid_df[[REG_NO_COLUMN, MODEL_COLUMN]].apply(
        lambda x: chassis_lookup.get(tuple(x), ""), axis=1
    )
    valid_df[ENGINE_COLUMN] = valid_df[[REG_NO_COLUMN, MODEL_COLUMN]].apply(
        lambda x: engine_lookup.get(tuple(x), ""), axis=1
    )

    # ---------------------------
    # Step 3: Combine Cleaned Valid Data with Unchanged Invalid Data
    # ---------------------------
    final_df = pd.concat([valid_df, invalid_df], ignore_index=True)
    print(f"Total records in final output: {len(final_df)}")
    # 🔽 Normalize column names before writing to DB
    final_df.columns = (
        final_df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    print("📝 Normalized column names in final_df")
    load_chunked(final_df, target_table, TARGET_SCHEMA)
    print(f"🎉 Completed cleaning for {source_table} → {target_table}\n")

def dup_clean_chassis_engine_no(source_table, target_table):

    print(f"\n==============================")
    print(f"▶ Cleaning: {source_table}")
    print(f"==============================\n")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    read_engine = pg_hook.get_sqlalchemy_engine()
    write_engine = pg_hook.get_sqlalchemy_engine()

    try:
        # READ
        df = pd.read_sql(
            text(f'SELECT * FROM "{TARGET_SCHEMA}"."{source_table}"'),
            con=read_engine
        )

        print(f"📊 Loaded rows = {len(df)}")
        if df.empty:
            print("⚠ No data found — skipping.")
            return

        # VALIDATION
        def is_valid_value(val):
            if pd.isna(val):
                return False
            val_str = str(val).strip().lower()
            return val_str != "" and val_str != "blank"

        mask_valid = (
            df['cleaned_chassis_number'].apply(is_valid_value) &
            df['cleaned_engine_number'].apply(is_valid_value)
        )

        invalid_values = df[~mask_valid].copy()
        invalid_values["removal_reason"] = "invalid chassis/engine numbers"
        load_log(invalid_values)
        print("masked invalid")

        df_valid = df[mask_valid].copy()

        # DATATYPE SAFETY (important)
        df_valid['policy_start_date'] = pd.to_datetime(df_valid['policy_start_date'], errors='coerce')
        df_valid['policy_end_date']   = pd.to_datetime(df_valid['policy_end_date'], errors='coerce')
        df_valid['policy_issue_date'] = pd.to_datetime(df_valid['policy_issue_date'], errors='coerce')
        df_valid['total_premium_payable'] = pd.to_numeric(df_valid.get('total_premium_payable'), errors='coerce')

        # TEMP TABLE
        temp_table = "tmp_dup_handling"
        print(f"🛠 Creating temp table → {TARGET_SCHEMA}.{temp_table}")

        # with write_engine.begin() as conn:
        #     conn.execute(text(f'DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{temp_table}"'))

        # print("dropped temp table successfully")
        # load_chunked(df_valid, temp_table, TARGET_SCHEMA)
        print(f"data loaded to {temp_table} successfully")

        # DEDUPE SQL (drop then create to ensure replace)
        sql = f"""
            CREATE TABLE "{TARGET_SCHEMA}"."{target_table}" AS
            WITH ranked AS (
                SELECT 
                    t.*,
                    CASE WHEN data IN ('base_2022','base_2023','base_2024') THEN 1 ELSE 2 END AS priority_group,
                    ROW_NUMBER() OVER (
                        PARTITION BY cleaned_chassis_number,
                                     cleaned_engine_number,
                                     policy_start_date,
                                     policy_end_date
                        ORDER BY 
                            CASE WHEN data IN ('base_2022','base_2023','base_2024') THEN 1 ELSE 2 END,
                            total_premium_payable DESC,
                            policy_issue_date DESC
                    ) AS rn
                FROM "{TARGET_SCHEMA}"."{temp_table}" t
            )
            SELECT *
            FROM ranked
            WHERE rn = 1;
        """

        print("▶ Running SQL duplicate resolution...")
        write_engine.dispose()
        write_engine = pg_hook.get_sqlalchemy_engine()
        print("engine disposed and created")
        with write_engine.begin() as conn:
            conn.execute(text("SET statement_timeout TO 0"))
            conn.execute(text(f'DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{target_table}"'))
            print(f"dropped {target_table} (if existed)")
            conn.execute(text(sql))
        print(f"✅ SQL duplicate cleanup completed → {TARGET_SCHEMA}.{target_table}")

        # DROP TEMP
        with write_engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{temp_table}"'))
        print(f"🧹 Dropped temp table {temp_table}")
        print(f"🎉 Completed cleaning for {source_table} → {target_table}\n")

    except Exception as e:
        # Print full error for debugging (keep short)
        print("❌ ERROR in dup_clean_chassis_engine_no:", e)
        raise

    finally:
        # Always dispose engines to avoid connection leaks
        try:
            read_engine.dispose()
        except Exception:
            pass
        try:
            write_engine.dispose()
        except Exception:
            pass


# ----------------------------------------------------------------------
# FUNCTION CALLING FOR CLEANING CHASSIS ENGINE NO 
# ----------------------------------------------------------------------
def clean_basiccleaned_basepr(**context):
    clean_chassis_engine_no(
        source_table=SOURCE_TABLE_1,
        target_table=TARGET_TABLE_1
    )


def clean_samechassis_engino(**context):
    clean_chassis_engine_no(
        source_table=SOURCE_TABLE_2,
        target_table=TARGET_TABLE_2
    )
# ----------------------------------------------------------------------
# FUNCTION CALLING FOR DUPLICATE CLEANING FOR CHASSIS ENGINE NO
# ----------------------------------------------------------------------
def duplicate_cleaning_basiccleaned_basepr(**context):
    dup_clean_chassis_engine_no(
        source_table=TARGET_TABLE_1,
        target_table=DUP_CLEAN_TARGET_1
    )


def duplicate_cleaning_clean_samechassis_engino(**context):
    dup_clean_chassis_engine_no(
        source_table=TARGET_TABLE_2,
        target_table=DUP_CLEAN_TARGET_2
    )

# ----------------------------------------------------------------------
# FINAL UNION QUERY 
# ----------------------------------------------------------------------
def final_union(**context):
    print("Appending basic clean and samechassis clean table")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        print(f"📌 creating final table {FINAL_TABLE} ...")
        conn.execute(text(UNION_QUERY))
        print("✅ Table created/updated.")
    print("🎉 Process completed.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="chassis_engine_cleanup",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["cleanup", "samechassis"],
) as dag:

    
    create_table_task = PythonOperator(
        task_id="create_samechassis_table",
        python_callable=run_samechassis_process,
        provide_context=True,
    )

    clean_basic_table_task = PythonOperator(
        task_id="clean_chassis_engine_no_basic",
        python_callable=clean_basiccleaned_basepr,
        provide_context=True,
    )

    clean_samechassis_table_task = PythonOperator(
        task_id="clean_same_chassis_engine_no",
        python_callable=clean_samechassis_engino,
        provide_context=True,
    )

    duplicate_basic_cleaning_task = PythonOperator(
        task_id="clean_duplicate_task_basic",
        python_callable=duplicate_cleaning_basiccleaned_basepr,
        provide_context=True,
    )

    duplicate_cleaning_samechassis_task = PythonOperator(
        task_id="duplicate_cleaning_same_chassis",
        python_callable=duplicate_cleaning_clean_samechassis_engino,
        provide_context=True,
    )

    final_task = PythonOperator(
        task_id="final_running",
        python_callable=final_union,
        provide_context=True,
    )


    # ---------------------------------------------------------
    # DAG TASK ORDER (FINAL & CORRECT)
    # ---------------------------------------------------------

    # Step 1
    create_table_task  >> [clean_basic_table_task, clean_samechassis_table_task]

    # Step 2 — EXPANDED (required)
    clean_basic_table_task >> duplicate_basic_cleaning_task
    clean_basic_table_task >> duplicate_cleaning_samechassis_task
    clean_samechassis_table_task >> duplicate_basic_cleaning_task
    clean_samechassis_table_task >> duplicate_cleaning_samechassis_task

    # Step 3
    [duplicate_basic_cleaning_task, duplicate_cleaning_samechassis_task] >> final_task 




