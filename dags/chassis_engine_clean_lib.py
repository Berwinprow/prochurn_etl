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
POSTGRES_CONN_ID = "postgres_cloud_prochurn"

DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")

TARGET_SCHEMA = get_schema("agg", META_JSON)
ARCHIVE_SCHEMA = get_schema("archive_log", META_JSON)
STAGE_SCHEMA = get_schema("agg", META_JSON)

BACKUP_TABLE = "appended_base_and_pr_basic_clean"
SOURCE_TABLE_1 = "appended_base_and_pr_basic_clean_working_table"
TARGET_TABLE_1 = "cleanchassisengine_on_appended_base_and_pr_basic_clean"
DUP_CLEAN_TARGET_1 = (
    "dupclean_cleanchassisengine_basiccleaned_appended_base_and_pr"
)

SOURCE_TABLE_2 = "samechassis_no_differr_eg_no"
TARGET_TABLE_2 = "clean_samechassisno_differregno"
DUP_CLEAN_TARGET_2 = (
    "dupclean_samechassisno_differregno"
)
FINAL_TABLE = "overallcleaned_chessis_engine"

LOG_TABLE = "chassis_removal_reason_lib"
FEATURE_ENG_LOG = get_log_tables("featurelog", META_JSON)
OUTER_CHUNK = 10000
INNER_CHUNK = 5000
LOG_SCHEMA =  get_schema("log", META_JSON)


# ---------------------------------------------------------
# Update chassis engine cleaning metadata
# ---------------------------------------------------------
def update_chassis_metadata(engine, backup_table, final_table):
    with engine.begin() as conn:
        before_cnt = conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."{backup_table}"'
            )
        ).scalar()

        final_cnt = conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."{final_table}"'
            )
        ).scalar()

    removed_cnt = (before_cnt or 0) - (final_cnt or 0)
    print(
        f"📊 Chassis metadata: before={before_cnt}, "
        f"after={final_cnt}, removed={removed_cnt}"
    )

    query = f"""
        UPDATE {LOG_SCHEMA}.{FEATURE_ENG_LOG}
        SET is_chasiss_engine_cleaned = 'YES',
            chasiss_cleaned_name      = :final_tbl,
            chassis_cleaned_cnt       = :final_cnt,
            chassis_removed_cnt     = :removed_cnt,
            timestamp                 = NOW()
        WHERE last_run_date = (
            SELECT last_run_date
            FROM {LOG_SCHEMA}.{FEATURE_ENG_LOG}
            WHERE appended_basepr_name = :backup_tbl
              AND is_chasiss_engine_cleaned = 'NO'
            ORDER BY timestamp DESC
            LIMIT 1
        );
    """

    with engine.begin() as conn:
        conn.execute(
            text(query),
            {
                "final_tbl": final_table,
                "final_cnt": final_cnt or 0,
                "removed_cnt": removed_cnt,
                "backup_tbl": backup_table,
            },
        )

    print(
        "✅ Updated chassis engine cleaned metadata "
        "in feature_eng_log"
    )


# ---------------------------------------------------------
# Archive existing chassis removal logs before new run
# ---------------------------------------------------------
def archive_chassis_logs(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    today = datetime.utcnow().date()
    archive_table = f"{LOG_TABLE}_{today.strftime('%Y%m%d')}"

    with engine.begin() as conn:
        conn.execute(
            text(f"CREATE SCHEMA IF NOT EXISTS {ARCHIVE_SCHEMA};")
        )
        # Ensure log table exists
        conn.execute(
            text(f"""
                CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.{LOG_TABLE} (
                    cleaned_reg_no text,
                    cleaned_chassis_number text,
                    cleaned_engine_number text,
                    model text,
                    policy_no text,
                    policy_start_date text,
                    policy_end_date text,
                    data text,
                    removal_reason text,
                    run_date date
                );
            """)
        )
        print(F"TABLE {LOG_TABLE} CREATED SUCESSFULLY")
        result = conn.execute(
            text(f"SELECT COUNT(1) FROM {LOG_SCHEMA}.{LOG_TABLE}")
        )
        count = result.scalar()
        if count and count > 0:
            conn.execute(
                text(
                    f"""
                CREATE TABLE IF NOT EXISTS {ARCHIVE_SCHEMA}.{archive_table} AS
                SELECT * FROM {LOG_SCHEMA}.{LOG_TABLE};
            """
                )
            )
            conn.execute(
                text(f"TRUNCATE TABLE {LOG_SCHEMA}.{LOG_TABLE};")
            )
            print(
                f"📦 Archived {count} rows to {ARCHIVE_SCHEMA}."
                f"{archive_table} and cleared main log."
            )
        else:
            print("ℹ No existing logs to archive.")


# ---------------------------------------------------------
# Get latest backup table from feature_eng_log
# ---------------------------------------------------------
def get_backup_table(engine):
    query = f"""
        SELECT appended_basepr_name
        FROM {LOG_SCHEMA}.{FEATURE_ENG_LOG}
        WHERE appended_basepr_name IS NOT NULL
          AND is_chasiss_engine_cleaned = 'NO'
        ORDER BY timestamp DESC
        LIMIT 1;
    """
    with engine.begin() as conn:
        row = conn.execute(text(query)).fetchone()
    if not row or not row[0]:
        raise Exception(
            "❌ No backup table found in feature_eng_log with "
            "is_chasiss_engine_cleaned = 'NO'"
        )
    print(f"🧭 Using backup table from metadata: {row[0]}")
    return row[0]


# ---------------------------------------------------------
# SQL query handling (create / delete / union)
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
    BACKUP_TABLE = get_backup_table(engine)
    with engine.begin() as conn:
        print("🔄 Truncating working table...")
        conn.execute(
            text(
                f"""
            DROP TABLE IF EXISTS {TARGET_SCHEMA}.{SOURCE_TABLE_1};
            CREATE TABLE {TARGET_SCHEMA}.{SOURCE_TABLE_1} AS
            SELECT * FROM {TARGET_SCHEMA}.{BACKUP_TABLE};
        """
            )
        )
        print("✅ Working table refreshed from backup")
        print("📌 Creating table samechassisno_differregno ...")
        conn.execute(text(CREATE_TABLE_SQL))
        print("✅ Table created/updated.")
        print("🧹 Removing matching rows from main table ...")
        conn.execute(text(DELETE_SQL))
        print("✅ Removed rows successfully.")

    print("🎉 Process completed.")


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
# Log Table
# ---------------------------------------------------------
def load_log(df):
    if df.empty:
        return

    LOG_COLUMNS = [
        "cleaned_reg_no",
        "cleaned_chassis_number",
        "cleaned_engine_number",
        "model",
        "policy_no",
        "policy_start_date",
        "policy_end_date",
        "data",
        "removal_reason",
        "run_date",
    ]

    df["run_date"] = datetime.utcnow().date()

    for col in LOG_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[LOG_COLUMNS]

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        df.to_sql(
            name="chassis_removal_reason_lib",
            schema=LOG_SCHEMA,
            con=conn,
            if_exists="append",  # always append
            index=False,
        )


# ---------------------------------------------------------
# Cleaning the Chassis Engine No for Both Table
# ---------------------------------------------------------
def clean_chassis_engine_no(source_table, target_table):
    print("\n==============================")
    print(f"▶ Cleaning: {source_table}")
    print("==============================\n")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    read_engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        text(
            f'SELECT * FROM "{TARGET_SCHEMA}"."{source_table}"'
        ),
        con=read_engine,
    )

    print(f"📊 Loaded rows = {len(df)}")
    if df.empty:
        print("⚠ No data found — skipping.")
        return

    REG_NO_COLUMN = "cleaned_reg_no"
    CHASSIS_COLUMN = "cleaned_chassis_number"
    ENGINE_COLUMN = "cleaned_engine_number"
    MODEL_COLUMN = "model"

    removal_logs = []

    valid_df = df[
        df[REG_NO_COLUMN].notnull()
        & ~df[REG_NO_COLUMN]
        .str.contains("new", case=False, na=False)
    ].copy()

    invalid_df = df[
        ~(
            df[REG_NO_COLUMN].notnull()
            & ~df[REG_NO_COLUMN]
            .str.contains("new", case=False, na=False)
        )
    ].copy()
    invalid_df["removal_reason"] = "invalid cleaned_reg_no"
    removal_logs.append(invalid_df)
    print(f"Valid records for cleaning: {len(valid_df)}")
    print(f"Invalid records (unchanged): {len(invalid_df)}")

    valid_df[CHASSIS_COLUMN] = (
        valid_df[CHASSIS_COLUMN].astype(str).fillna("")
    )
    valid_df[ENGINE_COLUMN] = (
        valid_df[ENGINE_COLUMN].astype(str).fillna("")
    )

    print(
        "Creating lookup dictionaries for chassis & engine "
        "(valid records only)..."
    )

    chassis_lookup = (
        valid_df.groupby([REG_NO_COLUMN, MODEL_COLUMN])[CHASSIS_COLUMN]
        .apply(lambda x: max(x, key=len))
        .to_dict()
    )
    engine_lookup = (
        valid_df.groupby([REG_NO_COLUMN, MODEL_COLUMN])[ENGINE_COLUMN]
        .apply(lambda x: max(x, key=len))
        .to_dict()
    )

    valid_df[CHASSIS_COLUMN] = valid_df[
        [REG_NO_COLUMN, MODEL_COLUMN]
    ].apply(lambda x: chassis_lookup.get(tuple(x), ""), axis=1)
    valid_df[ENGINE_COLUMN] = valid_df[
        [REG_NO_COLUMN, MODEL_COLUMN]
    ].apply(lambda x: engine_lookup.get(tuple(x), ""), axis=1)

    final_df = pd.concat([valid_df, invalid_df], ignore_index=True)
    print(f"Total records in final output: {len(final_df)}")

    final_df.columns = (
        final_df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    print("📝 Normalized column names in final_df")
    load_chunked(final_df, target_table, TARGET_SCHEMA)
    print(
        f"🎉 Completed cleaning for {source_table} → "
        f"{target_table}\n"
    )

    if removal_logs:
        log_df = pd.concat(removal_logs, ignore_index=True)
        load_log(log_df)
        print(
            f"🧾 Logged {len(log_df)} removal rows into "
            f"{LOG_SCHEMA}.chassis_removal_reason_lib"
        )


def dup_clean_chassis_engine_no(source_table, target_table):
    print("\n==============================")
    print(f"▶ Cleaning: {source_table}")
    print("==============================\n")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    read_engine = pg_hook.get_sqlalchemy_engine()
    write_engine = pg_hook.get_sqlalchemy_engine()

    try:
        df = pd.read_sql(
            text(
                f'SELECT * FROM "{TARGET_SCHEMA}"."{source_table}"'
            ),
            con=read_engine,
        )

        print(f"📊 Loaded rows = {len(df)}")
        if df.empty:
            print("⚠ No data found — skipping.")
            return

        removal_logs = []

        def is_valid_value(val):
            if pd.isna(val):
                return False
            val_str = str(val).strip().lower()
            return val_str != "" and val_str != "blank"

        mask_valid = (
            df["cleaned_chassis_number"].apply(is_valid_value)
            & df["cleaned_engine_number"].apply(is_valid_value)
        )

        invalid_values = df[~mask_valid].copy()
        invalid_values["removal_reason"] = (
            "invalid chassis/engine numbers"
        )
        removal_logs.append(invalid_values)
        print("masked invalid")

        df_valid = df[mask_valid].copy()

        df_valid["policy_start_date"] = pd.to_datetime(
            df_valid["policy_start_date"], errors="coerce"
        )
        df_valid["policy_end_date"] = pd.to_datetime(
            df_valid["policy_end_date"], errors="coerce"
        )
        df_valid["policy_issue_date"] = pd.to_datetime(
            df_valid["policy_issue_date"], errors="coerce"
        )
        df_valid["total_premium_payable"] = pd.to_numeric(
            df_valid.get("total_premium_payable"), errors="coerce"
        )

        # ---------------------------
        # Step 5: Handle Duplicates based on grouping columns
        # ---------------------------
        def prioritize_trim_group(group):
            base_values = ['2022_base', '2023_base', '2024_base']
            base_rows = group[group['data'].isin(base_values)]
            if not base_rows.empty:
                # Choose the record with the highest total premium payable among base records
                selected = base_rows.sort_values(by='total_premium_payable', ascending=False).iloc[0]
            else:
                # Otherwise choose the record with the latest policy issue date, then highest total premium payable
                selected = group.sort_values(by=['policy_issue_date', 'total_premium_payable'], ascending=[False, False]).iloc[0]
            return selected

        def assign_trim_group(group):
            if len(group) > 1:
                selected_row = prioritize_trim_group(group)
            else:
                selected_row = group.iloc[0]
            return selected_row

        # Group by the relevant columns and apply duplicate handling
        df_final = (
            df_valid
            .groupby(['cleaned_chassis_number', 'cleaned_engine_number', 'policy_start_date', 'policy_end_date'], group_keys=False)
            .apply(assign_trim_group)
            .reset_index(drop=True)
        )

        load_chunked(df_final,target_table,TARGET_SCHEMA)
        
        print(
            f"🎉 Completed cleaning for {source_table} → "
            f"{target_table}\n"
        )

        if removal_logs:
            log_df = pd.concat(removal_logs, ignore_index=True)
            load_log(log_df)
            print(
                f"🧾 Logged {len(log_df)} removal rows into "
                f"{LOG_SCHEMA}.chassis_removal_reason_lib"
            )

        
        print(
            f"log removed rows sucessfully "
        )

    except Exception as e:
        print("❌ ERROR in dup_clean_chassis_engine_no:", e)
        raise

    finally:
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
        target_table=TARGET_TABLE_1,
    )


def clean_samechassis_engino(**context):
    clean_chassis_engine_no(
        source_table=SOURCE_TABLE_2,
        target_table=TARGET_TABLE_2,
    )


# ----------------------------------------------------------------------
# FUNCTION CALLING FOR DUPLICATE CLEANING FOR CHASSIS ENGINE NO
# ----------------------------------------------------------------------
def duplicate_cleaning_basiccleaned_basepr(**context):
    dup_clean_chassis_engine_no(
        source_table=TARGET_TABLE_1,
        target_table=DUP_CLEAN_TARGET_1,
    )


def duplicate_cleaning_clean_samechassis_engino(**context):
    dup_clean_chassis_engine_no(
        source_table=TARGET_TABLE_2,
        target_table=DUP_CLEAN_TARGET_2,
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
        print("✅ Table created/dated.")
    print("🎉 Process completed.")

    backup_table = get_backup_table(engine)
    update_chassis_metadata(engine, backup_table, FINAL_TABLE)
    print("🎉 Process completed.")


# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2024, 11, 1),
#     "retries": 0,
#     "retry_delay": timedelta(minutes=3),
# }

# with DAG(
#     dag_id="chassis_engine_cleanup",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["cleanup", "samechassis"],
# ) as dag:

#     archive_logs_task = PythonOperator(
#         task_id="archive_chassis_logs",
#         python_callable=archive_chassis_logs,
#         provide_context=True,
#     )

#     create_table_task = PythonOperator(
#         task_id="create_samechassis_table",
#         python_callable=run_samechassis_process,
#         provide_context=True,
#     )

#     clean_basic_table_task = PythonOperator(
#         task_id="clean_chassis_engine_no_basic",
#         python_callable=clean_basiccleaned_basepr,
#         provide_context=True,
#     )

#     clean_samechassis_table_task = PythonOperator(
#         task_id="clean_same_chassis_engine_no",
#         python_callable=clean_samechassis_engino,
#         provide_context=True,
#     )

#     duplicate_basic_cleaning_task = PythonOperator(
#         task_id="clean_duplicate_task_basic",
#         python_callable=duplicate_cleaning_basiccleaned_basepr,
#         provide_context=True,
#     )

#     duplicate_cleaning_samechassis_task = PythonOperator(
#         task_id="duplicate_cleaning_same_chassis",
#         python_callable=duplicate_cleaning_clean_samechassis_engino,
#         provide_context=True,
#     )

#     final_task = PythonOperator(
#         task_id="final_running",
#         python_callable=final_union,
#         provide_context=True,
#     )

#     archive_logs_task >> create_table_task >> [
#         clean_basic_table_task,
#         clean_samechassis_table_task,
#     ]

#     clean_basic_table_task >> duplicate_basic_cleaning_task
#     clean_basic_table_task >> duplicate_cleaning_samechassis_task
#     clean_samechassis_table_task >> duplicate_basic_cleaning_task
#     clean_samechassis_table_task >> duplicate_cleaning_samechassis_task

#     [duplicate_basic_cleaning_task, duplicate_cleaning_samechassis_task] >> final_task
