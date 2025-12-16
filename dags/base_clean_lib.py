import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

from schema_table_config import get_column_mapping, get_schema, get_log_tables


# ---------------------------------------------------------
# Constants / Paths
# ---------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")
STAGE_SCHEMA = get_schema("stage", META_JSON)
TARGET_SCHEMA = get_schema("agg", META_JSON)
LOG_SCHEMA = get_schema("log", META_JSON)
BASE2024_REMOVED_LOG_TABLE = "base2024_removed_duplicates_lib"
META_DATA = get_log_tables("metadata", META_JSON)
COLUMN_JSON = str(DAGS_DIR / "config" / "column_mapping.json")
# Load only "base" mapping from JSON
BASE_COLUMN_MAPPING = get_column_mapping("base", COLUMN_JSON)


# ---------------------------------------------------------
# Update metadata log
# ---------------------------------------------------------
def update_base_metadata(
    table_name,
    cleaned_count,
    removed_count,
):
    """
    Update base_pr_metalog after base cleaning is completed.
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    update_sql = f"""
        UPDATE {LOG_SCHEMA}.{META_DATA}
        SET
            is_base_cleaned = 'YES',
            count_base = :cleaned_count,
            base_removed_cnt = :removed_count,
            timestamp = NOW()
        WHERE table_name = :table_name;
    """

    with engine.begin() as conn:
        conn.execute(
            text(update_sql),
            {
                "table_name": table_name,
                "cleaned_count": cleaned_count,
                "removed_count": removed_count,
            },
        )

    print(
        f"✅ Metadata updated for {table_name} | "
        f"count_base={cleaned_count}, "
        f"base_removed_cnt={removed_count}"
    )


# ---------------------------------------------------------
# Policy number cleaning
# ---------------------------------------------------------
def clean_policy_number(value):
    """Ensure policy_number is numeric and remove leading `'`."""
    if pd.isna(value):
        return None
    value = str(value).strip().lstrip("'")
    return value if value.isdigit() else None


# ---------------------------------------------------------
# Helper: apply column mapping (base mapping)
# ---------------------------------------------------------
def apply_base_column_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply base column mapping to the given dataframe.
    """
    print(
        "\n🔧 [Column Mapping] Starting mapping for "
        "DataFrame..."
    )

    # Step 1: normalize DF column names
    original_columns = list(df.columns)
    df.columns = [col.strip().lower() for col in df.columns]

    # Step 2: normalize mapping keys
    normalized_mapping = {
        k.strip().lower(): v for k, v in BASE_COLUMN_MAPPING.items()
    }

    # Step 3: find which columns will be mapped
    mapped_columns = set(df.columns).intersection(normalized_mapping.keys())
    used_mapping = {col: normalized_mapping[col] for col in mapped_columns}

    # Apply rename
    df.rename(columns=used_mapping, inplace=True)

    print(
        f"🔄 [Column Mapping] Total columns before mapping: "
        f"{len(original_columns)}"
    )
    print(
        f"🔄 [Column Mapping] Columns available for mapping: "
        f"{len(mapped_columns)}"
    )
    print("🔄 [Column Mapping] Detailed mapping applied:")
    for old_col, new_col in used_mapping.items():
        print(f"   🔹 '{old_col}' → '{new_col}'")

    print("✅ [Column Mapping] Completed.\n")
    return df


# ---------------------------------------------------------
# 1. Clean base_2024
# ---------------------------------------------------------
def clean_base_2024(**context):
    """
    Clean base_2024 table:
    - Apply column mapping
    - Clean policy_no
    - Normalize dates
    - Remove duplicates
    - Write cleaned and removed data
    """
    del context  # unused

    print("\n▶▶ Starting clean_base_2024() ...")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    # 1. Read full table from stage
    source_table = "base_2024"
    if not source_table:
        print("⚠️ No base tables found.")
        return

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))

    print(f"📥 Reading data from {STAGE_SCHEMA}.{source_table} ...")
    df = pd.read_sql(
        text(f'SELECT * FROM "{STAGE_SCHEMA}"."{source_table}"'),
        con=engine,
    )

    print(
        f"📊 Total rows loaded from {STAGE_SCHEMA}.{source_table}: "
        f"{len(df)}"
    )
    print(f"📊 Total columns: {len(df.columns)}")

    if df.empty:
        print("⚠️ base_2024 is empty. Nothing to clean.")
        return

    # 2. Apply column mapping
    df = apply_base_column_mapping(df)

    # Clean policy_no after mapping
    if "policy_no" in df.columns:
        print("🧹 Cleaning policy_no ...")

        # create clean version
        df["clean_policy_no"] = df["policy_no"].apply(clean_policy_number)

        # identify invalid policy numbers
        invalid_policy_rows = df[df["clean_policy_no"].isna()].copy()
        invalid_policy_rows["removal_reason"] = "Invalid policy_no"

        # keep only valid ones
        df = df[df["clean_policy_no"].notna()].copy()

        # replace original with cleaned
        df["policy_no"] = df["clean_policy_no"]
        df.drop(columns=["clean_policy_no"], inplace=True)

        print(
            "🧹 Invalid policy_no removed: "
            f"{len(invalid_policy_rows)}"
        )
    else:
        invalid_policy_rows = pd.DataFrame()

    # Universal date normalization
    date_cols = [
        "policy_start_date",
        "policy_end_date",
        "policy_issue_date",
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], errors="coerce")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            print(
                f"🗓 Normalized {col} → YYYY-MM-DD "
                "format"
            )

    # Duplicate logic
    dup_keys = ["policy_no", "policy_start_date", "policy_end_date"]

    print(f"🔍 Identifying duplicates based on: {dup_keys}")
    duplicates = df[df.duplicated(subset=dup_keys, keep=False)]

    print(
        "📊 Total duplicate rows found (all copies): "
        f"{len(duplicates)}"
    )

    # Remove duplicates, keep only the first row per duplicate group
    df_cleaned = df.drop_duplicates(subset=dup_keys, keep="first")
    print(
        "📊 Rows AFTER removing duplicates: "
        f"{len(df_cleaned)}"
    )

    # Build removed rows
    removed_rows = (
        pd.concat(
            [
                df.reset_index(drop=True),
                df_cleaned.reset_index(drop=True),
            ]
        )
        .drop_duplicates(keep=False)
        .reset_index(drop=True)
    )

    print(
        "📊 Rows moved to removed_duplicates table: "
        f"{len(removed_rows)}"
    )

    # 5. Write cleaned data
    print(f"💾 Writing cleaned data to {TARGET_SCHEMA}.base_2024")
    df_cleaned.to_sql(
        name="base_2024",
        schema=TARGET_SCHEMA,
        con=engine,
        if_exists="replace",
        index=False,
    )
    print("✅ Cleaned data written successfully.")

    cleaned_count = len(df_cleaned)

    # 6. Write removed duplicates to log
    print(
        "💾 Writing removed duplicate rows to "
        f"{LOG_SCHEMA}.{BASE2024_REMOVED_LOG_TABLE}"
    )
    if not removed_rows.empty:
        removed_rows.to_sql(
            name=BASE2024_REMOVED_LOG_TABLE,
            schema=LOG_SCHEMA,
            con=engine,
            if_exists="replace",
            index=False,
        )
        print("✅ Removed duplicates written successfully.")
    else:
        print(
            "ℹ️ No duplicates removed, so removed-rows table will "
            "be empty/unchanged."
        )

    removed_count = len(removed_rows)
    update_base_metadata(
        table_name=source_table,
        cleaned_count=cleaned_count,
        removed_count=removed_count,
    )

    print("🎉 clean_base_2024() completed.\n")


# ---------------------------------------------------------
# 2. Clean other base_% tables (only column mapping)
# ---------------------------------------------------------
def clean_other_base_tables(**context):
    """
    Clean other base_% tables (except base_2024):
    - Apply column mapping
    - Clean policy_no
    - Normalize dates
    - Write cleaned data
    - Log invalid policies
    """
    del context  # unused

    print("\n▶▶ Starting clean_other_base_tables() ...")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    # 1. Find all base_% tables in metadata except base_2024
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"""
                SELECT table_name
                FROM {LOG_SCHEMA}.{META_DATA}
                WHERE stage_loaded = 'YES'
                    AND is_base_cleaned = 'NO'
                    AND table_name LIKE 'base_%'
                    AND table_name != 'base_2024';
                """
            ),
            {"schema": STAGE_SCHEMA},
        ).fetchall()

    base_tables = [r[0] for r in result if r[0] != "base_2024"]

    if not base_tables:
        print(
            f"⚠️ No other base_% tables found in {STAGE_SCHEMA}. "
            "Nothing to process."
        )
        return

    print(
        "📋 Found base tables to process (excluding base_2024): "
        f"{base_tables}"
    )

    for table_name in base_tables:
        try:
            print(
                f"\n▶ Processing table: "
                f"{STAGE_SCHEMA}.{table_name}"
            )

            # Read full table
            df = pd.read_sql(
                text(f'SELECT * FROM "{STAGE_SCHEMA}"."{table_name}"'),
                con=engine,
            )

            print(
                "📊 Total rows loaded from "
                f"{STAGE_SCHEMA}.{table_name}: {len(df)}"
            )
            print(f"📊 Total columns: {len(df.columns)}")

            if df.empty:
                print(
                    f"⚠️ Table {table_name} is empty. "
                    "Skipping."
                )
                continue

            # Apply column mapping
            df = apply_base_column_mapping(df)

            # Clean policy_no after mapping
            if "policy_no" in df.columns:
                print("🧹 Cleaning policy_no ...")

                df["clean_policy_no"] = df["policy_no"].apply(
                    clean_policy_number
                )

                invalid_policy_rows = df[df["clean_policy_no"].isna()].copy()
                invalid_policy_rows["removal_reason"] = "Invalid policy_no"

                df = df[df["clean_policy_no"].notna()].copy()

                df["policy_no"] = df["clean_policy_no"]
                df.drop(columns=["clean_policy_no"], inplace=True)

                print(
                    "🧹 Invalid policy_no removed: "
                    f"{len(invalid_policy_rows)}"
                )
            else:
                invalid_policy_rows = pd.DataFrame()

            # Log invalid policy rows
            if not invalid_policy_rows.empty:
                log_table = f"{table_name}_removed_invalid"
                invalid_policy_rows.to_sql(
                    name=log_table,
                    schema=LOG_SCHEMA,
                    con=engine,
                    if_exists="replace",
                    index=False,
                )
                print(
                    "🧾 Logged invalid policy_no rows → "
                    f"{LOG_SCHEMA}.{log_table}"
                )

            # Policy start and end date conversion
            date_cols = [
                "policy_start_date",
                "policy_end_date",
                "policy_issue_date",
            ]

            for col in date_cols:
                if col in df.columns:
                    df[col] = (
                        pd.to_datetime(df[col], errors="coerce")
                        .dt.strftime("%Y-%m-%d %H:%M:%S")
                    )
                    print(
                        f"🗓 Normalized {col} → YYYY-MM-DD "
                        "format"
                    )

            print(
                "💾 Writing mapped data to "
                f"{TARGET_SCHEMA}.{table_name} "
                "(replace, same table name as stage)..."
            )
            df.to_sql(
                name=table_name,
                schema=TARGET_SCHEMA,
                con=engine,
                if_exists="replace",
                index=False,
            )

            print(
                "✅ Completed mapping + load for "
                f"{table_name}. Final row count in "
                f"DataFrame: {len(df)}"
            )

            cleaned_count = len(df)
            removed_count = len(invalid_policy_rows)

            update_base_metadata(
                table_name=table_name,
                cleaned_count=cleaned_count,
                removed_count=removed_count,
            )

        except Exception as exc:
            print(f"❌ Failed to process {table_name}: {exc}")
            continue

    print("\n🎉 base tables column mapping completed.\n")


# ---------------------------------------------------------
# Airflow DAG Definition
# ---------------------------------------------------------
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2024, 11, 1),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="base_clean_lib",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["base_clean", "pip_stage", "test_aggregation"],
# ) as dag:
#     clean_base_2024_task = PythonOperator(
#         task_id="clean_base_2024",
#         python_callable=clean_base_2024,
#         provide_context=True,
#     )

#     clean_other_base_tables_task = PythonOperator(
#         task_id="clean_other_base_tables",
#         python_callable=clean_other_base_tables,
#         provide_context=True,
#     )

#     [clean_base_2024_task, clean_other_base_tables_task]
