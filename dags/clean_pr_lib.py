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
META_DATA = get_log_tables("metadata", META_JSON)

PR22_DUP_TABLE = "pr_2022_removed_duplicates"
PR23_DUP_TABLE = "pr_2023_removed_duplicates"
PR24_DUP_TABLE = "pr_2024_removed_duplicates"

COLUMN_JSON = str(DAGS_DIR / "config" / "column_mapping.json")
PR_COLUMN_MAPPING = get_column_mapping("pr", COLUMN_JSON)

OUTER_CHUNK = 10000  # same as success scripts
INNER_CHUNK = 5000  # same as success scripts


# ---------------------------------------------------------
# Metadata update
# ---------------------------------------------------------
def update_pr_metadata(
    table_name: str,
    cleaned_count: int,
    removed_count: int,
) -> None:
    """
    Update PR metadata after cleaning.
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    update_sql = f"""
        UPDATE {LOG_SCHEMA}.{META_DATA}
        SET
            is_pr_cleaned   = 'YES',
            count_pr        = :cleaned_count,
            pr_removed_cnt  = :removed_count,
            timestamp       = NOW()
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
        f"✅ PR metadata updated for {table_name} | "
        f"count_pr={cleaned_count}, pr_removed_cnt={removed_count}"
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
# Common helper: apply PR column mapping
# ---------------------------------------------------------
def apply_pr_column_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply PR column mapping to the given dataframe.
    """
    print("\n🔧 [PR Column Mapping] Applying mapping...")

    df.columns = [col.strip().lower() for col in df.columns]

    normalized_mapping = {
        k.strip().lower(): v for k, v in PR_COLUMN_MAPPING.items()
    }

    mapped_columns = set(df.columns).intersection(normalized_mapping.keys())
    used_mapping = {col: normalized_mapping[col] for col in mapped_columns}

    df.rename(columns=used_mapping, inplace=True)

    print(f"🔄 Total columns mapped: {len(used_mapping)}")
    for old_col, new_col in used_mapping.items():
        print(f"   🔹 '{old_col}' → '{new_col}'")

    print("✅ Mapping completed.\n")
    return df


# ---------------------------------------------------------
# Chunk load
# ---------------------------------------------------------
def load_chunked(df: pd.DataFrame, table_name: str, schema: str) -> None:
    """
    Load dataframe to DB in chunks using OUTER_CHUNK / INNER_CHUNK.
    """
    total_rows = len(df)
    print(f"\n🚀 Starting LOAD → {schema}.{table_name}")
    print(f"Total rows = {total_rows}")

    # Step 1: create DB connection only for loading
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    first = True  # controls replace vs append

    # Step 2: outer loop chunk (10k)
    for start in range(0, total_rows, OUTER_CHUNK):
        end = min(start + OUTER_CHUNK, total_rows)
        chunk_df = df.iloc[start:end]

        print(f"➡ Loading rows {start} → {end}")

        write_mode = "replace" if first else "append"

        # Step 3: safe engine.begin()
        with engine.begin() as conn:
            chunk_df.to_sql(
                name=table_name,
                schema=schema,
                con=conn,
                if_exists=write_mode,
                index=False,
                chunksize=INNER_CHUNK,
                method="multi",
            )

        first = False

    print(f"✔ LOAD COMPLETE → {schema}.{table_name}\n")


# ---------------------------------------------------------
# Removed log loading
# ---------------------------------------------------------
def load_removed(df_removed: pd.DataFrame, table_name: str) -> None:
    """
    Load removed rows into log schema.
    """
    if df_removed.empty:
        print("ℹ No removed rows.")
        return

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        df_removed.to_sql(
            name=table_name,
            schema=LOG_SCHEMA,
            con=conn,
            if_exists="replace",
            index=False,
            chunksize=INNER_CHUNK,
        )

    print(f"🧾 Logged removed rows → {LOG_SCHEMA}.{table_name}")


# ---------------------------------------------------------
# PR 2022 cleaning
# ---------------------------------------------------------
def clean_pr_2022(**context) -> None:
    """
    Clean PR 2022 data and load cleaned + removed rows.
    """
    del context  # unused

    print("\n▶▶ clean_pr_2022 started")

    # Step 1: connect only for reading
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    read_engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        text(f'SELECT * FROM "{STAGE_SCHEMA}"."pr_2022"'),
        con=read_engine,
    )
    print(f"📊 Loaded rows: {len(df)}")

    if df.empty:
        return

    # Step 2: disconnect before cleaning
    read_engine.dispose()
    print("🔌 Closed DB connection before cleaning.\n")

    # Cleaning (no open DB connection)
    df = apply_pr_column_mapping(df)

    # Add missing columns
    if "zone" not in df.columns:
        df["zone"] = None
        print("✅ PR 2022 → zone column added with None")

    if "vehicle_segment" not in df.columns:
        df["vehicle_segment"] = None
        print("✅ PR 2022 → vehicle_segment column added with None")

    # Clean policy_no
    if "policy_no" in df.columns:
        print("🧹 Cleaning policy_no ...")

        df["clean_policy_no"] = df["policy_no"].apply(clean_policy_number)

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

    if (
        "total_od_premium" in df.columns
        and "total_tp_premium" in df.columns
    ):
        df["gst"] = (
            df["total_od_premium"].astype(float)
            + df["total_tp_premium"].astype(float)
        ) * 0.18

        df["total_premium_payable"] = (
            df["total_od_premium"].astype(float)
            + df["total_tp_premium"].astype(float)
            + df["gst"]
        )

    if "nop" in df.columns:
        before = len(df)
        df = df[df["nop"] == 1]
        print(f"📉 NOP: {before} → {len(df)}")

    def prioritize_duplicates(group: pd.DataFrame) -> pd.Series:
        latest = group["policy_issue_date"].max()
        candidates = group[group["policy_issue_date"] == latest]
        pos = candidates[candidates["net_premium"] >= 0]
        if not pos.empty:
            return pos.loc[pos["net_premium"].idxmax()]
        return candidates.iloc[0]

    df_clean = (
        df.groupby(
            ["policy_no", "policy_start_date", "policy_end_date"],
            group_keys=False,
        )
        .apply(prioritize_duplicates)
        .reset_index(drop=True)
    )

    removed = (
        pd.concat(
            [
                df.reset_index(drop=True),
                df_clean.reset_index(drop=True),
            ]
        )
        .drop_duplicates(keep=False)
        .reset_index(drop=True)
    )

    removed = removed.reset_index(drop=True)
    invalid_policy_rows = invalid_policy_rows.reset_index(drop=True)
    removed = pd.concat([removed, invalid_policy_rows], ignore_index=True)

    print(f"📊 Cleaned rows: {len(df_clean)}")
    print(f"📊 Removed rows: {len(removed)}")

    cleaned_count = len(df_clean)
    removed_count = len(removed)

    load_chunked(df_clean, "pr_2022", TARGET_SCHEMA)
    load_removed(removed, PR22_DUP_TABLE)

    update_pr_metadata(
        table_name="pr_2022",
        cleaned_count=cleaned_count,
        removed_count=removed_count,
    )

    print("🎉 PR 2022 Completed.\n")


# ---------------------------------------------------------
# PR 2023 cleaning
# ---------------------------------------------------------
def clean_pr_2023(**context) -> None:
    """
    Clean PR 2023 data and load cleaned + removed rows.
    """
    del context  # unused

    print("\n▶▶ Starting clean_pr_2023() ...")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        text(f'SELECT * FROM "{STAGE_SCHEMA}"."pr_2023"'),
        con=engine,
    )

    print(f"📊 [PR 2023] Loaded rows: {len(df)}")

    if df.empty:
        return

    df = apply_pr_column_mapping(df)

    if "vehicle_segment" not in df.columns:
        df["vehicle_segment"] = None
        print("✅ PR 2023 → vehicle_segment column added with None")

    # Clean and normalize zone for PR 2023
    if "zone" in df.columns:
        df["zone"] = df["zone"].astype(str).str.upper().str.strip()

        df["zone"] = df["zone"].replace(
            {
                "NORTH ZONE": "NORTH",
                "SOUTH ZONE": "SOUTH",
                "WEST ZONE": "WEST",
                "EAST": "EAST",
                "CORPORATE OFFICE": None,
            }
        )

        print(
            "✅ PR 2023 → zone cleaned "
            "(NORTH/SOUTH/EAST/WEST, Corporate → NULL)"
        )
    else:
        df["zone"] = None
        print("✅ PR 2023 → zone column missing, added as NULL")

    # Clean policy_no
    if "policy_no" in df.columns:
        print("🧹 Cleaning policy_no ...")

        df["clean_policy_no"] = df["policy_no"].apply(clean_policy_number)

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

    if (
        "total_od_premium" in df.columns
        and "total_tp_premium" in df.columns
    ):
        df["gst"] = (
            df["total_od_premium"].astype(float)
            + df["total_tp_premium"].astype(float)
        ) * 0.18

        df["total_premium_payable"] = (
            df["total_od_premium"].astype(float)
            + df["total_tp_premium"].astype(float)
            + df["gst"]
        )

    if "previous_policy" not in df.columns:
        raise ValueError("Missing previous_policy after mapping")

    df_sorted = df.sort_values("policy_issue_date", ascending=False)
    df_clean = df_sorted.drop_duplicates(
        subset="previous_policy",
        keep="first",
    )

    removed = (
        pd.concat(
            [
                df.reset_index(drop=True),
                df_clean.reset_index(drop=True),
            ]
        )
        .drop_duplicates(keep=False)
        .reset_index(drop=True)
    )

    removed = removed.reset_index(drop=True)
    invalid_policy_rows = invalid_policy_rows.reset_index(drop=True)
    removed = pd.concat([removed, invalid_policy_rows], ignore_index=True)

    print(f"📊 Cleaned rows: {len(df_clean)}")
    print(f"📊 Removed: {len(removed)}")

    cleaned_count = len(df_clean)
    removed_count = len(removed)

    load_chunked(df_clean, "pr_2023", TARGET_SCHEMA)
    load_removed(removed, PR23_DUP_TABLE)

    update_pr_metadata(
        table_name="pr_2023",
        cleaned_count=cleaned_count,
        removed_count=removed_count,
    )

    print("🎉 PR 2023 completed.\n")


# ---------------------------------------------------------
# PR 2024 cleaning
# ---------------------------------------------------------
def clean_pr_2024(**context) -> None:
    """
    Clean PR 2024 data and load cleaned + removed rows.
    """
    del context  # unused

    print("\n▶▶ Starting clean_pr_2024() ...")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        text(f'SELECT * FROM "{STAGE_SCHEMA}"."pr_2024"'),
        con=engine,
    )

    print(f"📊 [PR 2024] Loaded rows: {len(df)}")

    if df.empty:
        return

    df = apply_pr_column_mapping(df)

    if "zone" not in df.columns:
        df["zone"] = None
        print("✅ PR 2024 → zone column added with None")

    # Clean policy_no
    if "policy_no" in df.columns:
        print("🧹 Cleaning policy_no ...")

        df["clean_policy_no"] = df["policy_no"].apply(clean_policy_number)

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

    if (
        "total_od_premium" in df.columns
        and "total_tp_premium" in df.columns
    ):
        df["gst"] = (
            df["total_od_premium"].astype(float)
            + df["total_tp_premium"].astype(float)
        ) * 0.18

        df["total_premium_payable"] = (
            df["total_od_premium"].astype(float)
            + df["total_tp_premium"].astype(float)
            + df["gst"]
        )

    print("total od premium cleaning completed")

    def prioritize_duplicates(group: pd.DataFrame) -> pd.Series:
        latest = group["policy_issue_date"].max()
        latest_rows = group[group["policy_issue_date"] == latest]
        positive = latest_rows[latest_rows["net_premium"] >= 0]

        if not positive.empty:
            return positive.loc[positive["net_premium"].idxmax()]
        return latest_rows.iloc[0]

    df_clean = (
        df.groupby(
            ["policy_no", "policy_start_date", "policy_end_date"],
            group_keys=False,
        )
        .apply(prioritize_duplicates)
        .reset_index(drop=True)
    )
    print("prioritization completed")

    removed = (
        pd.concat(
            [
                df.reset_index(drop=True),
                df_clean.reset_index(drop=True),
            ]
        )
        .drop_duplicates(keep=False)
        .reset_index(drop=True)
    )

    removed = removed.reset_index(drop=True)
    invalid_policy_rows = invalid_policy_rows.reset_index(drop=True)

    # Drop duplicate columns before concat
    removed = removed.loc[:, ~removed.columns.duplicated()]
    invalid_policy_rows = invalid_policy_rows.loc[
        :, ~invalid_policy_rows.columns.duplicated()
    ]
    removed = pd.concat([removed, invalid_policy_rows], ignore_index=True)

    print(f"📊 Cleaned: {len(df_clean)}")
    print(f"📊 Removed: {len(removed)}")

    cleaned_count = len(df_clean)
    removed_count = len(removed)

    load_chunked(df_clean, "pr_2024", TARGET_SCHEMA)
    load_removed(removed, PR24_DUP_TABLE)

    update_pr_metadata(
        table_name="pr_2024",
        cleaned_count=cleaned_count,
        removed_count=removed_count,
    )

    print("🎉 PR 2024 completed.\n")


# ---------------------------------------------------------
# Airflow DAG
# ---------------------------------------------------------
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2024, 11, 1),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="pr_clean_lib_2022",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["pr_clean", "2022", "test_aggregation"],
# ) as dag:
#     clean_pr_2022_task = PythonOperator(
#         task_id="clean_pr_2022",
#         python_callable=clean_pr_2022,
#         provide_context=True,
#     )

#     clean_pr_2023_task = PythonOperator(
#         task_id="clean_pr_2023",
#         python_callable=clean_pr_2023,
#         provide_context=True,
#     )

#     clean_pr_2024_task = PythonOperator(
#         task_id="clean_pr_2024",
#         python_callable=clean_pr_2024,
#         provide_context=True,
#     )

#     [
#         clean_pr_2022_task,
#         clean_pr_2023_task,
#         clean_pr_2024_task,
#     ]
