import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from time import sleep

from schema_table_config import get_column_mapping, get_schema, get_log_tables

# ---------------------------------------------------------
# 🔧 Constants / Paths
# ---------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")
STAGE_SCHEMA = get_schema("stage", META_JSON)
TARGET_SCHEMA = get_schema("agg", META_JSON)
LOG_SCHEMA = get_schema("log", META_JSON)

PR22_DUP_TABLE = "pr_2022_removed_duplicates"
PR23_DUP_TABLE = "pr_2023_removed_duplicates"
PR24_DUP_TABLE = "pr_2024_removed_duplicates"

COLUMN_JSON = str(DAGS_DIR / "config" / "column_mapping.json")
PR_COLUMN_MAPPING = get_column_mapping("pr", COLUMN_JSON)

OUTER_CHUNK = 10000        # SAME as success scripts
INNER_CHUNK = 5000         # SAME as success scripts

# ---------------------------------------------------------
# Policy Number cleaning
# ---------------------------------------------------------
def clean_policy_number(value):
    """Ensures policy_number is numeric & removes leading `'`."""
    if pd.isna(value):
        return None
    value = str(value).strip().lstrip("'")
    return value if value.isdigit() else None

# ---------------------------------------------------------
# 🧩 Common helper: Apply PR column mapping
# ---------------------------------------------------------
def apply_pr_column_mapping(df: pd.DataFrame) -> pd.DataFrame:
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
# Chunk Load
# ---------------------------------------------------------
def load_chunked(df, table_name, schema):

    total_rows = len(df)
    print(f"\n🚀 Starting LOAD → {schema}.{table_name}")
    print(f"Total rows = {total_rows}")

    # STEP 1 — Create fresh DB connection *ONLY* for loading
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    first = True  # ⭐ IMPORTANT: controls replace vs append

    # STEP 2 — Perfect outer loop chunk (10k)
    for start in range(0, total_rows, OUTER_CHUNK):
        end = min(start + OUTER_CHUNK, total_rows)
        chunk_df = df.iloc[start:end]

        print(f"➡ Loading rows {start} → {end}")

        # ⭐ Determine write mode
        write_mode = "replace" if first else "append"

        # STEP 3 — Safe engine.begin() (SUCCESS SCRIPT PATTERN)
        with engine.begin() as conn:
            chunk_df.to_sql(
                name=table_name,
                schema=schema,
                con=conn,
                if_exists=write_mode,
                index=False,
                chunksize=INNER_CHUNK,
                method="multi"
            )

        first = False  # After first loop, switch to append

    print(f"✔ LOAD COMPLETE → {schema}.{table_name}\n")


# ---------------------------------------------------------
# Removed Log loading
# ---------------------------------------------------------
def load_removed(df_removed, table_name):

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
            chunksize=INNER_CHUNK
        )

    print(f"🧾 Logged removed rows → {LOG_SCHEMA}.{table_name}")


# ---------------------------------------------------------
# 🧼 PR 2022 CLEANING
# ---------------------------------------------------------
def clean_pr_2022(**context):

    print("\n▶▶ clean_pr_2022 started")

    # ❗ STEP 1 — CONNECT ONLY FOR READING
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    read_engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        text(f'SELECT * FROM "{STAGE_SCHEMA}"."pr_2022"'),
        con=read_engine,
    )
    print(f"📊 Loaded rows: {len(df)}")

    if df.empty:
        return

    # ❗ STEP 2 — DISCONNECT (VERY IMPORTANT — SUCCESS SCRIPT PATTERN)
    read_engine.dispose()
    print("🔌 Closed DB connection before cleaning.\n")

    # CLEANING (safe, no open DB connection)
    df = apply_pr_column_mapping(df)
    # ✅ Add missing zone column for PR 2022
    if "zone" not in df.columns:
        df["zone"] = None
        print("✅ PR 2022 → zone column added with None")
    
    if "vehicle_segment" not in df.columns:
        df["vehicle_segment"] = None
        print("✅ PR 2022 → vehicle_segment column added with None")

    # --- CLEAN POLICY NUMBER (same as BASE cleaning) ---
    if "policy_no" in df.columns:
        print("🧹 Cleaning policy_no ...")

        df["clean_policy_no"] = df["policy_no"].apply(clean_policy_number)

        invalid_policy_rows = df[df["clean_policy_no"].isna()].copy()
        invalid_policy_rows["removal_reason"] = "Invalid policy_no"

        df = df[df["clean_policy_no"].notna()].copy()
        df["policy_no"] = df["clean_policy_no"]
        df.drop(columns=["clean_policy_no"], inplace=True)

        print(f"🧹 Invalid policy_no removed: {len(invalid_policy_rows)}")
    else:
        invalid_policy_rows = pd.DataFrame()  # no policy_no in table

    date_cols = [
                "policy_start_date",
                "policy_end_date",
                "policy_issue_date"
            ]

    for col in date_cols:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], errors="coerce")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            print(f"🗓 Normalized {col} → YYYY-MM-DD format")

    if "total_od_premium" in df.columns and "total_tp_premium" in df.columns:
        df["gst"] = (
            (df["total_od_premium"].astype(float)) +
            (df["total_tp_premium"].astype(float))
        ) * 0.18

        df["total_premium_payable"] = (
            df["total_od_premium"].astype(float) +
            df["total_tp_premium"].astype(float) +
            df["gst"]
        )

    if "nop" in df.columns:
        before = len(df)
        df = df[df["nop"] == 1]
        print(f"📉 NOP: {before} → {len(df)}")

    # for col in ["policy_start_date", "policy_end_date", "policy_issue_date"]:
    #     if col in df.columns:
    #         df[col] = pd.to_datetime(df[col], errors="coerce")

    def prioritize_duplicates(group):
        latest = group["policy_issue_date"].max()
        candidates = group[group["policy_issue_date"] == latest]
        pos = candidates[candidates["net_premium"] >= 0]
        if not pos.empty:
            return pos.loc[pos["net_premium"].idxmax()]
        return candidates.iloc[0]

    df_clean = (
        df.groupby(["policy_no", "policy_start_date", "policy_end_date"],
                   group_keys=False)
        .apply(prioritize_duplicates)
        .reset_index(drop=True)
    )

    removed = (
        pd.concat([
            df.reset_index(drop=True),
            df_clean.reset_index(drop=True)
        ])
        .drop_duplicates(keep=False)
        .reset_index(drop=True)
    )

    removed = removed.reset_index(drop=True)
    invalid_policy_rows = invalid_policy_rows.reset_index(drop=True)
    removed = pd.concat([removed, invalid_policy_rows], ignore_index=True)

    print(f"📊 Cleaned rows: {len(df_clean)}")
    print(f"📊 Removed rows: {len(removed)}")

    # ❗ STEP 3 — LOAD USING PERFECT SUCCESS LOADER
    load_chunked(df_clean, "pr_2022", TARGET_SCHEMA)

    # ❗ STEP 4 — LOG REMOVED
    load_removed(removed, PR22_DUP_TABLE)

    print("🎉 PR 2022 Completed.\n")
# --------------------------------------------------------------------------
# 🧼 PR 2023 Cleaning
# --------------------------------------------------------------------------
def clean_pr_2023(**context):

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

    # ✅ Clean & normalize zone for PR 2023
    if "zone" in df.columns:
        df["zone"] = df["zone"].astype(str).str.upper().str.strip()

        df["zone"] = df["zone"].replace({
            "NORTH ZONE": "NORTH",
            "SOUTH ZONE": "SOUTH",
            "WEST ZONE": "WEST",
            "EAST": "EAST",
            "CORPORATE OFFICE": None
        })

        print("✅ PR 2023 → zone cleaned (NORTH/SOUTH/EAST/WEST, Corporate → NULL)")
    else:
        df["zone"] = None
        print("✅ PR 2023 → zone column missing, added as NULL")

    # --- CLEAN POLICY NUMBER (same as BASE cleaning) ---
    if "policy_no" in df.columns:
        print("🧹 Cleaning policy_no ...")

        df["clean_policy_no"] = df["policy_no"].apply(clean_policy_number)

        invalid_policy_rows = df[df["clean_policy_no"].isna()].copy()
        invalid_policy_rows["removal_reason"] = "Invalid policy_no"

        df = df[df["clean_policy_no"].notna()].copy()
        df["policy_no"] = df["clean_policy_no"]
        df.drop(columns=["clean_policy_no"], inplace=True)

        print(f"🧹 Invalid policy_no removed: {len(invalid_policy_rows)}")
    else:
        invalid_policy_rows = pd.DataFrame()  # no policy_no in table

    date_cols = [
                "policy_start_date",
                "policy_end_date",
                "policy_issue_date"
            ]

    for col in date_cols:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], errors="coerce")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            print(f"🗓 Normalized {col} → YYYY-MM-DD format")

    if "total_od_premium" in df.columns and "total_tp_premium" in df.columns:
        df["gst"] = (
            (df["total_od_premium"].astype(float)) +
            (df["total_tp_premium"].astype(float))
        ) * 0.18

        df["total_premium_payable"] = (
            df["total_od_premium"].astype(float) +
            df["total_tp_premium"].astype(float) +
            df["gst"]
        )
    if "previous_policy" not in df.columns:
        raise ValueError("Missing previous_policy after mapping")

    # df["policy_issue_date"] = pd.to_datetime(df["policy_issue_date"], errors="coerce")

    df_sorted = df.sort_values("policy_issue_date", ascending=False)
    df_clean = df_sorted.drop_duplicates(subset="previous_policy", keep="first")
    removed = (
        pd.concat([
            df.reset_index(drop=True),
            df_clean.reset_index(drop=True)
        ])
        .drop_duplicates(keep=False)
        .reset_index(drop=True)
    )

    removed = removed.reset_index(drop=True)
    invalid_policy_rows = invalid_policy_rows.reset_index(drop=True)
    removed = pd.concat([removed, invalid_policy_rows], ignore_index=True)

    print(f"📊 Cleaned rows: {len(df_clean)}")
    print(f"📊 Removed: {len(removed)}")

    # ❗ STEP 3 — LOAD USING PERFECT SUCCESS LOADER
    load_chunked(df_clean, "pr_2023", TARGET_SCHEMA)

    # ❗ STEP 4 — LOG REMOVED
    load_removed(removed, PR23_DUP_TABLE)
    print("🎉 PR 2023 completed.\n")


# --------------------------------------------------------------------------
# 🧼 PR 2024 Cleaning
# --------------------------------------------------------------------------
def clean_pr_2024(**context):

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
    # ✅ Add missing zone column for PR 2024
    if "zone" not in df.columns:
        df["zone"] = None
        print("✅ PR 2024 → zone column added with None")

    # --- CLEAN POLICY NUMBER (same as BASE cleaning) ---
    if "policy_no" in df.columns:
        print("🧹 Cleaning policy_no ...")

        df["clean_policy_no"] = df["policy_no"].apply(clean_policy_number)

        invalid_policy_rows = df[df["clean_policy_no"].isna()].copy()
        invalid_policy_rows["removal_reason"] = "Invalid policy_no"

        df = df[df["clean_policy_no"].notna()].copy()
        df["policy_no"] = df["clean_policy_no"]
        df.drop(columns=["clean_policy_no"], inplace=True)

        print(f"🧹 Invalid policy_no removed: {len(invalid_policy_rows)}")
    else:
        invalid_policy_rows = pd.DataFrame()  # no policy_no in table

    date_cols = [
                "policy_start_date",
                "policy_end_date",
                "policy_issue_date"
            ]

    for col in date_cols:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], errors="coerce")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            print(f"🗓 Normalized {col} → YYYY-MM-DD format")

    if "total_od_premium" in df.columns and "total_tp_premium" in df.columns:
        df["gst"] = (
            (df["total_od_premium"].astype(float)) +
            (df["total_tp_premium"].astype(float))
        ) * 0.18

        df["total_premium_payable"] = (
            df["total_od_premium"].astype(float) +
            df["total_tp_premium"].astype(float) +
            df["gst"]
        )
    print("total od preamium cleaning completed")
    # for col in ["policy_issue_date"]:
    #     df[col] = pd.to_datetime(df[col], errors="coerce")

    def prioritize_duplicates(group):
        latest = group["policy_issue_date"].max()
        latest_rows = group[group["policy_issue_date"] == latest]
        positive = latest_rows[latest_rows["net_premium"] >= 0]

        if not positive.empty:
            return positive.loc[positive["net_premium"].idxmax()]
        return latest_rows.iloc[0]

    df_clean = (
        df.groupby(
            ["policy_no", "policy_start_date", "policy_end_date"],
            group_keys=False
        ).apply(prioritize_duplicates).reset_index(drop=True)
    )
    print("prioritization completed")
    removed = (
        pd.concat([
            df.reset_index(drop=True),
            df_clean.reset_index(drop=True)
        ])
        .drop_duplicates(keep=False)
        .reset_index(drop=True)
    )

    removed = removed.reset_index(drop=True)
    invalid_policy_rows = invalid_policy_rows.reset_index(drop=True)
    
    # ✅ FIX: Drop duplicate columns before concat (IMPORTANT)
    removed = removed.loc[:, ~removed.columns.duplicated()]
    invalid_policy_rows = invalid_policy_rows.loc[:, ~invalid_policy_rows.columns.duplicated()]
    removed = pd.concat([removed, invalid_policy_rows], ignore_index=True)

    print(f"📊 Cleaned: {len(df_clean)}")
    print(f"📊 Removed: {len(removed)}")

    # ❗ STEP 3 — LOAD USING PERFECT SUCCESS LOADER
    load_chunked(df_clean, "pr_2024", TARGET_SCHEMA)

    # ❗ STEP 4 — LOG REMOVED
    load_removed(removed, PR24_DUP_TABLE)

    print("🎉 PR 2024 completed.\n")



# ---------------------------------------------------------
# 🚀 AIRFLOW DAG
# ---------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pr_clean_lib_2022",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["pr_clean", "2022", "test_aggregation"],
) as dag:

    clean_pr_2022_task = PythonOperator(
        task_id="clean_pr_2022",
        python_callable=clean_pr_2022,
        provide_context=True,
    )

    clean_pr_2023_task = PythonOperator(
        task_id="clean_pr_2023",
        python_callable=clean_pr_2023,
        provide_context=True,
    )

    clean_pr_2024_task = PythonOperator(
        task_id="clean_pr_2024",
        python_callable=clean_pr_2024,
        provide_context=True,
    )


    [clean_pr_2022_task,clean_pr_2023_task,clean_pr_2024_task] 
    # clean_pr_2023_task