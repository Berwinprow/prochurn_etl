from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential

from schema_table_config import get_schema, get_log_tables


# --------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(DAGS_DIR / "config" / "schema_metadata_config.json")


SOURCE_SCHEMA = get_schema("agg", META_JSON)

TARGET_SCHEMA = get_schema("agg", META_JSON)


POSTGRES_CONN_ID = "postgres_cloud_prochurn"
LOG_SCHEMA = get_schema("log", META_JSON)
META_LOG = get_log_tables("metadata", META_JSON)


# --------------------------------------------------------------------
# Safe log to SQL
# --------------------------------------------------------------------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def safe_to_sql(
    df,
    table_name,
    schema,
    engine,
    if_exists="replace",
    chunksize=50000,
    method="multi",
):
    """
    Safely write a DataFrame to SQL with retries.
    """
    if df.empty:
        print(f"⚠️ Skipping empty write to {schema}.{table_name}")
        return

    try:
        with engine.begin() as conn:
            df.to_sql(
                name=table_name,
                schema=schema,
                con=conn,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method=method,
            )

        print(
            "✅ Successfully loaded "
            f"{len(df)} rows into {schema}.{table_name}"
        )
    except SQLAlchemyError as exc:
        print(f"❌ SQL write failed for {schema}.{table_name}: {exc}")
        engine.dispose()
        raise


# --------------------------------------------------------------------
# Common column cleaning
# --------------------------------------------------------------------
def clean_columns(df):
    """
    Standardize column names.
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\\s+", " ", regex=True)
    )
    return df


# --------------------------------------------------------------------
# Update metadata after append (base / pr)
# --------------------------------------------------------------------
def update_append_metadata(
    engine,
    table_type,
    table_names,
    merged_table,
    per_table_counts,
):
    """
    Update metadata log after base / PR append.
    """
    if table_type == "base":
        flag_col = "is_base_appended"
        name_col = "base_appended_name"
        cnt_col = "base_appended_cnt"
    elif table_type == "pr":
        flag_col = "is_pr_appended"
        name_col = "pr_appended_name"
        cnt_col = "pr_appended_cnt"
    else:
        raise ValueError(f"Unsupported table_type: {table_type}")

    now = datetime.utcnow()

    query = f"""
        UPDATE {LOG_SCHEMA}.{META_LOG}
        SET {flag_col} = 'YES',
            {name_col} = :merged,
            {cnt_col} = :cnt,
            timestamp = :ts
        WHERE table_name = :tbl;
    """

    with engine.begin() as conn:
        for tbl in table_names:
            row_cnt = per_table_counts.get(tbl, 0)
            conn.execute(
                text(query),
                {
                    "merged": merged_table,
                    "cnt": row_cnt,
                    "ts": now,
                    "tbl": tbl,
                },
            )

    print(
        f"✅ Updated metadata ({table_type}) for tables "
        f"{table_names} → {merged_table}"
    )


# --------------------------------------------------------------------
# Get pending tables from metadata (base / pr)
# --------------------------------------------------------------------
def get_pending_table(engine, table_name):
    """
    Get list of tables pending append from metadata.
    """
    if table_name == "base":
        extra_condition = (
            "is_base_cleaned = 'YES' AND is_base_appended = 'NO'"
        )
    elif table_name == "pr":
        extra_condition = (
            "is_pr_cleaned = 'YES' AND is_pr_appended = 'NO'"
        )
    else:
        raise ValueError(
            f"Unsupported table_name: {table_name} "
            "(expected 'base' or 'pr')"
        )

    query = f"""
        SELECT table_name
        FROM {LOG_SCHEMA}.{META_LOG}
        WHERE stage_loaded = 'YES'
          AND {extra_condition}
        ORDER BY year;
    """

    df = pd.read_sql(query, engine)
    tables = df["table_name"].tolist()
    print(f"🧾 Pending {table_name} tables from metadata: {tables}")
    return tables


# --------------------------------------------------------------------
# Get previous (already appended) merged table for base / pr
# --------------------------------------------------------------------
def get_previous_table(engine, table_name):
    """
    Get the latest merged table name for base / PR.
    """
    if table_name == "base":
        col_name = "base_appended_name"
    elif table_name == "pr":
        col_name = "pr_appended_name"
    else:
        raise ValueError(f"Unsupported table_name: {table_name}")

    query = f"""
        SELECT {col_name}
        FROM {LOG_SCHEMA}.{META_LOG}
        WHERE {col_name} IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 1;
    """

    with engine.begin() as conn:
        row = conn.execute(text(query)).fetchone()

    prev_table = row[0] if row else None
    print(
        "🧭 Previous appended "
        f"{table_name} table from metadata: {prev_table}"
    )
    return prev_table


# --------------------------------------------------------------------
# Append Base Tables (metadata driven)
# --------------------------------------------------------------------
def append_base(**context):
    """
    Append base tables based on metadata.
    """
    del context  # unused

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # 1) Get pending Base tables (not yet appended)
    pending_tables = get_pending_table(engine, "base")
    if not pending_tables:
        print("⚠️ No Base tables pending append.")
        return

    # 2) Get previous merged table (if exists)
    previous_merged = get_previous_table(engine, "base")

    # 3) Set final merged table name
    merged_table_name = previous_merged or "base_merged_temp"

    # 4) Ensure schema exists
    with engine.begin() as conn:
        conn.execute(
            text(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')
        )
    print(f"🏗️ Schema {TARGET_SCHEMA} ready.")

    # 5) Collect all DataFrames
    all_base_dfs = []
    per_table_counts = {}

    # 5A) Include previous merged table if exists
    if previous_merged:
        try:
            old_df = pd.read_sql(
                f'SELECT * FROM "{TARGET_SCHEMA}"."{previous_merged}"',
                engine,
            )
            old_df = clean_columns(old_df)
            all_base_dfs.append(old_df)
            print(
                "📂 Loaded previous merged Base: "
                f"{previous_merged} ({len(old_df)} rows)"
            )
        except Exception as exc:  # noqa: BLE001
            print(
                "⚠️ Could not load previous merged Base "
                f"`{previous_merged}`: {exc}"
            )

    # 5B) Add all new pending base tables
    for tbl in pending_tables:
        df = pd.read_sql(
            f'SELECT * FROM "{SOURCE_SCHEMA}"."{tbl}"',
            engine,
        )
        df = clean_columns(df)
        df["data"] = tbl
        all_base_dfs.append(df)
        per_table_counts[tbl] = len(df)
        print(f"✅ Loaded Base {tbl} ({len(df)} rows)")

    # 6) Find common columns
    common_base_columns = list(
        set.intersection(*(set(df.columns) for df in all_base_dfs))
    )
    print("🔗 Common Base Columns:", common_base_columns)
    print(f"🧩 Common column count: {len(common_base_columns)}")

    # 7) Merge everything together
    base_merged = pd.concat(
        [df[common_base_columns] for df in all_base_dfs],
        ignore_index=True,
    )

    # 8) Write merged output
    safe_to_sql(
        base_merged,
        merged_table_name,
        TARGET_SCHEMA,
        engine,
        if_exists="replace",
    )
    print(
        "🎯 Base merged successfully → "
        f'{len(base_merged)} rows written to '
        f'"{TARGET_SCHEMA}"."{merged_table_name}"'
    )

    # 9) Update metadata for only newly appended tables
    update_append_metadata(
        engine=engine,
        table_type="base",
        table_names=pending_tables,
        merged_table=merged_table_name,
        per_table_counts=per_table_counts,
    )

    print("🏁 Base append process complete.\n")


# --------------------------------------------------------------------
# Append PR tables (metadata driven with old policy logic)
# --------------------------------------------------------------------
def append_pr(**context):
    """
    Load and combine PR tables with old policy number merge.
    """
    del context  # unused

    print("🚀 Starting PR append process...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # Step 1: Get PR source tables (from metadata)
    pending_tables = get_pending_table(engine, "pr")
    if not pending_tables:
        print("⚠️ No PR tables pending append.")
        return

    # Latest already-appended PR merged table (if any)
    previous_merged = get_previous_table(engine, "pr")
    merged_pr_name = previous_merged or "pr_merged_temp"

    print(f"🔍 Pending PR tables: {pending_tables}")
    print(f"🧭 Previous merged PR table: {previous_merged}")

    # Ensure target schema exists
    with engine.begin() as conn:
        conn.execute(
            text(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')
        )
    print(f"🏗️ Schema {TARGET_SCHEMA} ready for PR merge.")

    # Step 2: Load previous merged + each pending PR table
    all_pr_dfs = []
    per_table_counts = {}

    # 2A) Add previous merged PR (if exists)
    if previous_merged:
        try:
            old_pr = pd.read_sql(
                f'SELECT * FROM "{TARGET_SCHEMA}"."{previous_merged}"',
                engine,
            )
            old_pr = clean_columns(old_pr)
            all_pr_dfs.append(old_pr)
            print(
                "📂 Loaded existing merged PR: "
                f"{previous_merged} ({len(old_pr)} rows)"
            )
        except Exception as exc:  # noqa: BLE001
            print(
                "⚠️ Could not load previous merged PR "
                f"`{previous_merged}`: {exc}"
            )

    # 2B) Load each new pending PR table
    for tbl in pending_tables:
        df = pd.read_sql(
            f'SELECT * FROM "{SOURCE_SCHEMA}"."{tbl}"',
            engine,
        )
        df = clean_columns(df)

        if "insured_name" not in df.columns:
            df["insured_name"] = None

        df["data"] = tbl
        all_pr_dfs.append(df)
        per_table_counts[tbl] = len(df)
        print(
            f"✅ Loaded {tbl} ({len(df)} rows, "
            f"{len(df.columns)} columns)"
        )

    # Step 3: Merge all PR tables
    print("📦 Finding common columns across all PR datasets...")
    common_pr_columns = list(
        set.intersection(*(set(df.columns) for df in all_pr_dfs))
    )
    print("✅ Common PR Columns:", common_pr_columns)
    print(f"🧩 Common columns count: {len(common_pr_columns)}")

    pr_merged = pd.concat(
        [df[common_pr_columns] for df in all_pr_dfs],
        ignore_index=True,
    )
    print(
        "✅ Combined PR merged → "
        f"{len(pr_merged)} rows, {len(pr_merged.columns)} columns"
    )

    # Step 4: Align PR with Base structure
    base_table = "base_2022"
    print(
        "🧭 Using base table for structure alignment: "
        f"{base_table}"
    )

    sample_base = pd.read_sql(
        f'SELECT * FROM "{SOURCE_SCHEMA}"."{base_table}"',
        engine,
    )
    sample_base = clean_columns(sample_base)
    print(
        "🧩 Aligning PR structure with "
        f"{len(sample_base.columns)} base columns..."
    )

    for col in sample_base.columns:
        if col not in pr_merged.columns:
            pr_merged[col] = None

    print(
        "✅ Alignment done — PR merged now has "
        f"{len(pr_merged.columns)} columns total."
    )

    # Step 5: Old policy merge logic
    pr_2023_table = next(
        (t for t in pending_tables if "2023" in t),
        None,
    )
    if pr_2023_table:
        print(
            f"🔗 Merging old policy numbers from {pr_2023_table} ..."
        )
        pr_2023 = pd.read_sql(
            f'SELECT * FROM "{SOURCE_SCHEMA}"."{pr_2023_table}"',
            engine,
        )
        pr_2023 = clean_columns(pr_2023)

        if (
            "previous_policy" in pr_2023.columns
            and "policy_no" in pr_merged.columns
        ):
            before_merge = len(pr_merged)
            pr_merged = pr_merged.merge(
                pr_2023[["policy_no", "previous_policy"]],
                on="policy_no",
                how="left",
            )
            print(
                "✅ Merge completed — total rows: "
                f"{len(pr_merged)} "
                f"(change: {len(pr_merged) - before_merge})"
            )
        else:
            print(
                "⚠️ Columns 'policy_no' or 'previous_policy' missing, "
                "skipping merge."
            )
    else:
        print(
            "⚠️ No PR 2023 table found — skipping old policy merge."
        )

    # Step 6: Write PR merged output
    print(
        "💾 Writing "
        f"{len(pr_merged)} rows × {len(pr_merged.columns)} columns "
        f"to {TARGET_SCHEMA}.{merged_pr_name} ..."
    )
    safe_to_sql(
        pr_merged,
        merged_pr_name,
        TARGET_SCHEMA,
        engine,
        if_exists="replace",
    )
    print(
        "✅ PR merged successfully written to "
        f'"{TARGET_SCHEMA}"."{merged_pr_name}"'
    )

    # Step 7: Update PR metadata
    update_append_metadata(
        engine=engine,
        table_type="pr",
        table_names=pending_tables,
        merged_table=merged_pr_name,
        per_table_counts=per_table_counts,
    )

    print("🏁 Completed PR append process.\n")


# --------------------------------------------------------------------
# Airflow DAG Definition
# --------------------------------------------------------------------
# default_args = {
#     "owner": "airflow",
#     "start_date": datetime(2024, 2, 10),
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="basepr_append_full_logic",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["append", "merge", "postgres", "policy_link"],
# ) as dag:
#     append_base_data = PythonOperator(
#         task_id="append_base_tables",
#         python_callable=append_base,
#         provide_context=True,
#     )

#     append_pr_data = PythonOperator(
#         task_id="append_pr_tables",
#         python_callable=append_pr,
#         provide_context=True,
#     )

#     append_base_data >> append_pr_data
