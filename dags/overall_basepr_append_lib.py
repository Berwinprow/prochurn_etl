from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from datetime import datetime, timedelta
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.exc import SQLAlchemyError

# --------------------------------------------------------------------
# 🔧 CONFIG
# --------------------------------------------------------------------
SOURCE_SCHEMA = "test_aggregation"       # where base_% & pr_% tables exist
TARGET_SCHEMA = "test_aggregation"      # where final merged table will go
TARGET_TABLE = "appended_base_and_pr"
POSTGRES_CONN_ID = "postgres_cloud_prochurn"


# --------------------------------------------------------------------
# Safe Log to Sql
# --------------------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def safe_to_sql(df, table_name, schema, engine, if_exists="replace", chunksize=50000, method="multi"):
   
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
                method=method
            )

        print(f"✅ Successfully loaded {len(df)} rows into {schema}.{table_name}")

    except SQLAlchemyError as e:
        print(f"❌ SQL write failed for {schema}.{table_name}: {e}")
        engine.dispose()
        raise

# --------------------------------------------------------------------
# 🧹 Common column cleaning
# --------------------------------------------------------------------
def clean_columns(df):
    """Standardize column names."""
    df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", " ", regex=True)
    return df

# --------------------------------------------------------------------
# 🧭 Get Source Table Names
# --------------------------------------------------------------------
def get_source_table(table_type="base"):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{SOURCE_SCHEMA}'
          AND table_name ~ '^{table_type}_[0-9]+$'
        ORDER BY table_name ASC;
        """
        results = conn.execute(text(query)).fetchall()
    table_list = [r[0] for r in results]
    print(f"🔍 Found {len(table_list)} {table_type} tables → {table_list}")
    return table_list

# --------------------------------------------------------------------
# 📦 Append Base Tables
# --------------------------------------------------------------------
def append_base(**context):
    """Load and combine Base tables from Postgres."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    source_tables = get_source_table("base")
    if not source_tables:
        print("⚠️ No base tables found.")
        return
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))
    print(f"{TARGET_SCHEMA} created sucessfully")
    all_base_dfs = []
    for tbl in source_tables:
        df = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA}"."{tbl}"', engine)
        df = clean_columns(df)
        df["data"] = tbl  # same logic as original code
        all_base_dfs.append(df)
        print(f"✅ Loaded {tbl} ({len(df)} rows)")

    # Find common columns across all base datasets
    common_base_columns = list(set.intersection(*(set(df.columns) for df in all_base_dfs)))
    print("✅ Common PR Columns:", common_base_columns)
    print(f"🧩 Common columns count: {len(common_base_columns)}")
    base_merged = pd.concat([df[common_base_columns] for df in all_base_dfs], ignore_index=True)

    # Write temporary merged Base
    safe_to_sql(base_merged, "base_merged_temp", TARGET_SCHEMA, engine, if_exists="replace")
    print(f"🎯 Base merged → {len(base_merged)} rows written to {TARGET_SCHEMA}.base_merged_temp")

# --------------------------------------------------------------------
# 📦 Append PR Tables (with old policy no logic)
# --------------------------------------------------------------------
def append_pr(**context):
    """Load and combine PR tables with old policy number merge (same business logic)."""
    print("🚀 Starting PR append process...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # -------------------------------
    # Step 1: Get PR Source Tables
    # -------------------------------
    source_tables = get_source_table("pr")
    if not source_tables:
        print("⚠️ No PR tables found.")
        return

    print(f"🔍 Found {len(source_tables)} PR tables: {source_tables}")

    # -------------------------------
    # Step 2: Load Each PR Table
    # -------------------------------
    all_pr_dfs = []
    for tbl in source_tables:
        df = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA}"."{tbl}"', engine)
        df = clean_columns(df)
        
        if "insured_name" not in df.columns:
            df["insured_name"] = None

        df["data"] = tbl
        all_pr_dfs.append(df)
        print(f"✅ Loaded {tbl} ({len(df)} rows, {len(df.columns)} columns)")

    # -------------------------------
    # Step 3: Merge All PR Tables
    # -------------------------------
    print("📦 Finding common columns across all PR datasets...")
    common_pr_columns = list(set.intersection(*(set(df.columns) for df in all_pr_dfs)))
    print("✅ Common PR Columns:", common_pr_columns)
    print(f"🧩 Common columns count: {len(common_pr_columns)}")

    pr_merged = pd.concat([df[common_pr_columns] for df in all_pr_dfs], ignore_index=True)
    print(f"✅ Combined PR merged → {len(pr_merged)} rows, {len(pr_merged.columns)} columns")

    # -------------------------------
    # Step 4: Align PR with Base Structure
    # -------------------------------
    base_tables = get_source_table("base")
    print(f"🧭 Found {len(base_tables)} Base tables for structure alignment: {base_tables}")

    sample_base = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA}"."{base_tables[0]}"', engine)
    sample_base = clean_columns(sample_base)
    print(f"🧩 Aligning PR structure with {len(sample_base.columns)} base columns...")

    for col in sample_base.columns:
        if col not in pr_merged.columns:
            pr_merged[col] = None
    print(f"✅ Alignment done — PR merged now has {len(pr_merged.columns)} columns total.")

    # -------------------------------
    # Step 5: Old Policy Merge Logic
    # -------------------------------
    pr_2023_table = next((t for t in source_tables if "2023" in t), None)
    if pr_2023_table:
        print(f"🔗 Merging old policy numbers from {pr_2023_table} ...")
        pr_2023 = pd.read_sql(f'SELECT * FROM "{SOURCE_SCHEMA}"."{pr_2023_table}"', engine)
        pr_2023 = clean_columns(pr_2023)

        if "previous_policy" in pr_2023.columns and "policy_no" in pr_merged.columns:
            before_merge = len(pr_merged)
            pr_merged = pr_merged.merge(
                pr_2023[["policy_no", "previous_policy"]],
                on="policy_no",
                how="left"
            )
            print(f"✅ Merge completed — total rows: {len(pr_merged)} (change: {len(pr_merged) - before_merge})")
        else:
            print("⚠️ Columns 'policy no' or 'old policy no' missing, skipping merge.")
    else:
        print("⚠️ No PR 2023 table found — skipping old policy merge.")

    # -------------------------------
    # Step 6: Write PR Merged Output
    # -------------------------------
    print(f"💾 Writing {len(pr_merged)} rows × {len(pr_merged.columns)} columns to {TARGET_SCHEMA}.pr_merged_temp ...")
    safe_to_sql(pr_merged, "pr_merged_temp", TARGET_SCHEMA, engine, if_exists="replace")
    print(f"✅ PR merged successfully written to {TARGET_SCHEMA}.pr_merged_temp")
    print("🏁 Completed PR append process.\n")

# --------------------------------------------------------------------
# 🔗 Final Merge: Base + PR
# --------------------------------------------------------------------
def append_basepr(**context):
    """Combine Base + PR merged data and store final output."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    base_df = pd.read_sql(f'SELECT * FROM "{TARGET_SCHEMA}"."base_merged_temp"', engine)
    pr_df = pd.read_sql(f'SELECT * FROM "{TARGET_SCHEMA}"."pr_merged_temp"', engine)
    print(f"fetched data from base and pr")
    # Ensure 'old policy no' exists in Base
    if "previous_policy" not in base_df.columns:
        base_df["previous_policy"] = None
    print(f"previous policy ensure done")
    # Align structures before merging
    for col in base_df.columns:
        if col not in pr_df.columns:
            pr_df[col] = None
    for col in pr_df.columns:
        if col not in base_df.columns:
            base_df[col] = None
    print(f'common columns and extra columns added to base and pr')
    final_df = pd.concat([base_df, pr_df], ignore_index=True)
    safe_to_sql(final_df, TARGET_TABLE, TARGET_SCHEMA, engine, if_exists="replace")
    print(f"✅ Final merged Base+PR written → {TARGET_SCHEMA}.{TARGET_TABLE} ({len(final_df)} rows)")

# --------------------------------------------------------------------
# 🪄 Airflow DAG Definition
# --------------------------------------------------------------------
with DAG(
    dag_id="basepr_append_full_logic",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2024, 2, 10),
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    schedule_interval=None,
    catchup=False,
    tags=["append", "merge", "postgres", "policy_link"]
) as dag:

    append_base_data = PythonOperator(
        task_id="append_base_tables",
        python_callable=append_base,
        provide_context=True
    )

    append_pr_data = PythonOperator(
        task_id="append_pr_tables",
        python_callable=append_pr,
        provide_context=True
    )

    append_base_pr = PythonOperator(
        task_id="final_merge_base_pr",
        python_callable=append_basepr,
        provide_context=True
    )

    [append_base_data, append_pr_data] >> append_base_pr
