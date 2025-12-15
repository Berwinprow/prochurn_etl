from airflow import DAG
from airflow.operators.python import (
    PythonOperator,
)
from airflow.providers.postgres.hooks.postgres import (
    PostgresHook,
)
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy import text
from dateutil.relativedelta import relativedelta
from pathlib import Path
import pandas as pd
import re

from schema_table_config import (
    get_schema,
    get_log_tables,
)

# --------------------------------------------------------------------
# CONFIG (Hardcoded)
# --------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(
    DAGS_DIR / "config" / "schema_metadata_config.json"
)

SOURCE_SCHEMA = get_schema("agg", META_JSON)
SOURCE_TABLE_1 = "base_merged_temp"
SOURCE_TABLE_2 = "pr_merged_temp"

TARGET_TABLE = "appended_base_and_pr"
TARGET_SCHEMA = get_schema("agg", META_JSON)
TARGET_TABLE_2 = (
    "appended_base_and_pr_basic_clean"
)

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
LOG_SCHEMA = get_schema("log", META_JSON)

META_LOG = get_log_tables("metadata", META_JSON)
FEATURE_ENG_LOG = get_log_tables(
    "featurelog", META_JSON
)

OUTER_CHUNK = 10000
INNER_CHUNK = 5000

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
# Load Data To Postgres
# --------------------------------------------------------------------
def safe_load(df, table_name, schema):
    total_rows = len(df)
    print(
        f"\n🚀 Loading → {schema}.{table_name} "
        f"({total_rows} rows)"
    )

    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )
    engine = hook.get_sqlalchemy_engine()

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
# Universal BasePR Metadata Updater
# --------------------------------------------------------------------
def update_basepr_log(engine, mode, **kwargs):

    today = datetime.utcnow().date()

    if mode == "append":
        query = f"""
            INSERT INTO {LOG_SCHEMA}.{FEATURE_ENG_LOG}(
                last_run_date,
                is_basepr_appended,
                base_pr_appended_name,
                base_pr_cnt,
                timestamp
            )
            VALUES (:dt, 'YES', :tbl, :cnt, NOW())
            ON CONFLICT (last_run_date) DO UPDATE
            SET is_basepr_appended   = 'YES',
                base_pr_appended_name = :tbl,
                base_pr_cnt           = :cnt,
                timestamp             = NOW();
        """

        params = {
            "dt": today,
            "tbl": kwargs["table"],
            "cnt": kwargs["count"],
        }

    elif mode == "clean":
        query = f"""
            UPDATE {LOG_SCHEMA}.{FEATURE_ENG_LOG}
            SET appended_basepr_cleaned = 'YES',
                appended_basepr_name     = :tbl,
                basepr_cleaned_cnt       = :clean_cnt,
                basepr_removed_cnt       = :rem_cnt,
                timestamp                = NOW()
            WHERE last_run_date = :dt;
        """

        params = {
            "dt": today,
            "tbl": kwargs["table"],
            "clean_cnt": kwargs["clean_count"],
            "rem_cnt": kwargs["removed_count"],
        }

    else:
        raise ValueError(
            "❌ Unsupported mode. Use 'append' or 'clean'."
        )

    with engine.begin() as conn:
        conn.execute(text(query), params)

    print(f"📌 Metadata updated ({mode}) → {params}")


# --------------------------------------------------------------------
# Final Merge: Base + PR
# --------------------------------------------------------------------
def append_basepr(**context):
    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )
    engine = hook.get_sqlalchemy_engine()

    base_df = pd.read_sql(
        f'SELECT * FROM "{TARGET_SCHEMA}".{SOURCE_TABLE_1}',
        engine,
    )
    pr_df = pd.read_sql(
        f'SELECT * FROM "{TARGET_SCHEMA}".{SOURCE_TABLE_2}',
        engine,
    )
    print("📥 Fetched merged Base and PR tables")

    if "previous_policy" not in base_df.columns:
        base_df["previous_policy"] = None

    print("🛠️ Ensured 'previous_policy' column exists")

    for col in base_df.columns:
        if col not in pr_df.columns:
            pr_df[col] = None

    for col in pr_df.columns:
        if col not in base_df.columns:
            base_df[col] = None

    print("🧩 Base & PR columns aligned")

    final_df = pd.concat(
        [base_df, pr_df],
        ignore_index=True,
    )

    # 8) Write merged output
    safe_to_sql(
        final_df,
        TARGET_TABLE,
        TARGET_SCHEMA,
        engine,
        if_exists="replace",
    )

    print(
        f"✅ Final merged Base+PR written → "
        f"{TARGET_SCHEMA}.{TARGET_TABLE} "
        f"({len(final_df)} rows)"
    )

    # update_basepr_log(
    #     engine,
    #     mode="append",
    #     table=TARGET_TABLE,
    #     count=len(final_df),
    # )

    print("🏁 BasePR append process complete.\n")


# --------------------------------------------------------------------
# MAIN CLEANING FUNCTION
# --------------------------------------------------------------------
def clean_appended_base_pr():
    print("🔗 Connecting to PostgreSQL...")
    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )
    engine = hook.get_sqlalchemy_engine()

    query = (
        f"SELECT * FROM {SOURCE_SCHEMA}."
        f"{TARGET_TABLE};"
    )
    df = pd.read_sql(query, con=engine)

    print(f"✅ Loaded {len(df)} rows from {TARGET_TABLE}")

    df["policy_start_date"] = pd.to_datetime(
        df["policy_start_date"],
        errors="coerce",
    )
    df["policy_end_date"] = pd.to_datetime(
        df["policy_end_date"],
        errors="coerce",
    )

    before_df = df.copy()

    df = df.dropna(
        subset=["policy_start_date", "policy_end_date"]
    )
    removed = before_df[
        before_df["policy_start_date"].isna()
        | before_df["policy_end_date"].isna()
    ]

    print(
        f"🗑️ Removed {len(removed)} rows due to "
        "missing start/end date"
    )

    removed_start = removed[
        "policy_start_date"
    ].unique()
    removed_end = removed[
        "policy_end_date"
    ].unique()

    print("🟠 Distinct removed policy_start_date:", removed_start)
    print("🟠 Distinct removed policy_end_date:", removed_end)

    df["total_premium_payable"] = pd.to_numeric(
        df["total_premium_payable"]
        .astype(str)
        .str.strip(),
        errors="coerce",
    )

    before_premium = len(df)
    print(f"before premium {before_premium}")

    df = df[
        df["total_premium_payable"].notnull()
        & (df["total_premium_payable"] > 0.01)
    ]
    print(f"after premium removal: {len(df)}")

    def calculate_tenure_exact(start_date, end_date):
        diff = relativedelta(end_date, start_date)
        return (
            diff.years * 12
            + diff.months
            + (diff.days >= 0)
        )

    before_tenure = len(df)
    print(f"before tenure calc: {before_tenure}")

    df["Policy_Tenure(check)"] = df.apply(
        lambda row: calculate_tenure_exact(
            row["policy_start_date"],
            row["policy_end_date"],
        ),
        axis=1,
    )

    df = df[df["Policy_Tenure(check)"] > 10]
    print(f"after tenure cal : {len(df)}")

    def prioritize_rows(group):
        group["null_count"] = group.isnull().sum(
            axis=1
        )
        group = group.sort_values(
            by=[
                "null_count",
                "booked",
                "policy_start_date",
            ],
            ascending=[
                True,
                False,
                True,
            ],
        )
        return group.iloc[0]

    duplicates = df[
        df.duplicated(
            subset=[
                "policy_no",
                "policy_start_date",
                "policy_end_date",
            ],
            keep=False,
        )
    ]

    cleaned_duplicates = (
        duplicates.groupby(
            [
                "policy_no",
                "policy_start_date",
                "policy_end_date",
            ]
        )
        .apply(prioritize_rows)
        .reset_index(drop=True)
    )

    df_cleaned = df.drop_duplicates(
        subset=[
            "policy_no",
            "policy_start_date",
            "policy_end_date",
        ],
        keep=False,
    )

    df_cleaned = pd.concat(
        [df_cleaned, cleaned_duplicates],
        ignore_index=True,
    )

    print(
        f"✅ After deduplication → "
        f"{len(df_cleaned)} rows remain"
    )

    def clean_name(name):
        return re.sub(
            r"[^a-zA-Z0-9]",
            "",
            str(name),
        ).lower()

    columns_to_clean = {
        "insured_name": "Cleaned_insured_name",
        "new_branch_name_2": "Cleaned_Branch_Name_2",
        "state": "Cleaned_State_2",
        "zone": "Cleaned_Zone_2",
        "chassis_no": "Cleaned_Chassis_Number",
        "engine_no": "Cleaned_Engine_Number",
        "veh_reg_no": "Cleaned_Reg_no",
    }

    for orig_col, new_col in columns_to_clean.items():
        if orig_col in df_cleaned.columns:
            df_cleaned[new_col] = df_cleaned[
                orig_col
            ].apply(clean_name)
            print(
                f"✨ Created cleaned column: "
                f"'{new_col}' from '{orig_col}'"
            )

    chassis_nulls = (
        df_cleaned["Cleaned_Chassis_Number"]
        .isnull()
        .sum()
        if "Cleaned_Chassis_Number"
        in df_cleaned.columns
        else "N/A"
    )

    engine_nulls = (
        df_cleaned["Cleaned_Engine_Number"]
        .isnull()
        .sum()
        if "Cleaned_Engine_Number"
        in df_cleaned.columns
        else "N/A"
    )

    print("🔍 Null count before write:")
    print(f"   - Cleaned Chassis Number: {chassis_nulls}")
    print(f"   - Cleaned Engine Number : {engine_nulls}")

    df_cleaned.columns = (
        df_cleaned.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    print(
        "📝 Column names normalized to "
        "lowercase_with_underscores"
    )

    print(
        f"💾 Writing cleaned data to "
        f"{TARGET_SCHEMA}.{TARGET_TABLE_2} ..."
    )

    print(f"final data : {len(df_cleaned)}")

    safe_load(df_cleaned, TARGET_TABLE_2, TARGET_SCHEMA)

    print(
        "✅ Successfully written "
        f"{len(df_cleaned)} cleaned rows to "
        f"{TARGET_SCHEMA}.{TARGET_TABLE_2}"
    )

    total_before = len(before_df)
    total_after = len(df_cleaned)

    removed_cnt = total_before - total_after

    update_basepr_log(
        engine,
        mode="clean",
        table=TARGET_TABLE_2,
        clean_count=total_after,
        removed_count=removed_cnt,
    )

    print("✅ Successfully updated the metadata")


# --------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------
# default_args = {
#     "owner": "airflow",
#     "start_date": datetime(2024, 2, 10),
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="clean_appended_base_pr_dag",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["cleaning", "policy", "postmerge"],
# ) as dag:

#     append_base_pr = PythonOperator(
#         task_id="append_basepr",
#         python_callable=append_basepr,
#         provide_context=True,
#     )

#     clean_task = PythonOperator(
#         task_id="cleaning_appended_data",
#         python_callable=clean_appended_base_pr,
#         provide_context=True,
#     )

#     append_base_pr >> clean_task
