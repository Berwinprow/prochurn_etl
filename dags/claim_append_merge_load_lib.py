from datetime import datetime
import time
import gc
from pathlib import Path
import re

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import (
    Text,
    Integer,
    Float,
    DateTime,
)
from airflow.providers.postgres.hooks.postgres import (
    PostgresHook,
)
from airflow.operators.python import PythonOperator
from airflow import DAG

from schema_table_config import (
    get_schema,
    get_log_tables,
)


# ---------------------------------------------------------------------
# 🔧 Constants
# ---------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
JSON_PATH = str(
    DAGS_DIR / "config" / "schema_metadata_config.json"
)

POSTGRES_CONN_ID = "postgres_cloud_prochurn"

SOURCE_SCHEMA = get_schema("stage", JSON_PATH)
TARGET_SCHEMA = get_schema("agg", JSON_PATH)

TARGET_TABLE1 = "claim_append"
TARGET_TABLE2 = "claim_merge"

LOG_SCHEMA = get_schema("log", JSON_PATH)
CLAIM_LOG = get_log_tables("claimlog", JSON_PATH)
META_DATA = get_log_tables("metadata", JSON_PATH)


# ---------------------------------------------------------------------
# Removed Log Details
# ---------------------------------------------------------------------
def log_removed_rows(df_removed, reason, engine):
    """
    Log removed claim rows with reason into
    pip_log.claim_removed_reason_lib.
    """
    if df_removed.empty:
        return

    df_removed = df_removed.copy()
    df_removed["removal_reason"] = reason
    df_removed["logged_at"] = datetime.utcnow()

    df_removed.to_sql(
        name="claim_removed_reason_lib",
        schema=LOG_SCHEMA,
        con=engine,
        if_exists="replace",
        index=False,
    )

    print(
        f"⚠️ Logged {len(df_removed)} removed rows – "
        f"Reason: {reason}"
    )


# ---------------------------------------------------------------------
# Data Type Mapping
# ---------------------------------------------------------------------
def d_mapping(df):
    mapping = {}

    for col in df.columns:
        series = df[col]

        if pd.api.types.is_integer_dtype(series):
            mapping[col] = Integer()
        elif pd.api.types.is_float_dtype(series):
            mapping[col] = Float()
        elif pd.api.types.is_datetime64_any_dtype(series):
            mapping[col] = DateTime()
        else:
            mapping[col] = Text()

    return mapping


# ---------------------------------------------------------------------
# Fetch latest BasePR table from metadata
# ---------------------------------------------------------------------
def get_latest_basepr(engine):
    query = f"""
        SELECT renewal_policy_table
        FROM "{LOG_SCHEMA}"."{META_DATA}"
        WHERE renewal_policy_table IS NOT NULL
        ORDER BY last_updated_ts DESC
        LIMIT 1
    """

    result = engine.execute(text(query)).fetchone()
    return result[0] if result else None


# ---------------------------------------------------------------------
# Updating Metadata
# ---------------------------------------------------------------------
def update_claim_metadata(engine, table_name, row_count=None):
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
            UPDATE "{LOG_SCHEMA}"."{CLAIM_LOG}"
            SET is_appended = 'YES',
                appended_count = :row_count,
                last_updated_ts = :ts
            WHERE table_name = :table_name
            """
            ),
            {
                "table_name": table_name,
                "row_count": row_count or 0,
                "ts": datetime.utcnow(),
            },
        )


# ---------------------------------------------------------------------
# Claim tables list (fixed order)
# ---------------------------------------------------------------------
CLAIM_TABLES = [
    "claim_2022_part_1",
    "claim_2022_part_2",
    "claim_2023_part_1",
    "claim_2023_part_2",
    "claim_2024_part_1",
    "claim_2024_part_2",
]


# ---------------------------------------------------------------------
# Appending Claim Tables
# ---------------------------------------------------------------------
def append_claim_table():
    print("🔗 Initializing connection...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(
            text(
                f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}"'
            )
        )

    print("✔ Connection established")

    claim_tables = CLAIM_TABLES
    print("📌 CLAIM TABLES FOUND:", claim_tables)

    if not claim_tables:
        print("🚫 No claim tables found.")
        return

    dfs = []

    for tbl in claim_tables:
        print(f"📥 Loading table: {tbl}")

        df = pd.read_sql(
            f'SELECT * FROM pip_stage."{tbl}"',
            engine,
        )

        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        if (
            "status_of_claim.1" in df.columns
            and "updated_status" not in df.columns
        ):
            df.rename(
                columns={"status_of_claim.1": "updated_status"},
                inplace=True,
            )

        dfs.append(df)
        print(f"   ✔ Extracted {len(df)} rows")

    print("🧬 Combining all claim tables...")
    final_df = pd.concat(dfs, ignore_index=True)
    print(f"🔥 Total appended rows: {len(final_df)}")

    final_df.to_sql(
        name=TARGET_TABLE1,
        schema=TARGET_SCHEMA,
        con=engine,
        if_exists="replace",
        index=False,
    )

    print(
        f"✅ Successfully created {TARGET_SCHEMA}."
        f"{TARGET_TABLE1} with {len(final_df)} rows."
    )


# ---------------------------------------------------------------------
# Merging Claim Table with Status Aggregation
# ---------------------------------------------------------------------
def merge_claim_table():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    query = (
        f'SELECT * FROM "{TARGET_SCHEMA}"."{TARGET_TABLE1}"'
    )
    df = pd.read_sql(query, engine)

    def clean_name(name):
        return re.sub(r"[^a-zA-Z0-9]", "", str(name)).lower()

    df["cleaned_insured_name"] = df["insured_name"].apply(
        clean_name
    )
    print("clean done for insured name")

    group_cols = [
        "policy_no",
        "policy_start_date",
        "policy_end_date",
    ]
   
    df[group_cols] = df[group_cols].astype(str)

    df["number_of_claims"] = df.groupby(group_cols)[
        "policy_no"
    ].transform("count")
  
    status_cols = [
        "PAID",
        "WITHDRAWN",
        "CLOSURE OF CLAIM",
        "REPUDIATION",
        "CLOSURE OF CLAIMS",
    ]

    df_status = (
        df.groupby(group_cols)["updated_status"]
        .value_counts()
        .unstack()
        .fillna(0)
        .astype("Int64")
    )
  
    
    df_status = df_status.rename(
        columns={
            "PAID": "PAID",
            "WITHDRAWN": "WITHDRAWN",
            "CLOSURE OF CLAIM": "CLOSURE OF CLAIM",
            "REPUDIATION": "REPUDIATION",
            "CLOSURE OF CLAIMS": "CLOSURE OF CLAIMS",
        }
    )

    df["claim_status"] = (
        df["updated_status"]
        .map({"PAID": "Approved"})
        .fillna("Denied")
    )
   
    
    df_claim_status = (
        df.groupby(group_cols)["claim_status"]
        .value_counts()
        .unstack()
        .reindex(columns=["Approved", "Denied"], fill_value=0)
        .astype("Int64")
    )
    
    df["settle_date"] = pd.to_datetime(
        df["settle_date"],
        errors="coerce",
    )

    removed_dupes = df[
        df.duplicated(subset=group_cols, keep="last")
    ]
    log_removed_rows(
        removed_dupes,
        "Duplicate claim (keeping latest by settle_date)",
        engine,
    )

    df_latest = (
        df.sort_values(by="settle_date")
        .drop_duplicates(subset=group_cols, keep="last")
    )

    def resolve_duplicates(group):
        if len(group) > 1:
            group["null_count"] = group.isnull().sum(
                axis=1
            )
            return group.loc[group["null_count"].idxmin()]
        return group.iloc[0]

    df_latest_cleaned = (
        df_latest.groupby(group_cols)
        .apply(resolve_duplicates)
        .reset_index(drop=True)
    )

    df_final = pd.merge(
        df_latest_cleaned,
        df_status,
        on=group_cols,
        how="left",
    )
    df_final = pd.merge(
        df_final,
        df_claim_status,
        on=group_cols,
        how="left",
    )

    all_cols = (
        df.columns.tolist()
        + status_cols
        + ["Approved", "Denied", "number_of_claims"]
    )

    df_final = df_final[all_cols]
    object_cols = df_final.select_dtypes(include=["object"]).columns
    df_final[object_cols] = df_final[object_cols].fillna("")
    print("type changed")
    df_final.to_sql(
        name=TARGET_TABLE2,
        schema=TARGET_SCHEMA,
        con=engine,
        if_exists="replace",
        index=False,
    )
    print("loaded the data")
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
            INSERT INTO "{LOG_SCHEMA}"."{CLAIM_LOG}"
                (table_name, is_merged, merged_cnt,
                 timestamp)
            VALUES ('claim_merge', 'YES', :cnt, :ts)
            ON CONFLICT (table_name)
            DO UPDATE SET
                is_merged = 'YES',
                merged_cnt = :cnt,
                timestamp = :ts
            """
            ),
            {
                "cnt": len(df),
                "ts": datetime.utcnow(),
            },
        )

    print(
        f"✅ {len(df)} rows merged into "
        f"{TARGET_SCHEMA}.claim_merge"
    )


# ---------------------------------------------------------------------
# Merge BasePR with Claim Tables
# ---------------------------------------------------------------------
def merge_basepr_with_claim():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    print("engine connection done")

    engine.dispose()
    time.sleep(2)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    BASE_PR_TABLE = "mapping_old_policy"
    if not BASE_PR_TABLE:
        raise ValueError(
            "No renewal policy table found in metadata!"
        )

    CLAIM_TABLE = TARGET_TABLE2
    FINAL_TABLE = "overall_appended_basepr_claim"

    claim = pd.read_sql(
        f'SELECT * FROM "{TARGET_SCHEMA}"."{CLAIM_TABLE}"',
        con=engine,
    )

    for col in ["policy_start_date", "policy_end_date"]:
        if col in claim.columns:
            claim[col] = pd.to_datetime(
                claim[col], errors="coerce"
            )

    with engine.begin() as conn:
        conn.execute(
            text(
                f"DROP TABLE IF EXISTS "
                f"{TARGET_SCHEMA}.{FINAL_TABLE}"
            )
        )

    print("✅ Loaded claim table")

    chunk_size = 100000
    offset = 0
    first = True
    total_rows = 0

    while True:
        query = f"""
            SELECT *
            FROM "{TARGET_SCHEMA}"."{BASE_PR_TABLE}"
            ORDER BY policy_no
            OFFSET {offset}
            LIMIT {chunk_size}
        """

        base_pr = pd.read_sql(query, con=engine)

        if base_pr.empty:
            break

        for col in ["policy_start_date", "policy_end_date"]:
            if col in base_pr.columns:
                base_pr[col] = pd.to_datetime(
                    base_pr[col], errors="coerce"
                )

        merged = base_pr.merge(
            claim,
            on=[
                "policy_no",
                "policy_start_date",
                "policy_end_date",
            ],
            how="left",
            suffixes=("_policy", "_claim"),
        )

        print(
            f"✅ Merged chunk at offset {offset} "
            f"with {len(merged)} rows"
        )

        write_mode = "replace" if first else "append"

        merged.to_sql(
            name=FINAL_TABLE,
            schema=TARGET_SCHEMA,
            con=engine,
            if_exists=write_mode,
            index=False,
            dtype=d_mapping(merged),
        )

        total_rows += len(merged)
        offset += chunk_size
        first = False

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
            INSERT INTO "{LOG_SCHEMA}"."{CLAIM_LOG}"
                (table_name, is_basepr_claim_appended,
                 baseprclaim_cnt, timestamp)
            VALUES ('basepr_merged_with_claim', 'YES',
                    :cnt, :ts)
            ON CONFLICT (table_name)
            DO UPDATE SET
                is_basepr_claim_appended = 'YES',
                baseprclaim_cnt = :cnt,
                timestamp = :ts
            """
            ),
            {
                "cnt": total_rows,
                "ts": datetime.utcnow(),
            },
        )

    print(
        f"🎉 Successfully merged {total_rows} rows into "
        f"{TARGET_SCHEMA}.basepr_merged_with_claim"
    )

    engine.dispose()
    gc.collect()


# ---------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------
# with DAG(
#     dag_id="append_merge_claim_tables_dag",
#     default_args={
#         "owner": "airflow",
#         "start_date": datetime(2024, 1, 1),
#     },
#     schedule_interval=None,
#     catchup=False,
#     tags=["claim", "merge"],
# ) as dag:

#     append_claim = PythonOperator(
#         task_id="append_claim_tables",
#         python_callable=append_claim_table,
#     )

#     merge_claim = PythonOperator(
#         task_id="merge_claim_table",
#         python_callable=merge_claim_table,
#     )

#     final_table = PythonOperator(
#         task_id="basepr_append_with_claim",
#         python_callable=merge_basepr_with_claim,
#     )

#     append_claim >> merge_claim >> final_table
