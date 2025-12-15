from datetime import datetime, timedelta
import time
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import re

from schema_table_config import get_log_tables, get_schema


# ----------------------------------------------------------------------
# Config / constants
# ----------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parent
META_JSON = str(
    DAGS_DIR / "config" / "schema_metadata_config.json"
)

POSTGRES_CONN_ID = "postgres_cloud_prochurn"

SOURCE_TABLE = "policy_chain_feature"
SOURCE_SCHEMA = get_schema("agg", META_JSON)

TARGET_SCHEMA = get_schema("agg", META_JSON)
TARGET_SCHEMA_1 = get_schema("bi_dwh", META_JSON)

TARGET_TABLE_1 = "pricing_catlog"
TARGET_TABLE_2 = "final_policy_features"

LOG_SCHEMA = get_schema("log", META_JSON)
FEATURE_ENG_LOG = get_log_tables("featurelog", META_JSON)

OUTER_CHUNK = 10000
INNER_CHUNK = 5000


# ----------------------------------------------------------------------
# Update to metadata
# ----------------------------------------------------------------------
def update_new_feature_metadata(engine):
    # BEFORE count (optional) — from TARGET_TABLE_1
    before_cnt = pd.read_sql(
        text(
            f'''SELECT COUNT(*) AS cnt FROM "{TARGET_SCHEMA}"."{TARGET_TABLE_1}"'''
        ),
        con=engine,
    )["cnt"][0]

    # AFTER count — final table after new features
    after_cnt = pd.read_sql(
        text(
            f'''SELECT COUNT(*) AS cnt FROM "{TARGET_SCHEMA_1}"."{TARGET_TABLE_2}"'''
        ),
        con=engine,
    )["cnt"][0]

    sql = f"""
        UPDATE {LOG_SCHEMA}.{FEATURE_ENG_LOG}
        SET
            new_feature_completed = 'YES',
            new_feature_name = '{TARGET_SCHEMA_1}.{TARGET_TABLE_2}',
            new_feature_cnt = {after_cnt},
            timestamp = NOW()
        WHERE last_run_date = (
            SELECT last_run_date
            FROM {LOG_SCHEMA}.{FEATURE_ENG_LOG}
            WHERE new_feature_completed = 'NO'
            ORDER BY timestamp DESC
            LIMIT 1
        );
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print(f"✅ Updated new-feature metadata: after={after_cnt}, before={before_cnt}")


# ----------------------------------------------------------------------
# Load Data To Postgres (chunked)
# ----------------------------------------------------------------------
def load_chunked(df, table_name, schema):
    total_rows = len(df)
    print(
        f"\n🚀 Loading → {schema}.{table_name} ({total_rows} rows)"
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


# ----------------------------------------------------------------------
# creating new col feature
# ----------------------------------------------------------------------
def pricing_catlog():
    print("🔗 Connecting to PostgreSQL...")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    query = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE};"
    df = pd.read_sql(query, con=engine)
    print(f"✅ Loaded {len(df)} rows from {SOURCE_TABLE}")

    def clean_name(name):
        return re.sub(r"[^a-zA-Z0-9]", "", str(name)).lower()

    df["make_clean"] = df["manufacturer"].apply(clean_name)
    df["model_clean"] = df["model_policy"].apply(clean_name)

    group_cols = [
        "vehicle_age",
        "make_clean",
        "model_clean",
        "vehicle_idv",
        "cleaned_state_2",
        "start_year",
    ]
    agg_dict = {
        "total_od_premium": ["min", "mean", "max"],
        "total_tp_premium": ["min", "mean", "max"],
    }

    pricing_catalog = (
        df.groupby(group_cols, dropna=False)
        .agg(agg_dict)
        .reset_index()
    )
    print("pricing catlog done")

    pricing_catalog.columns = [
        "_".join(filter(None, col)).rstrip("_")
        for col in pricing_catalog.columns
    ]

    df_merged = pd.merge(
        df,
        pricing_catalog,
        how="left",
        on=[
            "vehicle_age",
            "make_clean",
            "model_clean",
            "vehicle_idv",
            "cleaned_state_2",
            "start_year",
        ],
    )
    print("merging the catlog using group by caluse")

    df_merged.columns = (
        df_merged.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    print("📝 Normalized column names in df_merged")

    load_chunked(df_merged, TARGET_TABLE_1, TARGET_SCHEMA)
    print(
        f"loaded pricing catlog data into {TARGET_TABLE_1} with row records of: "
        f"{len(df_merged)}"
    )


def build_policy_features():
    print("🔗 Connecting to PostgreSQL...")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    query = f"""SELECT * FROM {SOURCE_SCHEMA}.{TARGET_TABLE_1}
            ORDER BY 
                "cleaned_chassis_number", 
                "cleaned_engine_number", 
                "corrected_name", 
                "policy_start_date", 
                "policy_end_date";"""

    df = pd.read_sql(query, con=engine)
    print(f"✅ Loaded {len(df)} rows from {TARGET_TABLE_1}")

    df["policy_start_date"] = pd.to_datetime(df["policy_start_date"])
    df["policy_end_date"] = pd.to_datetime(df["policy_end_date"])

    def clean_name(name):
        return re.sub(r"[^a-zA-Z0-9]", "", str(name)).lower()

    df["rto_location_clean"] = df["rto_location"].apply(clean_name)
    df["fuel_type_clean"] = df["fuel_type"].apply(clean_name)
    df["product_name_clean"] = df["product_name"].apply(clean_name)
    df["vehicle_segment_clean"] = df["vehicle_segment"].apply(clean_name)

    print("cleaning completed")

    cat_cols = [
        "policy_status",
        "cleaned_state_2",
        "rto_location_clean",
        "model_clean",
        "fuel_type_clean",
        "make_clean",
        "product_name_clean",
        "vehicle_segment_clean",
    ]
    for c in cat_cols:
        df[c] = df[c].astype("category")

    group_cols = [
        "cleaned_chassis_number",
        "cleaned_engine_number",
        "corrected_name",
    ]
    df = df.sort_values(group_cols + ["policy_start_date", "policy_end_date"])

    df["renewal_flag"] = df["policy_status"].map(
        {"Renewed": 1, "Not Renewed": 0, "Open": 0}
    )
    df["is_active"] = df["policy_status"].eq("Open")
    print("renewal flag completed")

    g = df.groupby(group_cols)

    cum_sum = g["renewal_flag"].cumsum() - df["renewal_flag"]
    cum_count = g.cumcount()
    df["retention_rate_pct"] = np.where(
        cum_count > 0, cum_sum / cum_count, np.nan
    )

    cum_prem = g["total_premium_payable"].cumsum() - df[
        "total_premium_payable"
    ]
    df["avg_premium_hist"] = np.where(
        cum_count > 0, cum_prem / cum_count, np.nan
    )
    print("avg premium hist completed")

    df["prev_renew"] = g["renewal_flag"].shift().fillna(0)
    df["streak_block"] = (
        (df["prev_renew"] == 0)
        .astype(int)
        .groupby(df[group_cols].apply(tuple, axis=1))
        .cumsum()
    )
    df["retention_streak"] = df.groupby(group_cols)["prev_renew"].cumsum()
    df.drop(columns=["prev_renew", "streak_block"], inplace=True)

    df["lag_1_premium"] = g["total_premium_payable"].shift()
    df["previous_year_premium_ratio"] = (
        df["total_premium_payable"] / df["lag_1_premium"]
    )

    df["days_between_renewals"] = g["policy_start_date"].diff().dt.days
    print("days between renewal completed")

    def calc_gaps(d, keys):
        d = d.sort_values(keys + ["policy_start_date", "policy_end_date"])
        d["_gid"] = d.groupby(keys).ngroup()
        d["_is_pkg"] = d.groupby(["_gid", "policy_start_date"]).cumcount() > 0
        d["_prev_start"] = (
            d.groupby("_gid")["policy_start_date"]
            .transform(lambda x: x.shift().where(x != x.shift()).ffill())
        )

        ends = (
            d.groupby(["_gid", "policy_start_date"])["policy_end_date"]
            .min()
            .reset_index()
            .rename(
                columns={
                    "policy_start_date": "_prev_start",
                    "policy_end_date": "_prev_min_end",
                }
            )
        )

        d = d.merge(ends, on=["_gid", "_prev_start"], how="left")
        print("merge into main table completed")

        d["days_gap_prev_end_to_curr_start"] = np.where(
            d["_is_pkg"],
            0,
            np.where(
                d["_prev_min_end"].notna(),
                (d["policy_start_date"] - d["_prev_min_end"]).dt.days,
                np.nan,
            ),
        )

        d.drop(
            columns=["_gid", "_is_pkg", "_prev_start", "_prev_min_end"],
            inplace=True,
        )
        return d

    df = calc_gaps(df, group_cols)
    print("claim approval rate starting")

    claim_total = (df["approved"] + df["denied"]).replace(0, np.nan)
    df["claim_approval_rate"] = df["approved"] / claim_total
    print("claim approval rate completed")

    df["idv_premium_ratio"] = (
        df["vehicle_idv"] / df["total_premium_payable"]
    )
    df["add_on_adoption"] = (
        df["before_gst_add_on_gwp"]
        / df["total_premium_payable"].replace(0, np.nan)
    )
    df["od_tp_ratio"] = (
        df["total_od_premium"] / df["total_tp_premium"].replace(0, np.nan)
    )

    df["lag_1_ncb"] = g["ncb_amount"].shift()
    df["lag_1_od_premium"] = g["total_od_premium"].shift()
    df["lag_1_tp_premium"] = g["total_tp_premium"].shift()

    closed = df.loc[
        ~df["policy_status"].eq("Open"),
        [
            "renewal_flag",
            "cleaned_state_2",
            "rto_location_clean",
            "model_clean",
            "vehicle_segment_clean",
            "fuel_type_clean",
            "product_name_clean",
            "make_clean",
        ],
    ].copy()

    closed["churn_target"] = 1 - closed["renewal_flag"]

    risk_means = (
        closed.melt(id_vars="churn_target", var_name="risk_dim", value_name="key")
        .groupby(["risk_dim", "key"])["churn_target"]
        .mean()
        .rename("risk_score")
        .reset_index()
    )

    CHUNK_SIZE = 200000

    risk_map = {
        "cleaned_state_2": "state_risk_score",
        "rto_location_clean": "rto_risk_factor",
        "vehicle_segment_clean": "segment_risk_score",
        "model_clean": "model_risk_score",
        "fuel_type_clean": "fuel_type_risk_factor",
        "product_name_clean": "product_risk_factor",
        "make_clean": "manufacturer_risk_rate",
    }

    final_list = []

    print("🚀 Starting chunked merge for risk scores...")

    for start in range(0, len(df), CHUNK_SIZE):
        end = min(start + CHUNK_SIZE, len(df))
        chunk = df.iloc[start:end].copy()

        for dim, new_col in risk_map.items():
            mapping = (
                risk_means.query("risk_dim == @dim")[["key", "risk_score"]]
                .rename(columns={"key": dim, "risk_score": new_col})
            )
            chunk = chunk.merge(mapping, on=dim, how="left")

        final_list.append(chunk)
        print(f"✔ Risk mapping chunk merged: {start} → {end}")

    df = pd.concat(final_list, ignore_index=True)
    print(
        f"✔ Final DF after chunked risk merge: {df.shape[0]} rows, "
        f"{df.shape[1]} columns"
    )
    print("⬆️ Uploading full policy DF (with risk columns) to temp table tmp_policy_features...")
    load_chunked(df, "tmp_policy_features", TARGET_SCHEMA)
    print("✔ Temp table created: tmp_policy_features")

    df["total_revenue"] = df["total_premium_payable"]

    customer_total_revenue = df.groupby("customerid")["total_revenue"].sum()
    customer_total_purchases = df.groupby("customerid").size()
    customer_apv = customer_total_revenue / customer_total_purchases

    df_customer = df.drop_duplicates(subset="customerid", keep="first")
    df_customer["Churned_Binary"] = df_customer["overall_churned"].apply(
        lambda x: 1 if x == "Yes" else 0
    )

    unique_customers = df_customer["customerid"].nunique()
    churned_customers = df_customer[
        df_customer["Churned_Binary"] == 1
    ]["customerid"].nunique()
    churn_rate = (
        churned_customers / unique_customers if unique_customers > 0 else 0
    )
    print("churn binary sompleted")

    average_customer_lifespan = (
        1 / churn_rate if churn_rate != 0 else np.inf
    )
    customer_apf = customer_total_purchases
    customer_clv = customer_total_revenue * average_customer_lifespan
    print("customer clv starting")

    customer_metrics_df = pd.DataFrame(
        {
            "customerid": customer_total_revenue.index,
            "customer_apv": customer_apv.values,
            "customer_apf": customer_apf.values,
            "churn_rate": churn_rate,
            "average_customer_lifespan": average_customer_lifespan,
            "clv": customer_clv.values,
        }
    )
    print("clv completed")

    load_chunked(customer_metrics_df, "tmp_customer_metrics", TARGET_SCHEMA)
    print("✔ Temp table created: tmp_customer_metrics")

    print("🧹 Disposing old engine after heavy processing...")
    engine.dispose()

    print("😴 Sleeping 60 seconds to reset DB connection...")
    time.sleep(60)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    fresh_engine = hook.get_sqlalchemy_engine()
    print("🔗 Fresh DB engine created")

    print("🗑 Dropping existing target table...")
    with fresh_engine.begin() as conn:
        conn.execute(
            text(
                f"DROP TABLE IF EXISTS {TARGET_SCHEMA_1}.{TARGET_TABLE_2};"
            )
        )
    print("✔ Target table dropped")

    print("🚀 Creating final table using CTAS...")
    sql_ctas = f"""
    CREATE TABLE {TARGET_SCHEMA_1}.{TARGET_TABLE_2} AS
    SELECT 
        a.*, 
        b."customer_apv",
        b."customer_apf",
        b."churn_rate",
        b."average_customer_lifespan",
        b."clv"
    FROM {TARGET_SCHEMA}.tmp_policy_features a
    LEFT JOIN {TARGET_SCHEMA}.tmp_customer_metrics b
        ON a.customerid = b.customerid;
    """

    print("🚀 Running CTAS with fresh DB engine...")
    with fresh_engine.begin() as conn:
        conn.execute(text(sql_ctas))
    print("🎉 CTAS completed → Final table created successfully")

    print("🗑 Dropping temp table...")
    with fresh_engine.begin() as conn:
        conn.execute(
            text(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.tmp_customer_metrics;")
        )
        conn.execute(
            text(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.tmp_policy_features;")
        )
    print("✔ Temp table dropped")

    update_new_feature_metadata(fresh_engine)

    print("📊 Counting final rows...")
    with fresh_engine.begin() as conn:
        final_cnt = conn.execute(
            text(
                f"SELECT COUNT(*) FROM {TARGET_SCHEMA_1}.{TARGET_TABLE_2};"
            )
        ).scalar()

    print(f"✔ Final table row count: {final_cnt}")


# default_args = {
#     "owner": "airflow",
#     "start_date": datetime(2024, 11, 1),
#     "retries": 0,
#     "retry_delay": timedelta(minutes=2),
# }

# with DAG(
#     dag_id="creating_new_features",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["feature", "engineering"],
# ) as dag:

#     pricing_catlog_task = PythonOperator(
#         task_id="pricing_catlog",
#         python_callable=pricing_catlog,
#         provide_context=True,
#     )

#     policy_feature_task = PythonOperator(
#         task_id="build_policy_features",
#         python_callable=build_policy_features,
#         provide_context=True,
#     )

#     pricing_catlog_task >> policy_feature_task
