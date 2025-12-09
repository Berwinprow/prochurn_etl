from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
import numpy as np
import time
import re
from schema_table_config import get_log_tables, get_schema, get_column_mapping


DAG_DIR = Path(__file__).resolve().parent
POSTGRES_CONN_ID = "postgres_cloud_prochurn"
SOURCE_TABLE = "overall_cleaned_base_and_pr_ef_policyef"
SOURCE_SCHEMA = "test_aggregation"

TARGET_SCHEMA = "test_aggregation"
TARGET_TABLE_1 = "policyef_with_pricing_catlog"
TARGET_TABLE_2 = "policydata_with_fb_cc_pc_newfea_opti_correct"

OUTER_CHUNK = 10000
INNER_CHUNK = 5000

COLUMN_JSON = str(DAG_DIR / "config" / "column_mapping.json")
ZONE_TABLE = get_column_mapping("zonemapping", COLUMN_JSON)

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
# ----------------------------------------------------------------------
# creating new col feature
# ----------------------------------------------------------------------

def pricing_catlog():

    print("🔗 Connecting to PostgreSQL...")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # ---------------------------
    # Step 1: Load Data
    # ---------------------------
    query = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE};"
    df = pd.read_sql(query, con=engine)
    print(f"✅ Loaded {len(df)} rows from {SOURCE_TABLE}")

    # Clean make/model columns
    def clean_name(name):
        return re.sub(r'[^a-zA-Z0-9]', '', str(name)).lower()

    df['make_clean'] = df['manufacturer'].apply(clean_name)
    df['model_clean'] = df['model_policy'].apply(clean_name)

    # Create pricing catalog
    group_cols = ["vehicle_age", "make_clean", "model_clean", "vehicle_idv", "cleaned_state_2", "start_year"]
    agg_dict = {
        "total_od_premium": ["min", "mean", "max"],
        "total_tp_premium": ["min", "mean", "max"]
    }

    pricing_catalog = (
        df
        .groupby(group_cols, dropna=False)
        .agg(agg_dict)
        .reset_index()
    )
    print(f"pricing catlog done")
    # Flatten column names
    pricing_catalog.columns = [
        "_".join(filter(None, col)).rstrip("_") for col in pricing_catalog.columns
    ]

    # Merge pricing catalog into raw df
    df_merged = pd.merge(
        df,
        pricing_catalog,
        how="left",
        on=["vehicle_age", "make_clean", "model_clean", "vehicle_idv", "cleaned_state_2", "start_year"]
    )
    print(f"merging the catlog using group by caluse")

    # 🔽 Normalize column names before writing to DB
    df_merged.columns = (
        df_merged.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    print("📝 Normalized column names in df_merged")

    load_chunked(df_merged,TARGET_TABLE_1,TARGET_SCHEMA)
    print(f"loaded pricing catlog data into {TARGET_TABLE_1} with row records of: {len(df_merged)}")

def build_policy_features():
    print("🔗 Connecting to PostgreSQL...")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # ---------------------------
    # Step 1: Load Data
    # ---------------------------
    query = f"""SELECT * FROM {SOURCE_SCHEMA}.{TARGET_TABLE_1}
            ORDER BY 
                "cleaned_chassis_number", 
                "cleaned_engine_number", 
                "corrected_name", 
                "policy_start_date", 
                "policy_end_date";"""

    df = pd.read_sql(query, con=engine)
    print(f"✅ Loaded {len(df)} rows from {TARGET_TABLE_1}")

    # Dates, dtypes & ordering
    df['policy_start_date'] = pd.to_datetime(df['policy_start_date'])
    df['policy_end_date']   = pd.to_datetime(df['policy_end_date'])

    # Function to clean names
    def clean_name(name):
        return re.sub(r'[^a-zA-Z0-9]', '', str(name)).lower()

    # Clean make/model columns
    df['rto_location_clean'] = df['rto_location'].apply(clean_name)
    df['fuel_type_clean'] = df['fuel_type'].apply(clean_name)
    df['product_name_clean'] = df['product_name'].apply(clean_name)
    df['vehicle_segment_clean'] = df['vehicle_segment'].apply(clean_name)

    print("cleaning completed")
    # Convert high-cardinality object columns to category
    cat_cols = [
        'policy_status', 'cleaned_state_2', 'rto_location_clean',
        'model_clean', 'fuel_type_clean', 'make_clean', 'product_name_clean', 'vehicle_segment_clean'
    ]
    for c in cat_cols:
        df[c] = df[c].astype('category')

    group_cols = ['cleaned_chassis_number', 'cleaned_engine_number', 'corrected_name']
    df = df.sort_values(group_cols + ['policy_start_date', 'policy_end_date'])

    # Renewal flag & active indicator
    df['renewal_flag'] = df['policy_status'].map({'Renewed': 1, 'Not Renewed': 0,'Open': 0})
    df['is_active']    = df['policy_status'].eq('Open')
    print("renewal flag completed")

    g = df.groupby(group_cols)

    # Fast cumulative features
    # Historical retention rate
    cum_sum   = g['renewal_flag'].cumsum() - df['renewal_flag']
    cum_count = g.cumcount()
    df['retention_rate_pct'] = np.where(cum_count > 0, cum_sum / cum_count, np.nan)

    # Historical average premium
    cum_prem  = g['total_premium_payable'].cumsum() - df['total_premium_payable']
    df['avg_premium_hist'] = np.where(cum_count > 0, cum_prem / cum_count, np.nan)
    print("avg premium hist completed")
    # Retention streak (vectorised)
    df['prev_renew'] = g['renewal_flag'].shift().fillna(0)
    df['streak_block'] = (
        (df['prev_renew'] == 0)
        .astype(int)
        .groupby(df[group_cols].apply(tuple, axis=1))
        .cumsum()
    )
    df['retention_streak'] = (
        df.groupby(group_cols + ['streak_block'])['prev_renew'].cumsum()
    )
    df.drop(columns=['prev_renew', 'streak_block'], inplace=True)

    # Lagged premium & YoY ratio
    df['lag_1_premium'] = g['total_premium_payable'].shift()
    df['previous_year_premium_ratio'] = df['total_premium_payable'] / df['lag_1_premium']

    # Days between renewal start dates (regardless of package/overlap)
    df['days_between_renewals'] = g['policy_start_date'].diff().dt.days
    print("days between renewal completed")
    # Correct Package/Overlap-Aware Gap Logic
    def calc_gaps(d, keys):
        d = d.sort_values(keys + ['policy_start_date', 'policy_end_date'])

        # Unique group ID
        d['_gid'] = d.groupby(keys).ngroup()

        # Flag package policies (same start date, second+ occurrence)
        d['_is_pkg'] = d.groupby(['_gid', 'policy_start_date']).cumcount() > 0

        # Previous unique start (skip same start rows)
        d['_prev_start'] = (
            d.groupby('_gid')['policy_start_date']
            .transform(lambda x: x.shift().where(x != x.shift()).ffill())
        )

        # Map: (gid, start) → earliest end for that start
        ends = (
            d.groupby(['_gid', 'policy_start_date'])['policy_end_date']
            .min()
            .reset_index()
            .rename(columns={
                'policy_start_date': '_prev_start',
                'policy_end_date'  : '_prev_min_end'
            })
        )

        # Merge previous end into main table
        d = d.merge(ends, on=['_gid', '_prev_start'], how='left')
        print("merge into main table completed")
        # Compute final gap
        d['days_gap_prev_end_to_curr_start'] = np.where(
            d['_is_pkg'],
            0,
            np.where(
                d['_prev_min_end'].notna(),
                (d['policy_start_date'] - d['_prev_min_end']).dt.days,
                np.nan
            )
        )

        # Clean-up
        d.drop(columns=['_gid', '_is_pkg', '_prev_start', '_prev_min_end'], inplace=True)
        return d

    # APPLY THE GAP FUNCTION HERE
    df = calc_gaps(df, group_cols)
    print("claim approval rate starting")
    # Claim approval rate – safe divide
    claim_total = (df['approved'] + df['denied']).replace(0, np.nan)
    df['claim_approval_rate'] = df['approved'] / claim_total
    print("claim approval rate completed")
    # Simple ratios
    df['idv_premium_ratio']     = df['vehicle_idv'] / df['total_premium_payable']
    df['add_on_adoption']       = df['before_gst_add_on_gwp'] / df['total_premium_payable'].replace(0, np.nan)
    df['od_tp_ratio']           = df['total_od_premium'] / df['total_tp_premium'].replace(0, np.nan)

    # Lagged technical values
    df['lag_1_ncb']        = g['ncb_amount'].shift()
    df['lag_1_od_premium'] = g['total_od_premium'].shift()
    df['lag_1_tp_premium'] = g['total_tp_premium'].shift()

    # Risk scores (vectorised)
    closed = df.loc[~df['policy_status'].eq('Open'), [
        'renewal_flag', 'cleaned_state_2', 'rto_location_clean', 'model_clean', 'vehicle_segment_clean', 'fuel_type_clean', 'product_name_clean',
        'make_clean'
    ]].copy()

    closed['churn_target'] = 1 - closed['renewal_flag']

    risk_means = (
        closed.melt(id_vars='churn_target', var_name='risk_dim', value_name='key')
            .groupby(['risk_dim', 'key'])['churn_target'].mean()
            .rename('risk_score')
            .reset_index()
    )

    # ---------------------------
    # SAFE CHUNKED RISK MERGE
    # ---------------------------

    CHUNK_SIZE = 200000  

    risk_map = {
        'cleaned_state_2': 'state_risk_score',
        'rto_location_clean': 'rto_risk_factor',
        'vehicle_segment_clean': 'segment_risk_score',
        'model_clean': 'model_risk_score',
        'fuel_type_clean': 'fuel_type_risk_factor',
        'product_name_clean': 'product_risk_factor',
        'make_clean': 'manufacturer_risk_rate'
    }

    final_list = []

    print("🚀 Starting chunked merge for risk scores...")

    for start in range(0, len(df), CHUNK_SIZE):
        end = min(start + CHUNK_SIZE, len(df))
        chunk = df.iloc[start:end].copy()

        for dim, new_col in risk_map.items():
            mapping = (
                risk_means.query("risk_dim == @dim")[['key', 'risk_score']]
                .rename(columns={'key': dim, 'risk_score': new_col})
            )
            chunk = chunk.merge(mapping, on=dim, how="left")

        final_list.append(chunk)
        print(f"✔ Risk mapping chunk merged: {start} → {end}")

    # 🔥 FIX – COMBINE BACK INTO df
    df = pd.concat(final_list, ignore_index=True)
    print(f"✔ Final DF after chunked risk merge: {df.shape[0]} rows, {df.shape[1]} columns")
    print("⬆️ Uploading full policy DF (with risk columns) to temp table tmp_policy_features...")
    load_chunked(df, "tmp_policy_features", TARGET_SCHEMA)
    print("✔ Temp table created: tmp_policy_features")

    # Calculate total revenue (using 'total_premium_payable') per policy
    df['total_revenue'] = df['total_premium_payable']
    
    # Calculate total revenue per customer 
    customer_total_revenue = df.groupby('customerid')['total_revenue'].sum()

    # Calculate total number of purchases (policies) per customer
    customer_total_purchases = df.groupby('customerid').size()

    # Calculate Average Purchase Value (APV) per customer
    customer_apv = customer_total_revenue / customer_total_purchases

    # Convert 'Overall Churned' to a binary flag for churn rate calculation
    # (Creating a customer-level snapshot by dropping duplicate customerids)
    df_customer = df.drop_duplicates(subset='customerid', keep='first')
    df_customer['Churned_Binary'] = df_customer['overall_churned'].apply(lambda x: 1 if x == 'Yes' else 0)
    
    # Calculate global churn rate
    unique_customers = df_customer['customerid'].nunique()
    churned_customers = df_customer[df_customer['Churned_Binary'] == 1]['customerid'].nunique()
    churn_rate = churned_customers / unique_customers if unique_customers > 0 else 0
    print("churn binary sompleted")
    # Calculate the average customer lifespan (ACL) as the inverse of churn rate
    average_customer_lifespan = 1 / churn_rate if churn_rate != 0 else np.inf

    # Define Average Purchase Frequency (APF) per customer as the number of purchases
    customer_apf = customer_total_purchases

    # Calculate Customer Lifetime Value (CLV) for each customer
    # This simplifies to: total revenue per customer * average_customer_lifespan
    customer_clv = customer_total_revenue * average_customer_lifespan
    print("customer clv starting")
    # Create a customer-level metrics DataFrame to merge back with df
    customer_metrics_df = pd.DataFrame({
        'customerid': customer_total_revenue.index,
        'customer_apv': customer_apv.values,
        'customer_apf': customer_apf.values,  
        'churn_rate': churn_rate,             
        'average_customer_lifespan': average_customer_lifespan,  
        'clv': customer_clv.values
    })
    print("clv completed")

    # --------------------------------------------------------
    # 2️⃣ Create temp table for customer metrics
    # --------------------------------------------------------
    print("⬆️ Uploading customer metrics to temp table...")

    load_chunked(customer_metrics_df,"tmp_customer_metrics",TARGET_SCHEMA)

    print("✔ Temp table created: tmp_customer_metrics")

    # ---------------------------------------------
    # Dispose engine & sleep before SQL operations
    # ---------------------------------------------
    print("🧹 Disposing old engine after heavy processing...")
    engine.dispose()

    print("😴 Sleeping 60 seconds to reset DB connection...")
    time.sleep(60)

    # ---------------------------------------------
    # Create fresh engine
    # ---------------------------------------------
    hook = PostgresHook(postgres_conn_id='postgres_cloud_prochurn')
    fresh_engine = hook.get_sqlalchemy_engine()
    print("🔗 Fresh DB engine created")


    # ---------------------------------------------
    # DROP TARGET TABLE
    # ---------------------------------------------
    print("🗑 Dropping existing target table...")
    with fresh_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.{TARGET_TABLE_2};"))
    print("✔ Target table dropped")

    # --------------------------------------------------------
    # 4️⃣ CREATE TABLE AS SELECT (FASTEST JOIN METHOD)
    # --------------------------------------------------------
    print("🚀 Creating final table using CTAS...")

    sql_ctas = f"""
    CREATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE_2} AS
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


    print("🎉 CTAS completed → Final table created successfully")

    # ---------------------------------------------
    # DROP TEMP TABLE
    # ---------------------------------------------
    print("🗑 Dropping temp table...")
    with fresh_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.tmp_customer_metrics;"))
        conn.execute(text(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.tmp_policy_features;"))
    print("✔ Temp table dropped")


    # ---------------------------------------------
    # FINAL ROW COUNT
    # ---------------------------------------------
    print("📊 Counting final rows...")
    with fresh_engine.begin() as conn:
        final_cnt = conn.execute(
            text(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE_2};")
        ).scalar()

    print(f"✔ Final table row count: {final_cnt}")
        

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 11, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="creating_new_features",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["feature", "engineering"],
) as dag:

    pricing_catlog_task = PythonOperator(
        task_id="pricing_catlog",
        python_callable=pricing_catlog,
        provide_context=True,
    )

    policy_feature_task = PythonOperator(
        task_id="build_policy_features",
        python_callable=build_policy_features,
        provide_context=True,
    )

    pricing_catlog_task >> policy_feature_task