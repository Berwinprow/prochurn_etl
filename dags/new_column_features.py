import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import re
from sqlalchemy.exc import PendingRollbackError, OperationalError
import time, gc
from pathlib import Path
from schema_table_config import get_column_mapping, get_log_tables, get_schema

DAGS_DIR = Path(__file__).resolve().parent
JSON_PATH = str(DAGS_DIR / "config" / "schema_metadata_config.json")

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
TARGET_SCHEMA = get_schema("bi_dwh", JSON_PATH)
TARGET_TABLE = "final_policy_features"
TABLE = "baseprclaim"
SOURCE_SCHEMA = get_schema("agg", JSON_PATH)
LOG_SCHEMA = get_schema("log", JSON_PATH)
FEATURE_LOG = get_log_tables("featurelog", JSON_PATH)


def get_latest_addons_table(engine):
    query = f"""
        SELECT addons
        FROM {LOG_SCHEMA}.{FEATURE_LOG}
        WHERE addons IS NOT NULL
        ORDER BY date DESC
        LIMIT 1
    """
    result = engine.execute(text(query)).fetchone()
    if result:
        return result[0]   # ✅ return value
    else:
        print(f"no table found in {FEATURE_LOG}")
        return None

def update_renewal_rate_status():    
    hook = PostgresHook(postgres_conn_id="postgres_cloud_prochurn")
    conn = hook.get_conn()
    cur = conn.cursor()

    print("\n🚀 Running renewal rate status update...")

    sql = f"""

    ALTER TABLE {SOURCE_SCHEMA}.{TABLE}
    ADD COLUMN IF NOT EXISTS renewal_rate_status TEXT;

    DROP TABLE IF EXISTS temp_renewal_rate;
    CREATE TEMP TABLE temp_renewal_rate AS
    SELECT
        cleaned_chassis_no,
        cleaned_engine_no,
        corrected_name,
        policy_start_date,
        CASE 
            WHEN LAG(policy_end_date) OVER (
                PARTITION BY cleaned_chassis_no, cleaned_engine_no, corrected_name
                ORDER BY policy_start_date
            ) IS NULL THEN 'Null'

            WHEN policy_start_date <
                 LAG(policy_end_date) OVER (
                     PARTITION BY cleaned_chassis_no, cleaned_engine_no, corrected_name
                     ORDER BY policy_start_date
                 ) + INTERVAL '1 day' THEN 'Null'

            ELSE CASE
                WHEN ROUND(total_premium_payable::numeric,0) >
                     LAG(ROUND(total_premium_payable::numeric,0)) OVER (
                         PARTITION BY cleaned_chassis_no, cleaned_engine_no, corrected_name
                         ORDER BY policy_start_date
                     )
                THEN 'Increase'

                WHEN ROUND(total_premium_payable::numeric,0) <
                     LAG(ROUND(total_premium_payable::numeric,0)) OVER (
                         PARTITION BY cleaned_chassis_no, cleaned_engine_no, corrected_name
                         ORDER BY policy_start_date
                     )
                THEN 'Decrease'

                ELSE 'No Change'
            END
        END AS renewal_status
    FROM {SOURCE_SCHEMA}.{TABLE};

    UPDATE {SOURCE_SCHEMA}.{TABLE} t
    SET renewal_rate_status = tmp.renewal_status
    FROM temp_renewal_rate tmp
    WHERE t.cleaned_chassis_no = tmp.cleaned_chassis_no
      AND t.cleaned_engine_no = tmp.cleaned_engine_no
      AND t.corrected_name = tmp.corrected_name
      AND t.policy_start_date = tmp.policy_start_date;
    """

    cur.execute(sql)
    conn.commit()
    cur.close()

    print("✅ Completed: renewal rate status update\n")

def update_feature_log_newcol(engine, target_table, count_val):
    
    query = f"""
        INSERT INTO {LOG_SCHEMA}.{FEATURE_LOG}(date, new_col, count_of_new_col)
        VALUES (:dt, :tbl, :cnt)
        ON CONFLICT (date)
        DO UPDATE SET new_col = :tbl, count_of_new_col = :cnt
    """
    with engine.begin() as conn:
        conn.execute(text(query), {
            "dt": datetime.now().date(),
            "tbl": target_table,
            "cnt": count_val
        })


def build_policy_features():

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))
    SOURCE_TABLE = get_latest_addons_table(engine)
    # SOURCE_TABLE = "basepr_merged_with_claim"
    if not SOURCE_TABLE:
        print(f"no source table found in {SOURCE_TABLE}")
        return
    print(f"found table to process {SOURCE_TABLE}")

    query = f"""
    SELECT *
    FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
    ORDER BY 
    cleaned_chassis_no, 
    cleaned_engine_no, 
    corrected_name, 
    policy_start_date, 
    policy_end_date;
    """
    df = pd.read_sql(text(query), con=engine)
    print(f"datas loaded to query")

    # Dates, dtypes & ordering
    df['policy_start_date'] = pd.to_datetime(df['policy_start_date'])
    df['policy_end_date']   = pd.to_datetime(df['policy_end_date'])

    # Function to clean names
    def clean_name(name):
        return re.sub(r'[^a-zA-Z0-9]', '', str(name)).lower()
    
    # ========== START: New Columns (Inserted Mid-Script) ==========

    # 1. claim_happened_flag (based on "policy_no")
    if 'policy_no' in df.columns:
        df['claim_happened_flag'] = df['claim_no'].apply(lambda x: 'no' if pd.isna(x) else 'yes')

    # 2. policy_status (based on renewed_flag)
    if 'renewed_flag' in df.columns:
        df['policy_status'] = df['renewed_flag'].apply(lambda x: 'Open' if x == 2 else ('Not Renewed' if x == 0  else 'Renewed'))
        print(df['policy_status'].value_counts(dropna=False))

    # --- 1. churn_label (per customer_id & end_year) ---

    if {'customer_id', 'end_year', 'policy_status'}.issubset(df.columns):

        def calculate_churn_status(x):
            # If all policies in that year are 'Not Renewed' → yes (churned)
            # Else (at least one 'Renewed') → no (not churned)
            unique_statuses = x.unique()
            if len(unique_statuses) == 1 and unique_statuses[0] == 'Not Renewed':
                return 'yes'
            else:
                return 'no'

        df['churn_label'] = df.groupby(['customer_id', 'end_year'])['policy_status'] \
                            .transform(lambda x: calculate_churn_status(x))


    # --- 2. overall_churned (based on latest end_year per customer) ---

    if {'customer_id', 'end_year', 'churn_label'}.issubset(df.columns):
        # Find latest year per customer
        latest_year_map = df.groupby('customer_id')['end_year'].transform('max')

        # Filter latest year’s records
        latest_df = df[df['end_year'] == latest_year_map]

        # Pick churn_label for that latest year (equivalent to row_number = 1 in SQL)
        churn_map = latest_df.groupby('customer_id')['churn_label'].first()

        # Map back to main df
        df['overall_churned'] = df['customer_id'].map(churn_map)

   
    # ========== END: New Columns (Inserted Mid-Script) ==========
        print("new column adding done")

    # Clean make/model columns
    df['rto_location_clean'] = df['rto_location'].apply(clean_name)
    df['fuel_type_clean'] = df['fuel_type'].apply(clean_name)
    df['product_name_clean'] = df['product_name'].apply(clean_name)
    df['vehicle_segment_clean'] = df['vehicle_segment'].apply(clean_name)
    df['make_clean'] = df['manufacturer'].apply(clean_name)
    df['model_clean'] = df['cleaned_model'].apply(clean_name)

    # Convert high-cardinality object columns to category
    cat_cols = [
        'policy_status', 'state', 'rto_location_clean',
        'model_clean', 'fuel_type_clean', 'make_clean', 'product_name_clean', 'vehicle_segment_clean'
    ]
    for c in cat_cols:
        df[c] = df[c].astype('category')

    group_cols = ['cleaned_chassis_no', 'cleaned_engine_no', 'corrected_name']
    df = df.sort_values(group_cols + ['policy_start_date', 'policy_end_date'])

    # Renewal flag & active indicator
    df['renewal_flag_binary'] = df['policy_status'].map({'Renewed': 1, 'Not Renewed': 0,'Open': 0})
    df['is_active']    = df['policy_status'].eq('Open')

    g = df.groupby(group_cols)

    # Fast cumulative features
    # Historical retention rate
    cum_sum   = g['renewal_flag_binary'].cumsum() - df['renewal_flag_binary']
    cum_count = g.cumcount()
    df['retention_rate_pct'] = np.where(cum_count > 0, cum_sum / cum_count, np.nan)

    # Historical average premium
    cum_prem  = g['total_premium_payable'].cumsum() - df['total_premium_payable']
    df['avg_premium_hist'] = np.where(cum_count > 0, cum_prem / cum_count, np.nan)

    print("renewal flag done")

    # Retention streak (vectorised)
    df['prev_renew'] = g['renewal_flag_binary'].shift().fillna(0)
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
    
    print("days gap prev end to curr start done")
    # Claim approval rate – safe divide
    claim_total = (df['Approved'] + df['Denied']).replace(0, np.nan)
    df['claim_approval_rate'] = df['Approved'] / claim_total

    df['vehicle_idv'] = pd.to_numeric(df['vehicle_idv'], errors='coerce')
    df['total_premium_payable'] = pd.to_numeric(df['total_premium_payable'], errors='coerce')

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
        'renewal_flag_binary', 'state', 'rto_location_clean', 'model_clean', 'vehicle_segment_clean', 'fuel_type_clean', 'product_name_clean',
        'make_clean'
    ]].copy()

    closed['churn_target'] = 1 - closed['renewal_flag_binary']
    print("churn target done")
    risk_means = (
        closed.melt(id_vars='churn_target', var_name='risk_dim', value_name='key')
            .groupby(['risk_dim', 'key'])['churn_target'].mean()
            .rename('risk_score')
            .reset_index()
    )

    # Map each risk dimension back
    risk_map = {
        'state': 'state_risk_score',
        'rto_location_clean': 'rto_risk_factor',
        'vehicle_segment_clean': 'segment_risk_score',
        'model_clean': 'model_risk_score',
        'fuel_type_clean': 'fuel_type_risk_factor',
        'product_name_clean': 'product_risk_factor',
        'make_clean': 'manufacturer_risk_rate'
    }

    for dim, new_col in risk_map.items():
        df = df.merge(
            risk_means.query("risk_dim == @dim")[['key', 'risk_score']]
                    .rename(columns={'key': dim, 'risk_score': new_col}),
            on=dim, how='left'
        )

    print(f" Feature engineering complete – rows: {len(df):,}, cols: {df.shape[1]}")

    # Calculate total revenue (using 'total_premium_payable') per policy
    df['total_revenue'] = df['total_premium_payable']

    # Calculate total revenue per customer 
    customer_total_revenue = df.groupby('customer_id')['total_revenue'].sum()

    # Calculate total number of purchases (policies) per customer
    customer_total_purchases = df.groupby('customer_id').size()

    # Calculate Average Purchase Value (APV) per customer
    customer_apv = customer_total_revenue / customer_total_purchases

    # Convert 'overall_churned' to a binary flag for churn rate calculation
    # (Creating a customer-level snapshot by dropping duplicate customer_ids)
    df_customer = df.drop_duplicates(subset='customer_id', keep='first')
    df_customer['Churned_Binary'] = df_customer['overall_churned'].apply(lambda x: 1 if x == 'yes' else 0)

    # Calculate global churn rate
    unique_customers = df_customer['customer_id'].nunique()
    churned_customers = df_customer[df_customer['Churned_Binary'] == 1]['customer_id'].nunique()
    churn_rate = churned_customers / unique_customers if unique_customers > 0 else 0

    # Calculate the average customer lifespan (ACL) as the inverse of churn rate
    average_customer_lifespan = 1 / churn_rate if churn_rate != 0 else np.inf

    # Define Average Purchase Frequency (APF) per customer as the number of purchases
    customer_apf = customer_total_purchases

    # Calculate Customer Lifetime Value (CLV) for each customer
    # This simplifies to: total revenue per customer * average_customer_lifespan
    customer_clv = customer_total_revenue * average_customer_lifespan

    # Create a customer-level metrics DataFrame to merge back with df
    customer_metrics_df = pd.DataFrame({
        'customer_id': customer_total_revenue.index,
        'Customer_APV': customer_apv.values,
        'Customer_APF': customer_apf.values,  
        'Churn_Rate': churn_rate,             
        'Average_Customer_Lifespan': average_customer_lifespan,  
        'CLV': customer_clv.values
    })
    print("clv cal done")
    # Merge the customer metrics back into the original DataFrame based on customer_id
    df = df.merge(customer_metrics_df, on='customer_id', how='left')

    # Verify the merge by displaying the first few rows
    print(df.head())

    print("policy wise purchase column")

    df = df.sort_values(by=["policy_start_date","initial_policy_no"],ascending=[True,True])

    df["policy_wise_purchase"]= df.groupby("initial_policy_no").cumcount()+1

    print(f"df[['initial_policy_no', 'policy_wise_purchase']].head()")

    print("pricing catelog column adding script")

    df.columns = [col.lower().replace(" ","_") for col in df.columns]

    pricing_grp_col = ["vehicle_age","make","cleaned_model","vehicle_idv","state","start_year"]

    pricing_agg ={
        "total_od_premium": ["min","mean","max"],
        "total_tp_premium": ["min","mean","max"]
    }
    pricing_catalog = (df.groupby(pricing_grp_col,dropna=False).agg(pricing_agg).reset_index())

    pricing_catalog.columns=["_".join(col).rstrip("_") if isinstance(col, tuple) else col for col in pricing_catalog.columns]

    df =pd.merge(df,pricing_catalog,how="left",on= pricing_grp_col)
   
    print(f"✅ Pricing catalog columns added: {[c for c in pricing_catalog.columns if c not in pricing_grp_col]}")

    try:
        print("🧹 Disposing old engine before final write...")
        engine.dispose()
        time.sleep(2)

        # Recreate a clean connection
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        fresh_engine = hook.get_sqlalchemy_engine()

        print("🚀 Writing final data to Postgres...")
        with fresh_engine.begin() as conn:
            step = 10000
            for i in range(0, len(df), step):
                sub = df.iloc[i:i+step]
                sub.to_sql(
                    name=TARGET_TABLE,
                    schema=TARGET_SCHEMA,
                    index=False,
                    con=conn,
                    if_exists="append" if i>0 else "replace",
                    method=None,
                    chunksize = 10000
                )
                print(f"✅ wrote rows {i}–{i+step}")


        row_count = len(df)
        update_feature_log_newcol(fresh_engine, TARGET_TABLE, row_count)
        # update_renewal_rate_status(TARGET_SCHEMA, TARGET_TABLE)
        print(f"✅ Feature log updated: new_col = {TARGET_TABLE}, count = {row_count}")

    except (PendingRollbackError, OperationalError) as e:
        print(f"⚠️ Transaction rollback/timeout detected: {e}")
        fresh_engine.dispose()
        time.sleep(5)
        retry_engine = hook.get_sqlalchemy_engine()
        with retry_engine.begin() as conn:
            step = 10000
            for i in range(0, len(df), step):
                sub = df.iloc[i:i+step]
                sub.to_sql(
                    name=TARGET_TABLE,
                    schema=TARGET_SCHEMA,
                    index=False,
                    con=conn,
                    if_exists="append" if i>0 else "replace",
                    method=None,
                    chunksize = 10000
                )
                print(f"✅ wrote rows {i}–{i+step}")
        print("✅ Retry successful after rollback.")
    finally:
        fresh_engine.dispose()
        gc.collect()
        print("🧹 Engine disposed and memory cleaned up.")




# with DAG(
#     dag_id="creating_new_column_on_baseprclaim",
#     default_args={"owner": "airflow", "start_date": datetime(2024, 1, 1)},
#     schedule_interval=None,
#     catchup=False,
#     tags=["policy", "new", "column"]
# ) as dag:

#     new_column_on_finaltable = PythonOperator(
#         task_id="added_column_on_finaltable",
#         python_callable=build_policy_features
#     )

#     build_policy_features

#         task_id="added_column_on_finaltable",
#         python_callable=build_policy_features
#     )

#     build_policy_features
