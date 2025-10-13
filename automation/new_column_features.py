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

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
TARGET_SCHEMA = "pip_bi_dwh"
TARGET_TABLE = "policydata_with_fb_cc_pc_newfea_opti_correct"
SOURCE_SCHEMA = "pip_bi_dwh"
LOG_SCHEMA = "pip_log" 
FEATURE_LOG = "feature_eng_log"


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
    SOURCE_TABLE = get_latest_addons_table(engine)
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

    df['policy_start_date'] = pd.to_datetime(df['policy_start_date'])
    df['policy_end_date'] = pd.to_datetime(df['policy_end_date'])

    def clean_name(name):
        return re.sub(r'[^a-zA-Z0-9]', '', str(name)).lower()
    # ========== START: New Columns (Inserted Mid-Script) ==========
    # 1. claim_happened_flag (based on "policy_no")
    if 'policy_no' in df.columns:
        df['claim_happened_flag'] = df['policy_no'].apply(lambda x: 'no' if pd.isna(x) else 'yes')

    # 2. policy_status (based on renewed_flag)
    if 'renewed_flag' in df.columns:
        df['policy_status'] = df['renewed_flag'].apply(lambda x: 'Open' if x == 2 else ('Not Renewed' if x == 0  else 'Renewed'))
        print(df['policy_status'].value_counts(dropna=False))

    # 3. overall_churned (new logic: based on latest end_year per customer)
    if {'customer_id', 'end_year', 'policy_status'}.issubset(df.columns):
        latest_year_map = df.groupby('customer_id')['end_year'].transform('max')
        final_churn_status = (
            df[df['end_year'] == latest_year_map]
            .groupby('customer_id')['policy_status']
            .transform(lambda x: 'no' if 'renewed' in x.values else 'yes')
        )
        df['overall_churned'] = final_churn_status


    # 4. churn_label (per customer_id & year)
    if {'customer_id', 'policy_end_date', 'policy_status'}.issubset(df.columns):
        df['end_year'] = df['policy_end_date'].dt.year
        def calculate_churn_status(group):
            unique_statuses = group.unique()
            if len(unique_statuses) == 1 and unique_statuses[0] == 'not_renewed':
                return 'yes'
            else:
                return 'no'
        df['churn_label'] = df.groupby(['customer_id', 'end_year'])['policy_status'].transform(lambda x: calculate_churn_status(x))

    # 5. renewal_rate_status (lag + premium compare + gap)
    if {'cleaned_chassis_no', 'cleaned_engine_no', 'corrected_name', 'policy_start_date', 'policy_end_date', 'total_premium_payable'}.issubset(df.columns):
        df = df.sort_values(by=['cleaned_chassis_no', 'cleaned_engine_no', 'corrected_name', 'policy_start_date'])
        gtmp = df.groupby(['cleaned_chassis_no', 'cleaned_engine_no', 'corrected_name'])
        prev_end = gtmp['policy_end_date'].shift()
        prev_premium = gtmp['total_premium_payable'].shift()
        curr_premium = df['total_premium_payable']
        valid_gap = df['policy_start_date'] >= (prev_end + pd.Timedelta(days=1))

        df['renewal_rate_status'] = 'null'
        df.loc[valid_gap & (curr_premium > prev_premium), 'renewal_rate_status'] = 'increase'
        df.loc[valid_gap & (curr_premium < prev_premium), 'renewal_rate_status'] = 'decrease'
        df.loc[valid_gap & (curr_premium == prev_premium), 'renewal_rate_status'] = 'no_change'

    # ========== END: New Columns (Inserted Mid-Script) ==========

    df['rto_location_clean'] = df['rto_location'].apply(clean_name)
    df['fuel_type_clean'] = df['fuel_type'].apply(clean_name)
    df['product_name_clean'] = df['product_name'].apply(clean_name)
    df['vehicle_segment_clean'] = df['vehicle_segment'].apply(clean_name)
    df['new_vertical_clean'] = df['new_vertical'].apply(clean_name)
    df['make_clean'] = df['make'].apply(clean_name)
    df['model_clean'] = df['cleaned_model'].apply(clean_name)

    cat_cols = [
        'policy_status', 'state', 'rto_location_clean',
        'model_clean', 'fuel_type_clean', 'make_clean', 'product_name_clean',
        'vehicle_segment_clean', 'new_vertical_clean'
    ]
    for c in cat_cols:
        df[c] = df[c].astype('category')

    group_cols = ['cleaned_chassis_no', 'cleaned_engine_no', 'corrected_name']
    df = df.sort_values(group_cols + ['policy_start_date', 'policy_end_date'])

    df['renewal_flag_binary'] = df['policy_status'].map({'Renewed': 1, 'Not Renewed': 0}).fillna(0).astype(int)
    # df['renewal_flag_binary'] = df['renewal_flag_binary'].astype(int)
    df['is_active'] = df['policy_status'].eq('Open')



    g = df.groupby(group_cols)
    cum_sum = g['renewal_flag_binary'].cumsum() - df['renewal_flag_binary']
    cum_count = g.cumcount()
    df['retention_rate_pct'] = np.where(cum_count > 0, cum_sum / cum_count, np.nan)

    cum_prem = g['total_premium_payable'].cumsum() - df['total_premium_payable']
    df['avg_premium_hist'] = np.where(cum_count > 0, cum_prem / cum_count, np.nan)

    df['prev_renew'] = g['renewal_flag_binary'].shift().fillna(0)
    df['streak_block'] = (
        (df['prev_renew'] == 0)
        .astype(int)
        .groupby(df[group_cols].apply(tuple, axis=1))
        .cumsum()
    )
    df['retention_streak'] = df.groupby(group_cols + ['streak_block'])['prev_renew'].cumsum()
    df.drop(columns=['prev_renew', 'streak_block'], inplace=True)

    df['lag_1_premium'] = g['total_premium_payable'].shift()
    df['previous_year_premium_ratio'] = df['total_premium_payable'] / df['lag_1_premium']

    df['days_between_renewals'] = g['policy_start_date'].diff().dt.days

    def calc_gaps(d, keys):
        d = d.sort_values(keys + ['policy_start_date', 'policy_end_date'])
        d['_gid'] = d.groupby(keys).ngroup()
        d['_is_pkg'] = d.groupby(['_gid', 'policy_start_date']).cumcount() > 0
        d['_prev_start'] = (
            d.groupby('_gid')['policy_start_date']
            .transform(lambda x: x.shift().where(x != x.shift()).ffill())
        )
        ends = (
            d.groupby(['_gid', 'policy_start_date'])['policy_end_date']
            .min()
            .reset_index()
            .rename(columns={
                'policy_start_date': '_prev_start',
                'policy_end_date': '_prev_min_end'
            })
        )
        d = d.merge(ends, on=['_gid', '_prev_start'], how='left')
        d['days_gap_prev_end_to_curr_start'] = np.where(
            d['_is_pkg'],
            0,
            np.where(
                d['_prev_min_end'].notna(),
                (d['policy_start_date'] - d['_prev_min_end']).dt.days,
                np.nan
            )
        )
        d.drop(columns=['_gid', '_is_pkg', '_prev_start', '_prev_min_end'], inplace=True)
        return d

    df = calc_gaps(df, group_cols)
    print(f"calculation gaps done")

    claim_total = (df['Approved'] + df['Denied']).replace(0, np.nan)
    df['claim_approval_rate'] = df['Approved'] / claim_total
    print(f"claim approval rate done")
    df['vehicle_idv'] = pd.to_numeric(df['vehicle_idv'], errors='coerce')
    df['total_premium_payable'] = pd.to_numeric(df['total_premium_payable'], errors='coerce')

    df['idv_premium_ratio'] = df['vehicle_idv'] / df['total_premium_payable']
    print(f"idv premium ratio done")
    df['add_on_adoption'] = df['before_gst_add_on_gwp'] / df['total_premium_payable'].replace(0, np.nan)
    df['od_tp_ratio'] = df['total_od_premium'] / df['total_tp_premium'].replace(0, np.nan)

    df['lag_1_ncb'] = g['ncb_amount'].shift()
    df['lag_1_od_premium'] = g['total_od_premium'].shift()
    df['lag_1_tp_premium'] = g['total_tp_premium'].shift()

    closed = df.loc[~df['policy_status'].eq('Open'), [
        'renewal_flag_binary', 'state', 'rto_location_clean', 'model_clean',
        'vehicle_segment_clean', 'fuel_type_clean', 'product_name_clean',
        'make_clean'
    ]].copy()

    closed['churn_target'] = 1 - closed['renewal_flag_binary']

    risk_means = (
        closed.melt(id_vars='churn_target', var_name='risk_dim', value_name='key')
              .groupby(['risk_dim', 'key'])['churn_target'].mean()
              .rename('risk_score')
              .reset_index()
    )

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

    print(f"Feature engineering complete – rows: {len(df):,}, cols: {df.shape[1]}")

    df['total_revenue'] = df['total_premium_payable']

    customer_total_revenue = df.groupby('customer_id')['total_revenue'].sum()
    customer_total_purchases = df.groupby('customer_id').size()
    customer_apv = customer_total_revenue / customer_total_purchases

    df_customer = df.drop_duplicates(subset='customer_id', keep='first')
    df_customer['Churned_Binary'] = df_customer['overall_churned'].apply(lambda x: 1 if x == 'Yes' else 0)

    unique_customers = df_customer['customer_id'].nunique()
    churned_customers = df_customer[df_customer['Churned_Binary'] == 1]['customer_id'].nunique()
    churn_rate = churned_customers / unique_customers if unique_customers > 0 else 0
    average_customer_lifespan = 1 / churn_rate if churn_rate != 0 else np.inf
    customer_apf = customer_total_purchases
    customer_clv = customer_total_revenue * average_customer_lifespan
    print("Customer-level metrics calculated")
    customer_metrics_df = pd.DataFrame({
        'customer_id': customer_total_revenue.index,
        'Customer_APV': customer_apv.values,
        'Customer_APF': customer_apf.values,
        'Churn_Rate': churn_rate,
        'Average_Customer_Lifespan': average_customer_lifespan,
        'CLV': customer_clv.values
    })
    print("Customer-level metrics calculated")
    df = df.merge(customer_metrics_df, on='customer_id', how='left')
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
        with fresh_engine.begin() as conn:  # Safe transaction context
            df.to_sql(
                name=TARGET_TABLE,
                schema=TARGET_SCHEMA,
                index=False,
                con=conn,
                if_exists="replace",
                chunksize = 40000,
                method="multi"
            )

        row_count = len(df)
        update_feature_log_newcol(fresh_engine, TARGET_TABLE, row_count)
        print(f"✅ Feature log updated: new_col = {TARGET_TABLE}, count = {row_count}")

    except (PendingRollbackError, OperationalError) as e:
        print(f"⚠️ Transaction rollback/timeout detected: {e}")
        fresh_engine.dispose()
        time.sleep(5)
        retry_engine = hook.get_sqlalchemy_engine()
        with retry_engine.begin() as conn:
            df.to_sql(
                name=TARGET_TABLE,
                schema=TARGET_SCHEMA,
                index=False,
                con=conn,
                if_exists="replace",
                chunksize = 40000,
                method="multi"
            )
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

#     new_column_on_finaltable = PythonOperator(import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import re
from sqlalchemy.exc import PendingRollbackError, OperationalError
import time, gc

POSTGRES_CONN_ID = "postgres_cloud_prochurn"
TARGET_SCHEMA = "pip_bi_dwh"
TARGET_TABLE = "policydata_with_fb_cc_pc_newfea_opti_correct"
SOURCE_SCHEMA = "pip_bi_dwh"
LOG_SCHEMA = "pip_log" 
FEATURE_LOG = "feature_eng_log"


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
    SOURCE_TABLE = get_latest_addons_table(engine)
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

    df['policy_start_date'] = pd.to_datetime(df['policy_start_date'])
    df['policy_end_date'] = pd.to_datetime(df['policy_end_date'])

    def clean_name(name):
        return re.sub(r'[^a-zA-Z0-9]', '', str(name)).lower()
    # ========== START: New Columns (Inserted Mid-Script) ==========
    # 1. claim_happened_flag (based on "policy_no")
    if 'policy_no' in df.columns:
        df['claim_happened_flag'] = df['policy_no'].apply(lambda x: 'no' if pd.isna(x) else 'yes')

    # 2. policy_status (based on renewed_flag)
    if 'renewed_flag' in df.columns:
        df['policy_status'] = df['renewed_flag'].apply(lambda x: 'Open' if x == 2 else ('Not Renewed' if x == 0  else 'Renewed'))
        print(df['policy_status'].value_counts(dropna=False))

    # 3. overall_churned (new logic: based on latest end_year per customer)
    if {'customer_id', 'end_year', 'policy_status'}.issubset(df.columns):
        latest_year_map = df.groupby('customer_id')['end_year'].transform('max')
        final_churn_status = (
            df[df['end_year'] == latest_year_map]
            .groupby('customer_id')['policy_status']
            .transform(lambda x: 'no' if 'renewed' in x.values else 'yes')
        )
        df['overall_churned'] = final_churn_status


    # 4. churn_label (per customer_id & year)
    if {'customer_id', 'policy_end_date', 'policy_status'}.issubset(df.columns):
        df['end_year'] = df['policy_end_date'].dt.year
        def calculate_churn_status(group):
            unique_statuses = group.unique()
            if len(unique_statuses) == 1 and unique_statuses[0] == 'not_renewed':
                return 'yes'
            else:
                return 'no'
        df['churn_label'] = df.groupby(['customer_id', 'end_year'])['policy_status'].transform(lambda x: calculate_churn_status(x))

    # 5. renewal_rate_status (lag + premium compare + gap)
    if {'cleaned_chassis_no', 'cleaned_engine_no', 'corrected_name', 'policy_start_date', 'policy_end_date', 'total_premium_payable'}.issubset(df.columns):
        df = df.sort_values(by=['cleaned_chassis_no', 'cleaned_engine_no', 'corrected_name', 'policy_start_date'])
        gtmp = df.groupby(['cleaned_chassis_no', 'cleaned_engine_no', 'corrected_name'])
        prev_end = gtmp['policy_end_date'].shift()
        prev_premium = gtmp['total_premium_payable'].shift()
        curr_premium = df['total_premium_payable']
        valid_gap = df['policy_start_date'] >= (prev_end + pd.Timedelta(days=1))

        df['renewal_rate_status'] = 'null'
        df.loc[valid_gap & (curr_premium > prev_premium), 'renewal_rate_status'] = 'increase'
        df.loc[valid_gap & (curr_premium < prev_premium), 'renewal_rate_status'] = 'decrease'
        df.loc[valid_gap & (curr_premium == prev_premium), 'renewal_rate_status'] = 'no_change'

    # ========== END: New Columns (Inserted Mid-Script) ==========

    df['rto_location_clean'] = df['rto_location'].apply(clean_name)
    df['fuel_type_clean'] = df['fuel_type'].apply(clean_name)
    df['product_name_clean'] = df['product_name'].apply(clean_name)
    df['vehicle_segment_clean'] = df['vehicle_segment'].apply(clean_name)
    df['new_vertical_clean'] = df['new_vertical'].apply(clean_name)
    df['make_clean'] = df['make'].apply(clean_name)
    df['model_clean'] = df['cleaned_model'].apply(clean_name)

    cat_cols = [
        'policy_status', 'state', 'rto_location_clean',
        'model_clean', 'fuel_type_clean', 'make_clean', 'product_name_clean',
        'vehicle_segment_clean', 'new_vertical_clean'
    ]
    for c in cat_cols:
        df[c] = df[c].astype('category')

    group_cols = ['cleaned_chassis_no', 'cleaned_engine_no', 'corrected_name']
    df = df.sort_values(group_cols + ['policy_start_date', 'policy_end_date'])

    df['renewal_flag_binary'] = df['policy_status'].map({'Renewed': 1, 'Not Renewed': 0}).fillna(0).astype(int)
    # df['renewal_flag_binary'] = df['renewal_flag_binary'].astype(int)
    df['is_active'] = df['policy_status'].eq('Open')



    g = df.groupby(group_cols)
    cum_sum = g['renewal_flag_binary'].cumsum() - df['renewal_flag_binary']
    cum_count = g.cumcount()
    df['retention_rate_pct'] = np.where(cum_count > 0, cum_sum / cum_count, np.nan)

    cum_prem = g['total_premium_payable'].cumsum() - df['total_premium_payable']
    df['avg_premium_hist'] = np.where(cum_count > 0, cum_prem / cum_count, np.nan)

    df['prev_renew'] = g['renewal_flag_binary'].shift().fillna(0)
    df['streak_block'] = (
        (df['prev_renew'] == 0)
        .astype(int)
        .groupby(df[group_cols].apply(tuple, axis=1))
        .cumsum()
    )
    df['retention_streak'] = df.groupby(group_cols + ['streak_block'])['prev_renew'].cumsum()
    df.drop(columns=['prev_renew', 'streak_block'], inplace=True)

    df['lag_1_premium'] = g['total_premium_payable'].shift()
    df['previous_year_premium_ratio'] = df['total_premium_payable'] / df['lag_1_premium']

    df['days_between_renewals'] = g['policy_start_date'].diff().dt.days

    def calc_gaps(d, keys):
        d = d.sort_values(keys + ['policy_start_date', 'policy_end_date'])
        d['_gid'] = d.groupby(keys).ngroup()
        d['_is_pkg'] = d.groupby(['_gid', 'policy_start_date']).cumcount() > 0
        d['_prev_start'] = (
            d.groupby('_gid')['policy_start_date']
            .transform(lambda x: x.shift().where(x != x.shift()).ffill())
        )
        ends = (
            d.groupby(['_gid', 'policy_start_date'])['policy_end_date']
            .min()
            .reset_index()
            .rename(columns={
                'policy_start_date': '_prev_start',
                'policy_end_date': '_prev_min_end'
            })
        )
        d = d.merge(ends, on=['_gid', '_prev_start'], how='left')
        d['days_gap_prev_end_to_curr_start'] = np.where(
            d['_is_pkg'],
            0,
            np.where(
                d['_prev_min_end'].notna(),
                (d['policy_start_date'] - d['_prev_min_end']).dt.days,
                np.nan
            )
        )
        d.drop(columns=['_gid', '_is_pkg', '_prev_start', '_prev_min_end'], inplace=True)
        return d

    df = calc_gaps(df, group_cols)
    print(f"calculation gaps done")

    claim_total = (df['Approved'] + df['Denied']).replace(0, np.nan)
    df['claim_approval_rate'] = df['Approved'] / claim_total
    print(f"claim approval rate done")
    df['vehicle_idv'] = pd.to_numeric(df['vehicle_idv'], errors='coerce')
    df['total_premium_payable'] = pd.to_numeric(df['total_premium_payable'], errors='coerce')

    df['idv_premium_ratio'] = df['vehicle_idv'] / df['total_premium_payable']
    print(f"idv premium ratio done")
    df['add_on_adoption'] = df['before_gst_add_on_gwp'] / df['total_premium_payable'].replace(0, np.nan)
    df['od_tp_ratio'] = df['total_od_premium'] / df['total_tp_premium'].replace(0, np.nan)

    df['lag_1_ncb'] = g['ncb_amount'].shift()
    df['lag_1_od_premium'] = g['total_od_premium'].shift()
    df['lag_1_tp_premium'] = g['total_tp_premium'].shift()

    closed = df.loc[~df['policy_status'].eq('Open'), [
        'renewal_flag_binary', 'state', 'rto_location_clean', 'model_clean',
        'vehicle_segment_clean', 'fuel_type_clean', 'product_name_clean',
        'make_clean'
    ]].copy()

    closed['churn_target'] = 1 - closed['renewal_flag_binary']

    risk_means = (
        closed.melt(id_vars='churn_target', var_name='risk_dim', value_name='key')
              .groupby(['risk_dim', 'key'])['churn_target'].mean()
              .rename('risk_score')
              .reset_index()
    )

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

    print(f"Feature engineering complete – rows: {len(df):,}, cols: {df.shape[1]}")

    df['total_revenue'] = df['total_premium_payable']

    customer_total_revenue = df.groupby('customer_id')['total_revenue'].sum()
    customer_total_purchases = df.groupby('customer_id').size()
    customer_apv = customer_total_revenue / customer_total_purchases

    df_customer = df.drop_duplicates(subset='customer_id', keep='first')
    df_customer['Churned_Binary'] = df_customer['overall_churned'].apply(lambda x: 1 if x == 'Yes' else 0)

    unique_customers = df_customer['customer_id'].nunique()
    churned_customers = df_customer[df_customer['Churned_Binary'] == 1]['customer_id'].nunique()
    churn_rate = churned_customers / unique_customers if unique_customers > 0 else 0
    average_customer_lifespan = 1 / churn_rate if churn_rate != 0 else np.inf
    customer_apf = customer_total_purchases
    customer_clv = customer_total_revenue * average_customer_lifespan
    print("Customer-level metrics calculated")
    customer_metrics_df = pd.DataFrame({
        'customer_id': customer_total_revenue.index,
        'Customer_APV': customer_apv.values,
        'Customer_APF': customer_apf.values,
        'Churn_Rate': churn_rate,
        'Average_Customer_Lifespan': average_customer_lifespan,
        'CLV': customer_clv.values
    })
    print("Customer-level metrics calculated")
    df = df.merge(customer_metrics_df, on='customer_id', how='left')
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
