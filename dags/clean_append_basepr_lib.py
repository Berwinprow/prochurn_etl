from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import re

# --------------------------------------------------------------------
# 🔧 CONFIG (Hardcoded)
# --------------------------------------------------------------------
SOURCE_SCHEMA = "test_aggregation"
SOURCE_TABLE = "appended_base_and_pr"
TARGET_SCHEMA = "test_aggregation"
TARGET_TABLE = "basiccleaned_appended_base_and_pr"
POSTGRES_CONN_ID = "postgres_cloud_prochurn"
OUTER_CHUNK = 10000
INNER_CHUNK = 5000

# ----------------------------------------------------------------------
# Load Data To Postgres
# ----------------------------------------------------------------------
def safe_load(df, table_name, schema):
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



# --------------------------------------------------------------------
# 🧩 MAIN CLEANING FUNCTION 
# --------------------------------------------------------------------
def clean_appended_base_pr():
    print("🔗 Connecting to PostgreSQL...")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # ---------------------------
    # Step 1: Load Data
    # ---------------------------
    query = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE};"
    df = pd.read_sql(query, con=engine)
    print(f"✅ Loaded {len(df)} rows from {SOURCE_TABLE}")
    # Convert date columns to datetime using the names in your dataset
    df['policy_start_date'] = pd.to_datetime(df['policy_start_date'], errors='coerce')
    df['policy_end_date'] = pd.to_datetime(df['policy_end_date'], errors='coerce')

    before_df = df.copy()
    df = df.dropna(subset=['policy_start_date', 'policy_end_date'])
    removed = before_df[
        before_df['policy_start_date'].isna() |
        before_df['policy_end_date'].isna()
    ]
    print(f"🗑️ Removed {len(removed)} rows due to missing start/end date")
    # Distinct removed values
    removed_start = removed['policy_start_date'].unique()
    removed_end = removed['policy_end_date'].unique()
    print("🟠 Distinct removed policy_start_date:", removed_start)
    print("🟠 Distinct removed policy_end_date:", removed_end)

    # ---------------------------
    # Step 2: Filter Premium Values
    # ---------------------------
    # Use the column "total premium payable" as provided in your column list
    df['total_premium_payable'] = pd.to_numeric(df['total_premium_payable'].astype(str).str.strip(), errors='coerce')
    before_premium = len(df)
    print(f'before premium {before_premium}')
    df = df[df['total_premium_payable'].notnull() & (df['total_premium_payable'] > 0.01)]
    print(f' after premium removal: {len(df)}')
    # ---------------------------
    # Step 3: Calculate Policy Tenure & Filter
    # ---------------------------
    def calculate_tenure_exact(start_date, end_date):
        diff = relativedelta(end_date, start_date)
        return diff.years * 12 + diff.months + (diff.days >= 0)
    before_tenure = len(df)
    print(f'before tenure calc: {before_tenure}')
    df['Policy_Tenure(check)'] = df.apply(lambda row: calculate_tenure_exact(row['policy_start_date'], row['policy_end_date']), axis=1)
    df = df[df['Policy_Tenure(check)'] > 10]
    
    print(f'after tenure cal : {len(df)}')
    # ---------------------------
    # Step 4: Handle Duplicates and Prioritize
    # ---------------------------
    def prioritize_rows(group):
        # Count null values in each row to help with prioritization
        group['null_count'] = group.isnull().sum(axis=1)
        group = group.sort_values(by=['null_count', 'booked', 'policy_start_date'], ascending=[True, False, True])
        return group.iloc[0]

    # Identify duplicate rows based on 'policy no', 'policy start date', and 'policy end date'
    duplicates = df[df.duplicated(subset=['policy_no', 'policy_start_date', 'policy_end_date'], keep=False)]
   
    cleaned_duplicates = (
        duplicates.groupby(['policy_no', 'policy_start_date', 'policy_end_date'])
        .apply(prioritize_rows)
        .reset_index(drop=True)
    )
    df_cleaned = df.drop_duplicates(subset=['policy_no', 'policy_start_date', 'policy_end_date'], keep=False)
    df_cleaned = pd.concat([df_cleaned, cleaned_duplicates], ignore_index=True)
    print(f"✅ After deduplication → {len(df_cleaned)} rows remain")

    len(df_cleaned)
    import re

    # ---------------------------
    # Step 6: Clean Specified Name Columns
    # ---------------------------
    def clean_name(name):
        return re.sub(r'[^a-zA-Z0-9]', '', str(name)).lower()

    columns_to_clean = {
        "insured_name": "Cleaned_insured_name",
        "new_branch_name_2": "Cleaned_Branch_Name_2",
        "state": "Cleaned_State_2",
        "zone": "Cleaned_Zone_2",
        "chassis_no": "Cleaned_Chassis_Number",
        "engine_no": "Cleaned_Engine_Number",
        "veh_reg_no": "Cleaned_Reg_no"
    }
    for orig_col, new_col in columns_to_clean.items():
        if orig_col in df_cleaned.columns:
            df_cleaned[new_col] = df_cleaned[orig_col].apply(clean_name)
            print(f"✨ Created cleaned column: '{new_col}' from '{orig_col}'")
    # ---------------------------
    # Step 6: Print Null Counts Before Write
    # ---------------------------
    chassis_nulls = df_cleaned['Cleaned_Chassis_Number'].isnull().sum() if 'Cleaned_Chassis_Number' in df_cleaned.columns else 'N/A'
    engine_nulls = df_cleaned['Cleaned_Engine_Number'].isnull().sum() if 'Cleaned_Engine_Number' in df_cleaned.columns else 'N/A'
    print(f"🔍 Null count before write:")
    print(f"   - Cleaned Chassis Number: {chassis_nulls}")
    print(f"   - Cleaned Engine Number : {engine_nulls}")
    df_cleaned.columns = (
        df_cleaned.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    print("📝 Column names normalized to lowercase_with_underscores")
    # ---------------------------
    # Step 7: Write to Target Table
    # ---------------------------
    print(f"💾 Writing cleaned data to {TARGET_SCHEMA}.{TARGET_TABLE} ...")
    print(f'final data : {len(df_cleaned)}')
    safe_load(df_cleaned, TARGET_TABLE, TARGET_SCHEMA)
    print(f"✅ Successfully written {len(df_cleaned)} cleaned rows to {TARGET_SCHEMA}.{TARGET_TABLE}")

# --------------------------------------------------------------------
# 🪄 Airflow DAG Definition
# --------------------------------------------------------------------
with DAG(
    dag_id="clean_appended_base_pr_dag",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2024, 2, 10),
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    schedule_interval=None,
    catchup=False,
    tags=["cleaning", "policy", "postmerge"]
) as dag:

    clean_task = PythonOperator(
        task_id="cleaning_appended_data",
        python_callable=clean_appended_base_pr,
        provide_context=True
    )

    clean_task
