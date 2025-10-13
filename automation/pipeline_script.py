from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from load_data_to_postgres_stage import load_data_to_postgres_stage
from base_clean import cleanse_and_load_base_tables
from clean_pr import clean_and_load_pr_data
from base_pr_append import run_all_iterations
from Furzzy_match import fuzzy_matching
from claim_load_append_merge import append_claim_table , merge_claim_table , merge_basepr_with_claim
from addons import addon_column
from new_column_features import build_policy_features


default_args = {
    'owner':'etl_pipeline',
    'start_date': datetime(2025,1,1),
    'retries':5
}

with DAG(
    dag_id ='liberty_etl_data_pipeline',
    default_args =default_args,
    schedule_interval = None,
    catchup=False
)as dag:

    load_initial_data= PythonOperator(
        task_id = 'initial_data_load',
        python_callable = load_data_to_postgres_stage,
        
    )

    clean_base_data = PythonOperator(
        task_id="clean_and_load_base_data",
        python_callable=cleanse_and_load_base_tables
    )

    clean_pr_data = PythonOperator(
        task_id="clean_pr_file_data", 
        python_callable=clean_and_load_pr_data
    )
    append_basepr = PythonOperator(
        task_id = "base_pr_data_appending",
        python_callable = run_all_iterations

    )

    fuzzy_match = PythonOperator(
        task_id = "adding_fuzzy_matching_for_basepr_append",
        python_callable = fuzzy_matching
    )
    append_claim = PythonOperator(
        task_id = "append_claim",
        python_callable = append_claim_table
    )

    merge_claim = PythonOperator(
        task_id = "mergeclaim",
        python_callable = merge_claim_table
    )

    merge_baseprclaim = PythonOperator(
        task_id = "mergebaseprwithclaim",
        python_callable = merge_basepr_with_claim
    )

    addons = PythonOperator(
        task_id = "addon_columns",
        python_callable = addon_column
    )

    new_col = PythonOperator(
        task_id = "build_policy_features",
        python_callable = build_policy_features
    )

    load_initial_data >> clean_base_data >> clean_pr_data >> append_basepr >> fuzzy_match >> append_claim >> merge_claim >> merge_baseprclaim >> addons >> new_col