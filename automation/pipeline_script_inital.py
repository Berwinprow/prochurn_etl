from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from load_data_to_postgres_stage import load_data_to_postgres_stage
from base_clean import cleanse_and_load_base_tables
from clean_pr import clean_and_load_pr_data
from baseprappend_inital import append_base_pr_initial
# from Furzzy_match import fuzzy_matching



default_args = {
    'owner':'etl_pipeline_initial',
    'start_date': datetime(2025,1,1),
    'retries':0
}

with DAG(
    dag_id ='liberty_etl_data_pipeline_initial',
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
        python_callable = append_base_pr_initial

    )

    # fuzzy_match = PythonOperator(
    #     task_id = "adding_fuzzy_matching_for_basepr_append",
    #     python_callable = fuzzy_matching
    # )


    clean_base_data