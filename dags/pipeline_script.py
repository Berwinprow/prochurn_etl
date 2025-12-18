from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import logging
from pathlib import Path


# --- Import ETL Stage Functions ---
from load_data_to_postgres_stage_test_flake import load_data_to_postgres_stage
from base_clean import cleanse_and_load_base_tables
from clean_pr import clean_and_load_pr_data
from baseprappend_inital import append_base_pr_initial
from Furzzy_match import fuzzy_matching
from claim_load_append_merge import append_claim_table , merge_claim_table , merge_basepr_with_claim
from addons import addon_column
from new_column_features import build_policy_features, update_renewal_rate_status
from base_pr_append import run_all_iterations
from schema_table_config import ensure_all_schemas,get_logtable_details_from_json


postgres_conn_id = "postgres_cloud_prochurn"
DAG_DIR = Path(__file__).resolve().parent
json_path = str(DAG_DIR/"config"/"schema_metadata_config.json")
# ---------------------------------------------------------------------
# ✅ Custom Failure Email Callback (Gmail-based)
# ---------------------------------------------------------------------
# def send_failure_email(context):
#     dag_id = context.get('dag').dag_id
#     task_id = context.get('task_instance').task_id
#     exception = context.get('exception')
#     execution_date = context.get('execution_date')
#     log_url = context.get('task_instance').log_url

#     subject = f"🚨 Airflow Task Failed: {dag_id}.{task_id}"

#     html_content = f"""
#     <h3>🔴 Airflow Task Failure Alert</h3>
#     <p><b>DAG:</b> {dag_id}</p>
#     <p><b>Task:</b> {task_id}</p>
#     <p><b>Execution Date:</b> {execution_date}</p>
#     <p><b>Error:</b> {exception}</p>
#     <p><a href="{log_url}">🔗 View Logs</a></p>
#     """

#     # ✅ Send via Gmail SMTP connection
#     send_email(
#         to=["berwin.rayen@prowesstics.com"],
#         subject=subject,
#         html_content=html_content,
#         conn_id="smtp_default"  # Use your working Gmail connection
#     )
#     logging.info(f"[send_failure_email] Alert sent for task: {task_id}")

# ---------------------------------------------------------------------
# ✅ Optional Fallback Task (triggered when any task fails)
# ---------------------------------------------------------------------
# def fallback_reprocess(**kwargs):
#     logging.warning("[fallback_reprocess] Triggered fallback flow due to failure.")
#     try:
#         dag_run = kwargs.get('dag_run')
#         failed_tasks = [
#             ti.task_id for ti in dag_run.get_task_instances() if ti.state == 'failed'
#         ]
#         logging.info(f"[fallback_reprocess] Failed tasks: {failed_tasks}")

#         html_content = f"""
#         <h3>⚠️ ETL Fallback Triggered</h3>
#         <p>Some tasks failed in DAG <b>{dag_run.dag_id}</b>.</p>
#         <p><b>Failed Tasks:</b> {', '.join(failed_tasks) or 'None'}</p>
#         <p><b>Run ID:</b> {dag_run.run_id}</p>
#         <p>Fallback initiated to handle partial recovery.</p>
#         """

#         send_email(
#             to=["berwin.rayen@prowesstics.com"],
#             subject="⚠️ Fallback Triggered – ETL Recovery Started",
#             html_content=html_content,
#             conn_id="smtp_default"
#         )

#         # 🧰 You can add your minimal recovery or retry logic here
#         logging.info("[fallback_reprocess] ✅ Fallback recovery completed successfully.")
#     except Exception as e:
#         logging.error(f"[fallback_reprocess] ❌ Fallback failed: {e}")
#         raise

# ---------------------------------------------------------------------
# ✅ Default DAG Arguments
# ---------------------------------------------------------------------
default_args = {
    'owner': 'etl_pipeline_initial',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'on_failure_callback': send_failure_email,
}

# ---------------------------------------------------------------------
# ✅ DAG Definition
# ---------------------------------------------------------------------
with DAG(
    dag_id='initial_pipeline_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['liberty', 'azure', 'postgres', 'etl', 'resilient'],
) as dag:

    start = DummyOperator(task_id="start_pipeline")

    #  ---------------- Stage 0 ----------------
    # create_Schema = PythonOperator(
    #     task_id='Create_all_schemas',
    #     python_callable=ensure_all_schemas,
    #     provide_context=True,
    #     op_kwargs={
    #             "conn_id": postgres_conn_id,
    #             "json_path": json_path,
    #         },
    # )
    # create_log_tables = PythonOperator(
    #     task_id= "create_log_schema",
    #     python_callable = get_logtable_details_from_json,
    #     op_kwargs = {"conn_id": postgres_conn_id , "json_path":json_path},
    # )
    # ---------------- Stage 1 ----------------
    # load_initial_data = PythonOperator(
    #     task_id='initial_data_load',
    #     python_callable=load_data_to_postgres_stage,
    #     provide_context=True,
    # )

    # ---------------- Stage 2 ----------------
    clean_base_data = PythonOperator(
        task_id="clean_and_load_base_data",
        python_callable=cleanse_and_load_base_tables,
        provide_context=True,
    )

    # ---------------- Stage 3 ----------------
    clean_pr_data = PythonOperator(
        task_id="clean_pr_file_data",
        python_callable=clean_and_load_pr_data,
        provide_context=True,
    )

    # append_basepr_initial = PythonOperator(
    #     task_id="base_pr_data_appending",
    #     python_callable=append_base_pr_initial,
    #     provide_context=True,
    # )

    # ---------------- Stage 4 ----------------
    append_basepr = PythonOperator(
        task_id="base_pr_data_appending",
        python_callable=run_all_iterations,
        provide_context=True,
    )

    # ---------------- Stage 5 (Optional) ----------------
    fuzzy_match = PythonOperator(
        task_id="adding_fuzzy_matching_for_basepr_append",
        python_callable=fuzzy_matching,
        provide_context=True,
    )
    # append_claim = PythonOperator(
    #     task_id = "append_claim",
    #     python_callable = append_claim_table
    # )

    # merge_claim = PythonOperator(
    #     task_id = "mergeclaim",
    #     python_callable = merge_claim_table
    # )

    merge_baseprclaim = PythonOperator(
        task_id = "mergebaseprwithclaim",
        python_callable = merge_basepr_with_claim
    )
    add_on = PythonOperator(
        task_id="add_on",
        python_callable=addon_column,
        provide_context=True,
    )

    new_column_features = PythonOperator(
        task_id="new_column_features",
        python_callable=build_policy_features,
        provide_context=True,
    )

    update_renewal_task = PythonOperator(
        task_id="renewal_rate_update",
        python_callable=update_renewal_rate_status,
        provide_context=True,
    )

      # ---------------- EMAIL ALERT ON FAILURE ----------------
    # send_failure_email = EmailOperator(
    #     task_id='notify_failure_email',
    #     to=["berwin.rayen@prowesstics.com"],
    #     subject='🚨 Airflow Alert: {{ dag.dag_id }} Task Failure',
    #     html_content="""
    #     <h3>🔴 Airflow Task Failed</h3>
    #     <p><b>DAG:</b> {{ dag.dag_id }}</p>
    #     <p><b>Task:</b> {{ task_instance.task_id }}</p>
    #     <p><b>Execution Date:</b> {{ ts }}</p>
    #     <p><a href="{{ task_instance.log_url }}">🔗 View Logs</a></p>
    #     """,
    #     trigger_rule=TriggerRule.ONE_FAILED,  # ✅ triggers if *any* upstream task fails
    # )

    # # ---------------- FALLBACK RECOVERY ----------------
    # fallback_task = PythonOperator(
    #     task_id="fallback_reprocess_if_failed",
    #     python_callable=fallback_reprocess,
    #     provide_context=True,
    #     trigger_rule=TriggerRule.ONE_FAILED,  # ✅ runs when any upstream task fails
    # )

    # ---------------- END ----------------
    end = DummyOperator(
        task_id="end_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ---------------- DAG Flow ----------------
    

    start >> [clean_base_data , clean_pr_data ] >> append_basepr >> fuzzy_match
    fuzzy_match >> merge_baseprclaim >> add_on >> update_renewal_task >> new_column_features >> end
    
    # start >> [clean_base_data , clean_pr_data] >> append_basepr_initial

