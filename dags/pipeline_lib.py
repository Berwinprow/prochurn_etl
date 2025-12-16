from datetime import datetime, timedelta
from pathlib import Path
import logging

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# --- ETL stage functions (imported libs) ---
from load_data_to_postgres_lib import load_data_to_postgres_stage
from base_clean_lib import (
    clean_base_2024,
    clean_other_base_tables,
)
from clean_pr_lib import (
    clean_pr_2022,
    clean_pr_2023,
    clean_pr_2024,
)
from overall_basepr_append_lib import append_base, append_pr
from clean_append_basepr_lib import (
    clean_appended_base_pr,
    append_basepr,
)
from chassis_engine_clean_lib import (
    archive_chassis_logs,
    run_samechassis_process,
    clean_basiccleaned_basepr,
    clean_samechassis_engino,
    duplicate_cleaning_basiccleaned_basepr,
    duplicate_cleaning_clean_samechassis_engino,
    final_union,
)
from correcting_customer_name_lib import (
    corrected_name_null_cases,
    clean_corrected_name_fuzzy,
    archive_log,
)
from booked_case_handeling_lib import trim_columns, update_booked_status
from old_policy_mapping_lib import old_policy_mapping
from claim_append_merge_load_lib import (
    append_claim_table,
    merge_claim_table,
    merge_basepr_with_claim,
)
from feature_engineering_lib import (
    anomalies_claimerge_external_factors,
    add_policy_chain_features,
)
from altering_column_features_lib import (
    convert_and_reload,
    update_overall_churned,
    update_renewal_rate_status,
)
from creating_new_features_lib import pricing_catlog, build_policy_features
from future_prediction_lib import future_prediction
from not_renewed_reason_lib import (
    call_pred_data_def,
    call_historic_data_def,
)
from top_3_reason_lib import top_3_reason
from customer_segmentation_lib import cus_segmentation
from schema_table_config import (
    ensure_all_schemas,
    get_logtable_details_from_json,
)
from model_health_monitoring_lib import Monitoring


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
postgres_conn_id = "postgres_cloud_prochurn"
DAG_DIR = Path(__file__).resolve().parent
json_path = str(DAG_DIR / "config" / "schema_metadata_config.json")


# ---------------------------------------------------------------------
# (Optional) Failure email callback (commented out)
# ---------------------------------------------------------------------
def send_failure_email(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    exception = context.get('exception')
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    subject = f"🚨 Airflow Task Failed: {dag_id}.{task_id}"
    html_content = f"""
    <h3>🔴 Airflow Task Failure Alert</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Execution Date:</b> {execution_date}</p>
    <p><b>Error:</b> {exception}</p>
    <p><a href="{log_url}">🔗 View Logs</a></p>
    """

    send_email(
        to=["berwin.rayen@prowesstics.com"],
        subject=subject,
        html_content=html_content,
        conn_id="smtp_default",
    )
    logging.info(f"[send_failure_email] Alert sent for task: {task_id}")


# ---------------------------------------------------------------------
# (Optional) Fallback task for failures (commented out)
# ---------------------------------------------------------------------
# def fallback_reprocess(**kwargs):
#     logging.warning("[fallback_reprocess] Fallback triggered.")
#     try:
#         dag_run = kwargs.get('dag_run')
#         failed_tasks = [
#             ti.task_id for ti in dag_run.get_task_instances()
#             if ti.state == 'failed'
#         ]
#         logging.info(f"[fallback_reprocess] Failed: {failed_tasks}")
#         html_content = f"""
#         <h3>⚠️ ETL Fallback Triggered</h3>
#         <p>Some tasks failed in DAG <b>{dag_run.dag_id}</b>.</p>
#         <p><b>Failed Tasks:</b> {', '.join(failed_tasks) or 'None'}</p>
#         <p><b>Run ID:</b> {dag_run.run_id}</p>
#         """
#         send_email(
#             to=["berwin.rayen@prowesstics.com"],
#             subject="⚠️ Fallback Triggered – ETL Recovery Started",
#             html_content=html_content,
#             conn_id="smtp_default",
#         )
#         logging.info("[fallback_reprocess] ✅ Fallback completed.")
#     except Exception as e:
#         logging.error(f"[fallback_reprocess] ❌ Fallback failed: {e}")
#         raise


# ---------------------------------------------------------------------
# Default DAG args
# ---------------------------------------------------------------------
default_args = {
    "owner": "etl_pipeline_initial",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    'on_failure_callback': send_failure_email,
}


# ---------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------
with DAG(
    dag_id="liberty_etl_data_pipeline_lib",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=[
        "liberty",
        "azure",
        "postgres",
        "etl",
        "resilient",
    ],
) as dag:

    # ---------------- START ----------------
    with TaskGroup(group_id="start") as stage0_0_creation:
        start = DummyOperator(task_id="start_pipeline")

    # ---------------- Stage 0 ----------------
    with TaskGroup(group_id="schema_creation") as stage0_creation:
        create_Schema_task = PythonOperator(
            task_id="Create_all_schemas",
            python_callable=ensure_all_schemas,
            op_kwargs={
                "conn_id": postgres_conn_id,
                "json_path": json_path,
            },
            provide_context=True,
        )
        create_log_tables_task = PythonOperator(
            task_id="create_log_schema",
            python_callable=get_logtable_details_from_json,
            op_kwargs={
                "conn_id": postgres_conn_id,
                "json_path": json_path,
            },
        )

    # # ---------------- STAGE 1 ----------------
    # with TaskGroup(group_id="data_ingestion") as stage1_ingestion:
    #     etl_task = PythonOperator(
    #         task_id="process_azure_blob_to_postgres",
    #         python_callable=load_data_to_postgres_stage,
    #         provide_context=True,
    #     )

    # ---------------- STAGE 2 ----------------
    with TaskGroup(group_id="data_cleaning") as stage2_cleaning:
        clean_base_2024_task = PythonOperator(
            task_id="clean_base_2024",
            python_callable=clean_base_2024,
            provide_context=True,
        )

        clean_other_base_tables_task = PythonOperator(
            task_id="clean_other_base_tables",
            python_callable=clean_other_base_tables,
            provide_context=True,
        )

        clean_pr_2022_task = PythonOperator(
            task_id="clean_pr_2022",
            python_callable=clean_pr_2022,
            provide_context=True,
        )

        clean_pr_2023_task = PythonOperator(
            task_id="clean_pr_2023",
            python_callable=clean_pr_2023,
            provide_context=True,
        )

        clean_pr_2024_task = PythonOperator(
            task_id="clean_pr_2024",
            python_callable=clean_pr_2024,
            provide_context=True,
        )

        append_claim = PythonOperator(
            task_id="append_claim_tables",
            python_callable=append_claim_table,
        )

    # ---------------- STAGE 3 ----------------
    with TaskGroup(group_id="data_appending") as stage3_appending:
        append_base_data = PythonOperator(
            task_id="append_base_tables",
            python_callable=append_base,
            provide_context=True,
        )

        append_pr_data = PythonOperator(
            task_id="append_pr_tables",
            python_callable=append_pr,
            provide_context=True,
        )

        append_base_pr = PythonOperator(
            task_id="final_merge_base_pr",
            python_callable=append_basepr,
            provide_context=True,
        )

        clean_appended_basepr_task = PythonOperator(
            task_id="cleaning_appended_data",
            python_callable=clean_appended_base_pr,
            provide_context=True,
        )

    # ---------------- STAGE 3 CLEANING ----------------
    with TaskGroup(group_id="chassis_cleaning") as stage3_part2_cleaning:
        archive_logs_task = PythonOperator(
            task_id="archive_chassis_logs",
            python_callable=archive_chassis_logs,
            provide_context=True,
        )

        create_same_chassis_table_task = PythonOperator(
            task_id="create_samechassis_table",
            python_callable=run_samechassis_process,
            provide_context=True,
        )

        clean_basic_table_task = PythonOperator(
            task_id="clean_chassis_engine_no_basic",
            python_callable=clean_basiccleaned_basepr,
            provide_context=True,
        )

        clean_samechassis_table_task = PythonOperator(
            task_id="clean_same_chassis_engine_no",
            python_callable=clean_samechassis_engino,
            provide_context=True,
        )

        duplicate_basic_cleaning_task = PythonOperator(
            task_id="clean_duplicate_task_basic",
            python_callable=duplicate_cleaning_basiccleaned_basepr,
            provide_context=True,
        )

        duplicate_cleaning_samechassis_task = PythonOperator(
            task_id="duplicate_cleaning_same_chassis",
            python_callable=duplicate_cleaning_clean_samechassis_engino,
            provide_context=True,
        )

        final_union_task = PythonOperator(
            task_id="final_running",
            python_callable=final_union,
            provide_context=True,
        )

    # ---------------- Stage 3 part3 - name cleaning ----------------
    with TaskGroup(group_id="customer_name_cleaning") as stage3_part3_cleaning:
        archive_null_log_task = PythonOperator(
            task_id="archive_null_log",
            python_callable=archive_log,
            op_kwargs={"log_table": "corrected_name_null_log"},
            provide_context=True,
        )

        archive_fuzzy_log_task = PythonOperator(
            task_id="archive_fuzzy_log",
            python_callable=archive_log,
            op_kwargs={"log_table": "corrected_name_fuzzy_log"},
            provide_context=True,
        )

        null_case_task = PythonOperator(
            task_id="null_case_handeling",
            python_callable=corrected_name_null_cases,
            provide_context=True,
        )

        furzzy_task = PythonOperator(
            task_id="furzzy_match",
            python_callable=clean_corrected_name_fuzzy,
            provide_context=True,
        )

    # ---------------- Stage 3 part4 - booked / oldpolicy ----------------
    with TaskGroup(group_id="bookedcases_oldpolicy_cleaning") as stage3_part4_cleaning:
        trim_task = PythonOperator(
            task_id="unwanted_column_removal",
            python_callable=trim_columns,
        )

        booked_task = PythonOperator(
            task_id="booked_cases",
            python_callable=update_booked_status,
            provide_context=True,
        )

        policy_mapping_task = PythonOperator(
            task_id="Mapping_Old_Policy",
            python_callable=old_policy_mapping,
        )

    # ---------------- Stage 3 part5 - merging ----------------
    with TaskGroup(group_id="data_merging") as stage3_part5_merging:
        merge_claim = PythonOperator(
            task_id="merge_claim_table",
            python_callable=merge_claim_table,
        )

        final_table = PythonOperator(
            task_id="basepr_append_with_claim",
            python_callable=merge_basepr_with_claim,
        )

    # ---------------- STAGE 4 ----------------
    with TaskGroup(group_id="feature_engineering") as stage4_processing:
        anomali_task = PythonOperator(
            task_id="external_factors_addons",
            python_callable=anomalies_claimerge_external_factors,
        )

        sql_task = PythonOperator(
            task_id="sql_column_addon",
            python_callable=add_policy_chain_features,
        )

        datatype_convert_task = PythonOperator(
            task_id="convert_datatypes_and_reload",
            python_callable=convert_and_reload,
        )

        overall_churned_task = PythonOperator(
            task_id="update_overall_churned",
            python_callable=update_overall_churned,
        )

        renewal_rate_task = PythonOperator(
            task_id="update_renewal_rate_status",
            python_callable=update_renewal_rate_status,
        )

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

    # ---------------- STAGE 5 ----------------
    with TaskGroup(group_id="prediction_renewal") as stage5_processing:
        prediction_task = PythonOperator(
            task_id="renewed_notrenewed_pred",
            python_callable=future_prediction,
        )

        task_model_prediction = PythonOperator(
            task_id="reason_for_prediction_table",
            python_callable=call_pred_data_def,
        )

        task_policy_status = PythonOperator(
            task_id="reason_for_policy_status_table",
            python_callable=call_historic_data_def,
        )

        task_top3 = PythonOperator(
            task_id="generate_top_3_reasons",
            python_callable=top_3_reason,
        )

        segmentation_task = PythonOperator(
            task_id="customer_segmenatation",
            python_callable=cus_segmentation,
        )

    # ---------------- STAGE 6 ----------------
    with TaskGroup(group_id="health_monitoring") as stage6_monitoring:
        monitoring_task = PythonOperator(
            task_id="model_health_monitoring",
            python_callable=Monitoring,
        )

    # ---------------- END ----------------
    end = DummyOperator(
        task_id="end_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ---------------- DAG Flow ----------------
    start >> create_Schema_task >> create_log_tables_task 

    create_log_tables_task >> [
        clean_base_2024_task,
        clean_other_base_tables_task,
        clean_pr_2022_task,
        clean_pr_2023_task,
        clean_pr_2024_task,
        append_claim,
    ]

    upstream_tasks = [
        clean_base_2024_task,
        clean_other_base_tables_task,
        clean_pr_2022_task,
        clean_pr_2023_task,
        clean_pr_2024_task,
    ]

    upstream_tasks >> append_base_data
    upstream_tasks >> append_pr_data

    # append_base_pr should wait for ALL three
    [append_base_data, append_pr_data] >> append_base_pr

    append_base_pr >> clean_appended_basepr_task
    clean_appended_basepr_task >> archive_logs_task
    archive_logs_task >> create_same_chassis_table_task
    create_same_chassis_table_task >> [
        clean_basic_table_task,
        clean_samechassis_table_task,
    ]

    clean_basic_table_task >> duplicate_basic_cleaning_task
    clean_basic_table_task >> duplicate_cleaning_samechassis_task
    clean_samechassis_table_task >> duplicate_basic_cleaning_task
    clean_samechassis_table_task >> duplicate_cleaning_samechassis_task

    (
        [duplicate_basic_cleaning_task, duplicate_cleaning_samechassis_task]
        >> final_union_task
        >> archive_null_log_task
    )
    archive_null_log_task >> archive_fuzzy_log_task >> null_case_task
    null_case_task >> furzzy_task >> trim_task >> booked_task
    booked_task >> policy_mapping_task >> merge_claim >> final_table

    (
        final_table
        >> anomali_task
        >> sql_task
        >> datatype_convert_task
        >> overall_churned_task
        >> renewal_rate_task
        >> pricing_catlog_task
    )
    pricing_catlog_task >> policy_feature_task >> prediction_task
    prediction_task >> [task_model_prediction, task_policy_status] >> task_top3
    task_top3 >> segmentation_task >> monitoring_task >> end
