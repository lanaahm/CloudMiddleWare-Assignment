from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from extract import main as MainExtract

# [START]
with DAG(dag_id="ETL-Airflow", start_date=days_ago(2), tags=["ETL"]) as dag:
    start = DummyOperator(task_id="start")

    # [START task_group_extract]
    with TaskGroup("extract", tooltip="Tasks for extract") as extract_process:
        task_1 = PythonOperator(task_id="extract_zipfile", python_callable=MainExtract)
        task_1
    # [END task_group_extract]

    # [START task_group_transfrom]
    with TaskGroup("transfrom", tooltip="Tasks for transfrom") as transfrom:
        task_1 = DummyOperator(task_id="task_1")

        # [START task_group_inner_transfrom]
        with TaskGroup("inner_transfrom", tooltip="Tasks for inner_section2") as inner_transfrom:
            task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
            task_3 = DummyOperator(task_id="task_3")
            task_4 = DummyOperator(task_id="task_4")

            [task_2, task_3] >> task_4
        # [END task_group_inner_transfrom]

    # [END task_group_transfrom]
    
    end = DummyOperator(task_id='end')
    
    start >> extract_process >> transfrom >> end
# [END]