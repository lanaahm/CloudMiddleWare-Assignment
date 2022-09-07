from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from extract import main as main_extract
from utils.init_table import main as init_transfrom
from transform import transfrom_customer
from transform import transfrom_product
from transform import transfrom_transaction
from load import main as loads
# [START]
with DAG(dag_id="ETL-Airflow", start_date=days_ago(2), tags=["ETL"]) as dag:
    start = DummyOperator(task_id="start")

    # [START task_extract]
    with TaskGroup("extract", tooltip="Tasks for extract") as extract_process:
        task_1 = PythonOperator(task_id="extract_zipfile", python_callable=main_extract)
        task_1
    # [END task_extract]

    # [START task_transfrom]
    with TaskGroup("transfrom", tooltip="Tasks for transfrom") as transfrom:
        task_0 = PythonOperator(task_id="init_transfrom", python_callable=init_transfrom)
        task_1 = PythonOperator(task_id="transfrom_customer", python_callable=transfrom_customer)
        task_2 = PythonOperator(task_id="transfrom_product", python_callable=transfrom_product)
        task_3 = PythonOperator(task_id="transfrom_transaction", python_callable=transfrom_transaction)
        task_0 >> [task_1, task_2, task_3]
    # [END task_transfrom]

    # [START task_extract]
    with TaskGroup("load", tooltip="Tasks for load") as load_process:
        task_1 = PythonOperator(task_id="load_allDataTransfrom_toCleanTables", python_callable=loads)
        task_1
    # [END task_extract]

    end = DummyOperator(task_id='end')
    start >> extract_process >> transfrom >> load_process >> end
# [END]