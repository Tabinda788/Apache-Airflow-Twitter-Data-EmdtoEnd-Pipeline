from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
import psycopg2
import psycopg2.extras



with DAG("ex_task_group", default_args= {
    "owner": "airflow", "start_date" : datetime(2022,1,1)
}) as dag:
    start = DummyOperator(task_id="START")
    # a=  DummyOperator(task_id="Task_A")
    # a1= DummyOperator(task_id="Task_A1")
    # b = DummyOperator(task_id="Task_B")
    # c = DummyOperator(task_id="Task_C")
    # d = DummyOperator(task_id="Task_D")
    # e = DummyOperator(task_id="Task_E")
    # f = DummyOperator(task_id="Task_F")
    # g = DummyOperator(task_id="Task_G")
    end = DummyOperator(task_id="end")
    
    with TaskGroup("A-A1", tooltip ="Task Group for A and A1") as gr_1:
        a= DummyOperator(task_id="Task_A")
        a1=DummyOperator(task_id="Task_A1")
        b=DummyOperator(task_id="Task_B")
        c= DummyOperator(task_id="Task_C")
        a >> a1
        
    with TaskGroup("D-E-F-G", tooltip="Nested Task Group") as gr_2:
        d= DummyOperator(task_id="Task_D")
        with TaskGroup("E-F-G",tooltip="Inner nested task group") as sgr_2:
            e =DummyOperator(task_id="Task_E")
            f =DummyOperator(task_id="Task_F")
            g=DummyOperator(task_id="Task_G")
            
            e >> f
            e >> g
    
# start >> a >> a1 >> b >> c >> d >>e >>f >> g >> end

# start >> gr_1 >> d >> e >> f >> g >> end

# task a and a1 will be processed in parlled with task b and c
start >> gr_1 >> gr_2 >> end