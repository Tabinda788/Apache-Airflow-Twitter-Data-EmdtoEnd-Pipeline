from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
import psycopg2
import psycopg2.extras
from twitter_etl import run_twitter_etl, upload_to_s3, delete_from_s3



default_args = {
    'owner': 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2021,10,6),
    'email' : ['tabu@example.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=10) 
}


with DAG(
    default_args=default_args,
    dag_id='complete_twitter_etl',
    description='My first etl code for airflow twitter',
    start_date=datetime(2021, 10, 6),
    schedule_interval='@daily',
   
) as dag:
    run_etl = PythonOperator(
        task_id='run_twitter_etl',
        python_callable=run_twitter_etl
        
    )
    
    create_table_in_postgres = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists twitter_table_data1 (
                index TEXT,
                tweets TEXT,
                created_at date,
                reply_count int,
                retweet_count int,
                like_count int,
                quote_count int,
                domain_names TEXT,
                entity_names TEXT
            )
        """
    )
    
    
    load_data_to_postgresql = BashOperator(
        task_id="load_data_to_postgresql",
        bash_command="python /opt/airflow/dags/load_data.py "
    )
    
    delete_data = PostgresOperator(
        task_id='delete_data_from_table',
        postgres_conn_id='postgres_localhost', 
        sql="""
            delete from twitter_table_data1;
        """
    )
    
    task_delete_from_s3 = PythonOperator(
        task_id='delete_from_s3',
        python_callable=delete_from_s3,
        op_kwargs={
            'keys': 'elonmusk_tweets.csv',
            'bucket_name': 'tabinda-airflow-bucket'
        }
    )
    
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/opt/airflow/dags/elonmusk_tweets.csv',
            'key': 'elonmusk_tweets.csv',
            'bucket_name': 'tabinda-airflow-bucket'
        }
    )
    
#    load_to_s3  
run_etl  >> create_table_in_postgres >> delete_data >> load_data_to_postgresql >> task_delete_from_s3 >>task_upload_to_s3