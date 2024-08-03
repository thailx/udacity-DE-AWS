import pendulum
from datetime import timedelta

from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from final_project_operators.redshift_custom_operator import PostgreSQLOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'Phoebe',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# Define the DAG with the appropriate schedule and description
@dag(
    default_args=default_args,
    description='A workflow to create tables in Redshift using Airflow',
    schedule_interval='0 * * * *'
)
def create_redshift_tables_dag():
    start_task = DummyOperator(task_id='Begin_execution')

    create_tables_task = PostgreSQLOperator(
        task_id='Create_tables',
        postgres_conn_id='redshift',
        sql='create_tables.sql'
    )

    end_task = DummyOperator(task_id='Stop_execution')

    # Set task dependencies
    start_task >> create_tables_task >> end_task

# Instantiate the DAG
dag_instance = create_redshift_tables_dag()
