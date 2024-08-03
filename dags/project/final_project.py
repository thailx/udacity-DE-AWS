from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from udacity.common.final_project_sql_statements import SqlQueries

# Define default arguments for the DAG
default_args = {
    'owner': 'Phoebe',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# Define the DAG with appropriate schedule and description
@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    end_date=datetime(2018, 11, 2),
    schedule_interval='0 * * * *'
)
def final_project():

    begin_execution = DummyOperator(task_id='Begin_execution')

    stage_events_task = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='dend-phoebe',
        s3_key='log-data',
        log_json_file='log_json_path.json'
    )

    stage_songs_task = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='dend-phoebe',
        s3_key='song-data/A/A/'
    )

    load_fact_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dim_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert,
        mode='truncate-insert'
    )

    load_song_dim_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql_query=SqlQueries.song_table_insert,
        mode='truncate-insert'
    )

    load_artist_dim_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql_query=SqlQueries.artist_table_insert,
        mode='truncate-insert'
    )

    load_time_dim_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert,
        mode='truncate-insert'
    )

    data_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    end_execution = DummyOperator(task_id='Stop_execution')

    begin_execution >> [stage_events_task, stage_songs_task] >> load_fact_table >> \
    [load_user_dim_table, load_song_dim_table, load_artist_dim_table, load_time_dim_table] >> \
    data_quality_checks >> end_execution

final_project_dag = final_project()
