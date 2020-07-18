from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.subdag_operator import SubDagOperator
from subdag import load_dim_to_redshift_dag
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry': False,
}

dag_name = 'datapipelin_dag'
dag = DAG(dag_name,
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@hourly'
        )

start_operator = PostgresOperator(
    task_id = "Begin_execution_create_tables",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = '../create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_events",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    json_path = "s3://udacity-dend/log_json_path.json",
    dag = dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id ='Stage_songs',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_songs",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    dag = dag
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "songplays",
    select_query = SqlQueries.songplay_table_insert,
)

user_task_id = "Load_user_dim_table"
load_user_dimension_table = SubDagOperator(
    subdag = load_dim_to_redshift_dag(
        parent_dag_name = dag_name,
        task_id = user_task_id,
        redshift_conn_id = "redshift",
        table = "users",
        truncate_table = True,
        sql_query = SqlQueries.user_table_insert,
        start_date = datetime(2019, 1, 12)
    ),
    task_id = user_task_id,
    dag = dag
)

song_task_id = "Load_song_dim_table"
load_song_dimension_table = SubDagOperator(
    subdag = load_dim_to_redshift_dag(
        parent_dag_name = dag_name,
        task_id = song_task_id,
        redshift_conn_id = "redshift",
        table = "songs",
        truncate_table = True,
        sql_query = SqlQueries.song_table_insert,
        start_date = datetime(2019, 1, 12)
    ),
    task_id = song_task_id,
    dag = dag
)


artists_task_id = 'Load_artist_dim_table'
load_artist_dimension_table = SubDagOperator(
    subdag = load_dim_to_redshift_dag(
        parent_dag_name = dag_name,
        task_id = artists_task_id,
        redshift_conn_id = "redshift",
        table = "artists",
        truncate_table = True,
        sql_query = SqlQueries.artist_table_insert,
        start_date = datetime(2019, 1, 12)
    ),
    task_id = artists_task_id,
    dag = dag,
)

time_task_id = "Load_time_dim_table"
load_time_dimension_table = SubDagOperator(
    subdag = load_dim_to_redshift_dag(
        parent_dag_name = dag_name,
        task_id = time_task_id,
        redshift_conn_id = "redshift",
        table = "time",
        truncate_table = True,
        sql_query = SqlQueries.time_table_insert,
        start_date = datetime(2019, 1, 12)
    ),
    task_id = time_task_id,
    dag = dag,
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    tables = [
        'songplays',
        'users',
        'songs',
        'artists',
        'time'
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator