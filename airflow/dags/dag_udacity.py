from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 6, 23),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(
    'dag_udacity',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@once'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_source_path='s3://udacity-dend/log_data/',
    schema='public',
    table='staging_events',
    copy_format="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'",
    truncate_table=1,
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_source_path='s3://udacity-dend/song_data/',
    schema='public',
    table='staging_songs',
    copy_format="FORMAT AS JSON 'auto' COMPUPDATE OFF REGION 'us-west-2';",
    truncate_table=1,
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    schema='public',
    table='songplays',
    query=SqlQueries.songplay_table_insert,
    truncate_table=1,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    schema='public',
    table='users',
    query=SqlQueries.user_table_insert,
    truncate_table=1,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    schema='public',
    table='songs',
    query=SqlQueries.song_table_insert,
    truncate_table=1,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    schema='public',
    table='artists',
    query=SqlQueries.artist_table_insert,
    truncate_table=1,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    schema='public',
    table='time',
    query=SqlQueries.time_table_insert,
    truncate_table=1,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    tables=['public.users', 'public.songs', 'public.artists', 'public.time'],
    rows_to_be_valid=10,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator