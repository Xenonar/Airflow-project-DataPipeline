from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator

from helpers import SqlQueries
from helpers import CreateTable


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs= 1,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_songs_tables = PostgresOperator(
    task_id='create_songs',
    dag=dag,
    sql=CreateTable.create_songs,
    postgres_conn_id="redshift"
)
create_artists_tables = PostgresOperator(
    task_id='create_artists',
    dag=dag,
    sql=CreateTable.create_artists,
    postgres_conn_id="redshift"
)
create_users_tables = PostgresOperator(
    task_id='create_users',
    dag=dag,
    sql=CreateTable.create_users,
    postgres_conn_id="redshift"
)
create_time_tables = PostgresOperator(
    task_id='create_time',
    dag=dag,
    sql=CreateTable.create_time,
    postgres_conn_id="redshift"
)
create_stage_events_tables = PostgresOperator(
    task_id='create_stage_events',
    dag=dag,
    sql=CreateTable.create_stage_events,
    postgres_conn_id="redshift"
)
create_stage_songs_tables = PostgresOperator(
    task_id='create_stage_songs',
    dag=dag,
    sql=CreateTable.create_stage_songs,
    postgres_conn_id="redshift"
)
create_songplays_tables = PostgresOperator(
    task_id='create_songplays',
    dag=dag,
    sql=CreateTable.create_songplays,
    postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    JSONPaths="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    JSONPaths="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert,
    table='songplays',
    append_data=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.user_table_insert,
    table='users',
    append_data=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.song_table_insert,
    table='songs',
    append_data=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.artist_table_insert,
    table='artists',
    append_data=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.time_table_insert,
    table='time',
    append_data=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table=['songplays','songs','artists','users','time','staging_songs','staging_events'],
    val=[6820,14896,10025,104,6820,14896,8056],
 )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

[create_stage_songs_tables,create_stage_events_tables]>>create_songplays_tables
create_songplays_tables>>[create_users_tables,create_time_tables]
create_songplays_tables>>[create_songs_tables,create_artists_tables]
[create_songs_tables,create_artists_tables]>>start_operator
[create_users_tables,create_time_tables]>>start_operator

start_operator>>stage_events_to_redshift
start_operator>>stage_songs_to_redshift

stage_events_to_redshift>>load_songplays_table
stage_songs_to_redshift>>load_songplays_table

load_songplays_table>>load_user_dimension_table
load_songplays_table>>load_song_dimension_table
load_songplays_table>>load_artist_dimension_table
load_songplays_table>>load_time_dimension_table

load_user_dimension_table>>run_quality_checks
load_song_dimension_table>>run_quality_checks
load_artist_dimension_table>>run_quality_checks
load_time_dimension_table>>run_quality_checks

run_quality_checks>>end_operator

