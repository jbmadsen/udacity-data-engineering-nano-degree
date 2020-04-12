import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (ExecuteSQLOnRedshiftOperator,
                               StageToRedshiftOperator, 
                               LoadFactOperator, 
                               LoadDimensionOperator,
                               DataQualityOperator)
from helpers import SqlQueries


# AWS Environment Variables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


# Default arguments for DAG with arguments as specified by Project Specification

default_args = {
    'owner': 'jbmadsen',
    'start_date': datetime(2019, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


# DAG object creation
# Runs every hour: https://crontab.guru/every-1-hour

dag = DAG(
    'sparkify_dag',
    default_args=default_args,
    description='Load and transform data from S3 in Redshift with Airflow',
    schedule_interval='0 * * * *', 
    catchup=True,
    max_active_runs=1
)


# Task Operators

start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
)

create_staging_tables_redshift = ExecuteSQLOnRedshiftOperator(
    task_id='Create_staging_tables',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.create_staging_tables
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    json="auto"
)

create_main_tables_redshift = ExecuteSQLOnRedshiftOperator(
    task_id='Create_main_tables',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.create_main_tables
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag, 
    provide_context=True,
    table="songplays",
    truncate_table=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag, 
    provide_context=True,
    table="users",
    truncate_table=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag, 
    provide_context=True,
    table="songs",
    truncate_table=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag, 
    provide_context=True,
    table="artists",
    truncate_table=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag, 
    provide_context=True,
    table="time",
    truncate_table=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    tables=[
               { "table": "songplays", "key": "songplay_id" },
               { "table": "users", "key": "user_id" },
               { "table": "songs", "key": "song_id" },
               { "table": "artists", "key": "artist_id" },
               { "table": "time", "key": "start_time" },
           ]
)

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)


# Task Dependencies

start_operator >> create_staging_tables_redshift

create_staging_tables_redshift >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> create_main_tables_redshift

create_main_tables_redshift >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, 
                         load_song_dimension_table, 
                         load_artist_dimension_table,
                         load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator

# End
