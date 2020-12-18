#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This file contains an AirFlow DAG to load data from an S3 bucket into Redshift tables. The steps taken are as follows:

  1. Copy the data from the S3 bucket to 2 staging tables on Redshift.
  2. Copy the facts to a table on Redshift.
  3. Copy the dimensions to 4 tables on Redshift.
  4. Check the quality of the data copied.

The configurations are loaded from `dp.cfg`, where the S3 bucket name and folders are stored.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
    LoadBillboardOperator,
    LoadSpotifyOperator,
    LoadGeniusOperator,
)
from helpers import SqlQueries
import configparser

CFG_FILE = '/home/miguel/udacity/project_final/aws.cfg'
config = configparser.ConfigParser()
config.read_file(open(CFG_FILE))

default_args = {
    'owner': 'Miguel F.',
    'start_date': datetime(2006, 1, 1),
    """
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    """
    'email_on_failure': False,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    'udac_music_trend_analysis',
    default_args=default_args,
    description='Load data from Billboard and Spotify into Redshift tables',
    #schedule_interval=None,
    schedule_interval='@yearly'
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

create_tables_on_redshift = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    postgres_conn_id='redshift_credentials',
    sql=SqlQueries.create_tables
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift_credentials',
    table='staging_songs',
    columns='song_id, artist_id, artist_latitude, artist_longitude, artist_location, title, year, duration, artist_name',
    s3_bucket=config.get('S3','bucket'),
    s3_key=config.get('S3','song_folder'),
    json_path=config.get('S3','song_jsonpath'),
    delete_before_insert=False,#TODO True
    skip=True#TODO False
)

stage_chart_to_redshift = LoadBillboardOperator(
    task_id='Stage_chart',
    dag=dag,
    redshift_conn_id='redshift_credentials',
    to_table='staging_charts',
    delete_before_insert=False,#TODO True
    chart_name=config.get('BILLBOARD','chart_name'),
    provide_context=True,
    skip=True#TODO False
)

stage_features_to_redshift = LoadSpotifyOperator(
    task_id='Stage_features',
    dag=dag,
    redshift_conn_id='redshift_credentials',
    to_table='staging_features',
    delete_before_insert=False,
    chart_name=config.get('BILLBOARD','chart_name'),
    provide_context=True,
    spotify_client_id=config.get('SPOTIFY','client_id'),
    spotify_client_secret=config.get('SPOTIFY','client_secret'),
    skip=True#TODO False
)

stage_lyrics_to_redshift = LoadGeniusOperator(
    task_id='Stage_lyrics',
    dag=dag,
    redshift_conn_id='redshift_credentials',
    to_table='staging_lyrics',
    delete_before_insert=False,
    chart_name=config.get('BILLBOARD','chart_name'),
    provide_context=True,
    genius_access_token=config.get('GENIUS','client_token'),
    skip=False,
    most_common_count=5
)

start_operator >> create_tables_on_redshift
create_tables_on_redshift >> stage_chart_to_redshift
create_tables_on_redshift >> stage_songs_to_redshift
stage_chart_to_redshift >> stage_features_to_redshift
stage_chart_to_redshift >> stage_lyrics_to_redshift

"""
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift_credentials',
    to_table="songplays",
    sql_select=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    delete_before_insert=True,
    redshift_conn_id='redshift_credentials',
    to_table="users",
    sql_select=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    delete_before_insert=True,
    redshift_conn_id='redshift_credentials',
    to_table="songs",
    sql_select=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    delete_before_insert=True,
    redshift_conn_id='redshift_credentials',
    to_table="artists",
    sql_select=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    delete_before_insert=True,
    redshift_conn_id='redshift_credentials',
    to_table="time",
    sql_select=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift_credentials',
    sql_queries=SqlQueries.select_nulls_count,
    expected_values=[ [(0,)] for x in SqlQueries.select_nulls_count ]
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)
"""
