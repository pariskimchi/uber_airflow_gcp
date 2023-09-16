from datetime import datetime , timedelta

import numpy as np
import pandas as pd 
import os 
import json 
import csv 
import io

from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from google.cloud import bigquery, storage
from airflow.operators.python import PythonOperator
from airflow.decorators import task 

import logging


GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "swift-impulse-338203"
OBJECT_NAME = "uber_data.csv"

BUCKET_NAME = "uber-analysis-bucket"
# STAGING_DATASET = "uber_staging_dataset"
DATASET = "uber_dataset"
LOCATION = "us-central1"
gcs_credentials_file_path = "/opt/airflow/dags/swift-impulse-338203-ed5735a72d3c.json"


default_args = {
    'owner':'haneul',
    'depends_on_past':False,
    'retries':1, 
    'start_date': days_ago(2), 
    'retry_delay': timedelta(minutes=5), 
    'provide_context':True
}


# Extract 
# @task()
def extract(**context):
    """
    
    """
    # Create Storage instance
    storage_client = storage.Client.from_service_account_json(gcs_credentials_file_path)
    
    bucket = storage_client.get_bucket(BUCKET_NAME)
    blob = bucket.blob(OBJECT_NAME)
    
    ### download the file as a string 
    file_content = blob.download_as_text()
    
    df = pd.read_csv(io.StringIO(file_content), sep=',')
    
    df.to_csv(f'./{OBJECT_NAME}', index=False)
    
    dest_obj_path = os.path.join(os.getcwd(), f"{OBJECT_NAME}")
    
    
    context['ti'].xcom_push(key='dest_obj_path', value=dest_obj_path)
    
    return dest_obj_path

# @task()
def transform(**context):
    
        # logging.info(f"input_df shape: {df.shape}")
        
        source_csv_path = context['ti'].xcom_pull(key=f'dest_obj_path')

        # df = context.get('ti').xcom_pull(key='df')
        df = pd.read_csv(source_csv_path)
        # Specify your transformation logic here
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
        datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
        datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
        datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
        datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
        datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

        datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
        datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
        datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
        datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
        datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

        datetime_dim['datetime_id'] = datetime_dim.index
        datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                                'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

        passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
        passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
        passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]

        trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
        trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
        trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]
        rate_code_type = {
            1:"Standard rate",
            2:"JFK",
            3:"Newark",
            4:"Nassau or Westchester",
            5:"Negotiated fare",
            6:"Group ride"
        }

        rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
        rate_code_dim['rate_code_id'] = rate_code_dim.index
        rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
        rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]


        pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=True)
        pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
        pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']] 


        dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
        dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
        dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]

        payment_type_name = {
            1:"Credit card",
            2:"Cash",
            3:"No charge",
            4:"Dispute",
            5:"Unknown",
            6:"Voided trip"
        }
        payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
        payment_type_dim['payment_type_id'] = payment_type_dim.index
        payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
        payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]

        fact_table = df.merge(passenger_count_dim, on='passenger_count') \
                .merge(trip_distance_dim, on='trip_distance') \
                .merge(rate_code_dim, on='RatecodeID') \
                .merge(pickup_location_dim, on=['pickup_longitude', 'pickup_latitude']) \
                .merge(dropoff_location_dim, on=['dropoff_longitude', 'dropoff_latitude'])\
                .merge(datetime_dim, on=['tpep_pickup_datetime','tpep_dropoff_datetime']) \
                .merge(payment_type_dim, on='payment_type') \
                [['VendorID', 'datetime_id', 'passenger_count_id',
                'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
                'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                'improvement_surcharge', 'total_amount']]

        
        result_df_dict =  {"datetime_dim":datetime_dim,
        "passenger_count_dim":passenger_count_dim,
        "trip_distance_dim":trip_distance_dim,
        "rate_code_dim":rate_code_dim,
        "pickup_location_dim":pickup_location_dim,
        "dropoff_location_dim":dropoff_location_dim,
        "payment_type_dim":payment_type_dim,
        "fact_table":fact_table}
        
        ## key= table_name,value = 
        result_df_path_dict = dict()
        curr_dir = os.getcwd()
        for table_name, dict_df in result_df_dict.items():
            # to_Csv 
            dict_df.to_csv(f'{table_name}.csv', index=False)
            result_df_path_dict[f'{table_name}'] = os.path.join(curr_dir, f'{table_name}.csv')
        
        context['ti'].xcom_push(key='result_df_dict', value=result_df_path_dict)

        
# @task()        
def load(**context):
        
        # 
        result_df_path_dict = context['ti'].xcom_pull(key='result_df_dict')
        
        # Create bigquery instance 
        bigquery_client = bigquery.Client.from_service_account_json(gcs_credentials_file_path)
        # bigquery_client
        dataset_ref = DATASET
        
        # check bq dataset list 
        dataset_list = []
        for bq_dataset_obj in bigquery_client.list_datasets():
            dataset_list.append(bq_dataset_obj)
        
        # dataset_check 
        if dataset_ref in dataset_list:
            bigquery_dataset = bigquery_client.get_dataset(f"{PROJECT_ID}.{DATASET}")
        else:
            new_bq_dataset= bigquery.Dataset(f"{PROJECT_ID}.{DATASET}")
            new_bq_dataset.location = 'US'
            
            bigquery_dataset = bigquery_client.create_dataset(new_bq_dataset)
            print(f"CREATE DATASET {bigquery_client.project}.{bigquery_dataset.dataset_id}")
        
        #### load_from_dataframe loop 
        for table_name, df_csv_path in result_df_path_dict.items():
            logging.info(f"processing {table_name} into bigquery Table")
            table_ref = bigquery_dataset.table(table_name)
            
            job = bigquery_client.load_table_from_dataframe(pd.read_csv(df_csv_path),
                                                        table_ref, 
                                                        location='US')
            job.result()
            logging.info(f"Loaded DataFrame into {table_ref.path}")
        
        
        logging.info(" DONE ")

with DAG('uber_etl_operator_dag', schedule_interval=timedelta(days=1),
         default_args=default_args) as dag:
    
    
    extract_data_dag= PythonOperator(
        task_id='extract_from_gcs',
        dag=dag,
        python_callable=extract, 
        provide_context=True
    )
    
    transform_data_dag = PythonOperator(
        task_id='transform_data',
        dag=dag, 
        python_callable=transform, 
        provide_context=True
    )
    
    load_data_dag = PythonOperator(
        task_id='Load_data_to_bq',
        dag=dag, 
        python_callable=load,
        provide_context=True
    )
    
extract_data_dag >> transform_data_dag >> load_data_dag