from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import pandas as pd
from pyarrow import parquet as pq
from io import BytesIO
from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import Variable
from airflow.exceptions import AirflowException

second_layer = Variable.get('second_layer')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 1, 1),
}

# Function to convert JSON data to Parquet and partition by country and ID
def convert_json_to_parquet():
    try:
        # Import the function from the Python file to convert JSON to Parquet

        spark = SparkSession.builder \
            .appName("BreweriesDataTransformation") \
            .getOrCreate()

        raw_brew_data_df = spark.read.json("all_breweries_data.json", multiLine = True)

        # Execute the function to convert JSON to Parquet
        convert_json_to_parquet()

    except Exception as e:
        raise AirflowException(f"Failed to convert JSON to Parquet: {str(e)}")

# Function to upload Parquet data to GCS bucket
def upload_parquet_to_gcs_bucket():
    try:
        # Convert DataFrame to Parquet format
        table = pq.read_table(raw_brew_data_df)
        
        # Write Parquet data to BytesIO buffer
        buf = BytesIO()
        pq.write_table(table, buf)
        
        # Initialize Google Cloud Storage client
        client = storage.Client()
        
        # Get the specified bucket
        bucket = client.get_bucket('breweries-list')
        
        # Create a new blob object in the bucket
        blob = bucket.blob('breweries_partioned_parquet')
        
        # Upload the Parquet data to the blob
        blob.upload_from_file(buf, content_type='application/octet-stream')
        
        print(f"Parquet data uploaded to GCS bucket: gs://{'breweries-list'}/{'breweries_partioned_parquet'}")
    except Exception as e:
        raise AirflowException(f"Failed to upload Parquet data to Google Cloud Storage: {str(e)}")

# Define the DAG
with DAG('convert_json_to_parquet_and_upload_to_gcs', 
         default_args=default_args,
         schedule_interval='@daily') as dag:

    # Task to convert JSON data to Parquet
    convert_to_parquet_task = PythonOperator(
        task_id='convert_json_to_parquet',
        python_callable=convert_json_to_parquet
    )

    # Task to upload Parquet data to GCS bucket
    upload_to_gcs_task = PythonOperator(
        task_id='upload_parquet_to_gcs_bucket',
        python_callable=upload_parquet_to_gcs_bucket
    )

    # Set task dependencies
    convert_to_parquet_task >> upload_to_gcs_task
