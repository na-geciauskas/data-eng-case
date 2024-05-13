from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import Variable
from airflow.exceptions import AirflowException

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 1, 1),
}

first_layer = Variable.get('first_layer')

def extract_data_from_api():
    # Import your Python file to extract data from the API endpoint
    from first_layer import fetch_breweries_data
    
    # Execute the function to extract data
    raw_data = fetch_breweries_data(200)

    return raw_data

def save_data_to_json(raw_data, object_name): 
    # Return the extracted data
    return data

def upload_to_gcs_bucket(data, bucket_name, object_name):
    try:
        hook = GoogleCloudStorageHook()
        hook.upload(bucket_name=bucket_name, object_name=object_name, data=data)
    except Exception as e:
        raise AirflowException(f"Failed to upload data to Google Cloud Storage: {str(e)}")

with DAG('extract_api_data_and_upload_to_gcs', 
         default_args=default_args,
         schedule_interval='@daily') as dag:

    extract_data_task = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=extract_data_from_api
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs_bucket',
        python_callable=upload_to_gcs_bucket,
        op_kwargs={
            'bucket_name': 'breweries-list',
            'object_name': 'all_breweries_data.json'
        },
        provide_context=True
    )

    extract_data_task >> upload_to_gcs_task
