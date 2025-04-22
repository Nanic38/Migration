from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
import boto3
import json
import sys
import time
from time import sleep
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
import snowflake.connector
import logging  

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)

default_args = {
    "owner": "IMAX",
    "start_date": datetime(2023, 10, 29),
}

# Initialize the DAG
dag = DAG(
    dag_id="SNF_DOCUMENTRY",
    default_args=default_args,
    description="Documentary Master Airflow DAG",
    schedule_interval="@once",
    catchup=False,
)

def get_most_recent_s3_object(bucket_name, prefix, **kwargs):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest2 = max(page["Contents"], key=lambda x: x["LastModified"])
            if latest is None or latest2["LastModified"] > latest["LastModified"]:
                latest = latest2
    logging.info(latest["Key"])
    file_path, file_name = latest["Key"].rsplit("/", 1)
    file_path = file_path if file_path[-1] == "/" else f"{file_path}/"
    return file_path, file_name

def execute_snowflake_sql(**kwargs):
    #get Snowflake variables
    snowflake_params = {
        "user": Variable.get("sf_user"),
        "password": Variable.get("sf_password"),
        "account": Variable.get("sf_account"),
        "warehouse": Variable.get("sf_warehouse"),
        "database": Variable.get("sf_database"),
        "schema": Variable.get("sf_schema"),
    }
    #print(sf_user)
    
    # Create a Snowflake connection
    connection = snowflake.connector.connect(**snowflake_params)
    cursor = connection.cursor()

    # Your Snowflake SQL statement using file_path and file_name
    file_path, file_name = get_most_recent_s3_object(
        bucket_name="imax-datalake-snowflake-bronze-dev",
        prefix="GBO_Documtry/"
    )
    sql_statement = (f"CALL IMAX01_BRONZE_RAW_DEV.IMAX_GBO.PROC_LOAD_DATA_FROM_S3_TO_BRONZE_CSV"
                     f"('imax-datalake-snowflake-bronze-dev','{file_path}','{file_name}',"
                     f"'SRC_DOCUMNTRY_DAILY_TRANS');")
    


    # Execute the SQL statement
    cursor.execute(sql_statement)

    try:
        query_id = cursor.sfqid
        logging.info(query_id)
    
        # Wait for the procedure to complete
        while connection.is_still_running(connection.get_query_status_throw_if_error(query_id)):
            time.sleep(10)

        # Check if the procedure was successful
        query_status = connection.get_query_status(query_id)
        if connection.is_an_error(query_status):
            logging.error(f'ERROR in Snowflake procedure. Status: {query_status}')
        else:
             logging.info('Snowflake procedure executed successfully.')

    except ProgrammingError as err:
        logging.error('ERROR in Snowflake procedure {0}'.format(err))
 
    # Close the connection
    cursor.close()
    connection.close()


# START & END TASK
START = DummyOperator(task_id="START", default_args=default_args, dag=dag)
END = DummyOperator(task_id="END", default_args=default_args, dag=dag)



Snf_bronze_load = PythonOperator(
    task_id="Snf_bronze_load",
    python_callable=execute_snowflake_sql,
    default_args=default_args,
    dag=dag
)


START >> Snf_bronze_load >> END