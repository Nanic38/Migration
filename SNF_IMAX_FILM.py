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
import logging  # Add this import for logging


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
    dag_id="SNF_IMAX_FILM",
    default_args=default_args,
    description="Documentary Master Airflow DAG",
    schedule_interval="@once",
    catchup=False,
)

glue_client = boto3.client('glue', region_name='us-west-2')


def run_csv2prq_imax_film_job():
    glue_client.start_job_run(JobName='csv2prq_imax_film')
    response = glue_client.get_job_runs(JobName = 'csv2prq_imax_film')
    job_run_id = response["JobRuns"][0]["Id"]
    while True:
        status = glue_client.get_job_run(JobName='csv2prq_imax_film', RunId=job_run_id)
        if status['JobRun']['JobRunState'] == 'SUCCEEDED':
            break
        elif status['JobRun']['JobRunState'] == 'FAILED':
                raise Exception('Csv2parquet Glue job failed.')
        time.sleep(10)


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

    # Create a Snowflake connection
    connection = snowflake.connector.connect(**snowflake_params)
    cursor = connection.cursor()

    # Your Snowflake SQL statement using file_path and file_name
    file_path, file_name = get_most_recent_s3_object(
        bucket_name="imax-datalake-snowflake-bronze-dev",
        prefix="Master_Enrich/Enrich_IMAX_Film/"
    )
    sql_statement = (f"CALL IMAX01_BRONZE_RAW_DEV.IMAX_MASTER.PROC_LOAD_DATA_FROM_S3_TO_BRONZE"
                     f"('imax-datalake-snowflake-bronze-dev','{file_path}','{file_name}',"
                     f"'SRC_Enrich_Documentary');")

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

def execute_sf_audit(**kwargs):
    snowflake_params = {
        "user": Variable.get("sf_user"),
        "password": Variable.get("sf_password"),
        "account": Variable.get("sf_account"),
        "role": Variable.get("sf_role"),
        "warehouse": Variable.get("sf_warehouse"),
        "database": Variable.get("sf_database_g"),
        "schema": Variable.get("sf_schema_g"),
    }

    # Create a Snowflake connection
    connection = snowflake.connector.connect(**snowflake_params)
    cursor = connection.cursor()

    # Execute the stored procedure
    sql_statement = "CALL SF_AUDIT_DEV.ETL_AUDIT.MAIN_SP(5)"
    cursor.execute(sql_statement)

    try:
        query_id = cursor.sfqid
        logging.info(query_id)
        while connection.is_still_running(connection.get_query_status_throw_if_error(query_id)):
            time.sleep(10)
    except ProgrammingError as err:
        logging.error('ERROR in Snowflake procedure {0}'.format(err))

    # Close the connection
    cursor.close()
    connection.close()

# START & END TASK
START = DummyOperator(task_id="START", default_args=default_args, dag=dag)
END = DummyOperator(task_id="END", default_args=default_args, dag=dag)

Csv2parquet = PythonOperator(
    task_id="Csv2parquet",
    python_callable=run_csv2prq_imax_film_job,
    default_args=default_args,
    dag=dag,
)

Snf_bronze_load = PythonOperator(
    task_id="Snf_bronze_load",
    python_callable=execute_snowflake_sql,
    default_args=default_args,
    dag=dag
)

Snf_gold_load = PythonOperator(
    task_id="Snf_gold_load",
    python_callable=execute_sf_audit,
    default_args=default_args,
    dag=dag
)

START >> Csv2parquet >> Snf_bronze_load >> Snf_gold_load >> END



