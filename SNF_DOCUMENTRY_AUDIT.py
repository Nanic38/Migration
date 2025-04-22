from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import snowflake.connector
import time

# Define default arguments for the DAG
default_args = {
    "owner": "IMAX",
    "start_date": datetime(2023, 10, 29),
}

# Initialize the new DAG
dag = DAG(
    dag_id="SNF_DOCUMENTRY_AUDIT",
    default_args=default_args,
    description="Snowflake Audit DAG",
    schedule_interval="@once",
    catchup=False,
)

# Define a Python function to execute the Snowflake stored procedure
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
    sql_statement = "Call sf_audit_dev.etl_audit.main_sp(13)"
    cursor.execute(sql_statement)
    
    try:
        query_id = cursor.sfqid
        logging.info(query_id)
    
        # Wait for the procedure to complete
        while connection.is_still_running(connection.get_query_status_throw_if_error(query_id)):
            time.sleep(10)

        # Check if the procedure was successful
        query_status = connection.get_query_status(query_id)
        if query_status == 'SUCCESS':
            logging.info('Snowflake procedure executed successfully.')
        else:
            logging.error(f'ERROR in Snowflake procedure. Status: {query_status}')

    except ProgrammingError as err:
        logging.error('ERROR in Snowflake procedure {0}'.format(err))

    # Close the connection
    cursor.close()
    connection.close()
# START & END TASK
START = DummyOperator(task_id="START", default_args=default_args, dag=dag)
END = DummyOperator(task_id="END", default_args=default_args, dag=dag)

# Define a PythonOperator for executing the stored procedure in the new DAG
Snf_gold_load = PythonOperator(
    task_id="Snf_gold_load",
    python_callable=execute_sf_audit,
    default_args=default_args,
    dag=dag
)

START >> Snf_gold_load >> END

