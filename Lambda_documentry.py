import json
import boto3
import logging 
import sys

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
    print(latest["Key"])
    file_path, file_name = latest["Key"].rsplit("/", 1)
    file_path = file_path if file_path[-1] == "/" else f"{file_path}/"
    file = file_path + file_name
    return(file)
    
def lambda_handler(event, context):
    # Set up logging
    logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        ]
    )
    
    file = get_most_recent_s3_object(
        bucket_name="imax-datalake-source-inbound-dev", prefix="GBO_Documtry/"
        )
    logging.info(file)
    print(file)
    
    s3 = boto3.resource('s3')
    copy_source = {
    'Bucket': 'imax-datalake-source-inbound-dev',
    'Key': file
    }
    logging.info(copy_source)
    try:
        s3.meta.client.copy(copy_source, 'imax-datalake-snowflake-bronze-dev', file)
    except Exception as e:
        logging.error(f"Error in program! {e}")
    finally:    
        logging.info("File copied Successfully!")
