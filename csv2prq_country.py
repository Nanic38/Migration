import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from datetime import datetime
from pyspark.sql.functions import input_file_name
from pyspark.sql import functions as F  # Add this import

# Create a GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())

# Initialize Spark session
spark = glueContext.spark_session

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_SOURCE_PATH', 'S3_OUTPUT_PATH'])

# Extract parameters
source_path = args['S3_SOURCE_PATH']
output_path = args['S3_OUTPUT_PATH']

# Read CSV data from source path
try:
    source_data = spark.read.csv(source_path, header=True, inferSchema=True)
    logging.info ("Data read successfully from source path")
except Exception as e:
    logging.error(f"Error reading data from source path: {e}")
    sys.exit(1)    

# Add a new column with the current date in the filename
current_date_str = datetime.now().strftime('%Y%m%d')

# Extract the source file name from the input_file_name
source_data_with_filename = source_data.withColumn("source_filename", input_file_name())

# Extract the base name of the source file without the extension
source_data_with_filename = source_data_with_filename.withColumn("source_filename", F.expr("substring_index(substring_index(source_filename, '/', -1), '.', 1)"))

# Create the output file name
source_data_with_filename = source_data_with_filename.withColumn("output_filename", F.concat(F.col("source_filename"), F.lit("_"), F.lit(current_date_str), F.lit(".parquet")))

# Write the data to a single Parquet file with the new filename
source_data_with_filename.coalesce(1).write.parquet(output_path + "/" + current_date_str, mode='overwrite')
