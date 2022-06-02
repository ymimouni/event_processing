from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType


# Data files folder.
DATA_FOLDER = "data/"

# Data files.
ORDERS_DATA = "orders.csv"
POLLING_DATA = "polling.csv"
CONNECTIVITY_STATUS_DATA = "connectivity_status.csv"

# Analysis results folder path.
ANALYSIS_RESULTS_FOLDER = "analysis_results/"

# Analysis results file.
ANALYSIS_RESULTS_FILE = "analysis_data.csv"

# Application name.
APP_NAME = "EventProcessing"

# Define the schema, corresponding to a line in the csv data file for Orders.
orders_schema = StructType([
    StructField('row_num', IntegerType(), nullable=True),
    StructField('order_creation_time', TimestampType(), nullable=True),
    StructField('order_id', IntegerType(), nullable=True),
    StructField('device_id', StringType(), nullable=True)])

# Define the schema, corresponding to a line in the csv data file for Polling Events.
polling_schema = StructType([
    StructField('row_num', IntegerType(), nullable=True),
    StructField('polling_creation_time', TimestampType(), nullable=True),
    StructField('device_id', StringType(), nullable=True),
    StructField('error_code', StringType(), nullable=True),
    StructField('status_code', IntegerType(), nullable=True)])

# Define the schema, corresponding to a line in the csv data file for Connectivity Status.
connectivity_status_schema = StructType([
    StructField('row_num', IntegerType(), nullable=True),
    StructField('cs_creation_time', TimestampType(), nullable=True),
    StructField('status', StringType(), nullable=True),
    StructField('device_id', StringType(), nullable=True)])


def get_spark_session():
    """
    Gets an existing :class:`SparkSession` or, if there is no existing one, creates a new one with the name APP_NAME.
    """
    return SparkSession.builder.appName(APP_NAME).getOrCreate()
