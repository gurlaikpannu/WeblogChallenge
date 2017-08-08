from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, FloatType, IntegerType


spark_context = SparkContext("local", "Analyze Logs")
sql_context = SQLContext(spark_context)
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("elb", StringType(), True),
    StructField("client_port", StringType(), True),
    StructField("backend_port", StringType(), True),
    StructField("request_processing_time", FloatType(), True),
    StructField("backend_processing_time", FloatType(), True),
    StructField("response_processing_time", FloatType(), True),
    StructField("elb_status_code", IntegerType(), True),
    StructField("backend_status_code", IntegerType(), True),
    StructField("received_bytes", IntegerType(), True),
    StructField("sent_bytes", IntegerType(), True),
    StructField("request", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("ssl_cipher", StringType(), True),
    StructField("ssl_protocol", StringType(), True)
])

data_frame = sql_context.read.format("com.databricks.spark.csv").option("delimiter", " ").schema(schema).load("data/2015_07_22_mktplace_shop_web_log_sample.log.gz")

print(data_frame.show())