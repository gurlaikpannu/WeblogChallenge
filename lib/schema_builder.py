from pyspark.sql.types import StructType, StructField, TimestampType, StringType, FloatType, IntegerType


def build_elb_log_schema():
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("elb", StringType(), True),
        StructField("client_ip_port", StringType(), True),
        StructField("backend_ip_port", StringType(), True),
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

    return schema