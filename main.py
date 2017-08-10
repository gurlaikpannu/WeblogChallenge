from sys import path
path.append("lib/")

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, split, udf
import schema_builder
from user_agents import parse as parse_user_agent


def extract_browser_and_version(user_agent):
  if user_agent in {None: 1, "-": 1}:
    return

  parsed_user_agent = parse_user_agent(user_agent)

  return "{0}/{1}".format(
    parsed_user_agent.browser.family,
    parsed_user_agent.browser.version_string
  )


spark_context = SparkContext("local", "Analyze Logs")
sql_context = SQLContext(spark_context)

browser_ver_udf = udf(extract_browser_and_version, StringType())
data_frame = (
    sql_context.read
               .format("csv")
               .option("delimiter", " ")
               .schema(schema_builder.build_elb_log_schema())
               .load("data/2015_07_22_mktplace_shop_web_log_sample.log.gz")
)

# Strip out port number - Create new colum for client IP without port
data_frame = data_frame.withColumn("client_ip", split(col("client_ip_port"), ":")[0])

# Extract browser and version from user_agent and create a new column
data_frame = data_frame.withColumn("browser_ver", browser_ver_udf(col("user_agent")))

print(data_frame.show())