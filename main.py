from sys import path
path.append("lib/")

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, split, udf, coalesce, lag, lit, sum as pyspark_sum, countDistinct, desc, dense_rank
from pyspark.sql.window import Window
import schema_builder
from user_agents import parse as parse_user_agent


WINDOW_SIZE = 900  # 15 minutes


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


###### DEFINING DISTINCT USER BY CLIENT IP AND "BROWSER NAME & VERSION" ######
# Strip out port number - Create new colum for client IP without port
data_frame = data_frame.withColumn("client_ip", split(col("client_ip_port"), ":")[0])

# Extract browser and version from user_agent and create a new column
data_frame = data_frame.withColumn("browser_ver", browser_ver_udf(col("user_agent")))
##############################################################################


###### SESSIONIZE BY CLIENT IP AND WEB BROWSER (WITH VERSION) ######
window = Window.partitionBy("client_ip", "browser_ver").orderBy("timestamp")
diff = coalesce(col("timestamp").cast("float") - lag("timestamp", 1).over(window).cast("float"), lit(0))
indicator = (diff > WINDOW_SIZE).cast("integer")
session = pyspark_sum(indicator).over(window).alias("session")
data_frame = data_frame.select("*", session)
####################################################################


# Count By Sessions
print(
  "Count By Sessions are: \n",
  data_frame.groupBy("session")
            .count()
            .collect()
)


# Average Session Time
data_frame = data_frame.withColumn("marginal_session_time", diff)
print(
  "Average Session Time is: \n",
  data_frame.groupBy("session")
            .avg("marginal_session_time")
            .collect()
)


# Distinct Users by Session
print(
  "Distinct Users by Session are: \n",
  data_frame.groupBy("session")
            .agg(countDistinct("client_ip", "browser_ver"))
            .collect()
)


# Unique Users With The Longest Session Time
data_frame = (
  data_frame.groupBy("session", "client_ip", "browser_ver")
            .agg(pyspark_sum("marginal_session_time").alias("session_time_sums"))
            .alias("session_time_sums")
)
window = Window().partitionBy("session").orderBy(desc("session_time_sums"))
data_frame = data_frame.withColumn("rank", dense_rank().over(window))
print(
  "Unique Users With The Longest Session Time are: \n",
  data_frame.where(col("rank") == 1)
            .collect()
)