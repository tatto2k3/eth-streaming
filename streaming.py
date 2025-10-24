from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, expr
from pyspark.sql.types import *

spark = (SparkSession.builder
         .appName("ETH_IForest_Serve_Spark")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
         .getOrCreate())

schema = StructType([
    StructField("blockNumber", LongType()),
    StructField("timestamp", LongType()),
    StructField("miner", StringType()),
    StructField("tx_count", IntegerType()),
    StructField("hash", StringType()),
    StructField("from", StringType()),
    StructField("to", StringType()),
    StructField("value_wei", LongType()),
    StructField("gas", LongType()),
    StructField("gasPrice_wei", LongType()),
    StructField("nonce", LongType()),
    StructField("input_len", IntegerType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "eth.tx")
       .option("startingOffsets", "latest")
       .option("maxOffsetsPerTrigger", 500)
       .option("failOnDataLoss", "false")
       .load())

df = (raw.selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).alias("r"))
      .select("r.*")
      .dropDuplicates(["hash"])
      .withColumn("event_time", to_timestamp(col("timestamp")))  # UNIX seconds
      .withColumn("value_eth", col("value_wei") / expr("1e18"))
      .withColumn("gasPrice_gwei", col("gasPrice_wei") / expr("1e9")))

# In ra console (debug)
console_query = (df.writeStream
                 .format("console")
                 .option("truncate", False)
                 .option("numRows", 5)
                 .start())

# Ghi CSV mỗi 30s
csv_query = (df.writeStream
             .format("csv")
             .option("path", "./output/eth-streaming-csv")
             .option("checkpointLocation", "./checkpoint/eth-csv")
             .option("header", True)
             .trigger(processingTime='30 seconds')
             .start())

console_query.awaitTermination()


# from predict_udf import anomaly_udf
# from pyspark.sql import functions as F

# feature_cols = ["value_eth", "gas", "gasPrice_gwei", "nonce", "input_len"]
# df_pred = df.withColumn("recon_error", anomaly_udf(F.array(*feature_cols)))

# threshold = 0.02
# df_alert = df_pred.withColumn("is_anomaly", (F.col("recon_error")[0] > threshold))

# (df_alert
#  .filter("is_anomaly = true")
#  .selectExpr("to_json(struct(*)) AS value")
#  .writeStream
#  .format("kafka")
#  .option("kafka.bootstrap.servers", "localhost:9092")
#  .option("topic", "eth.alerts")
#  .option("checkpointLocation", "./checkpoint/eth-alerts")
#  .start())
