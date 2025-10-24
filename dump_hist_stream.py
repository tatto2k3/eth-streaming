from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, expr
from pyspark.sql.types import *

SPARK_VER = "4.0.1"     # đúng với pyspark.__version__
SCALA_BIN = "2.13"      # Spark 4.x dùng Scala 2.13

spark = (
    SparkSession.builder
    .appName("DumpHist")
    .config(
        "spark.jars.packages",
        f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BIN}:{SPARK_VER}"
    )
    .getOrCreate()
)

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
       .option("startingOffsets", "earliest")   # lấy từ đầu
       .load())

df = (raw.selectExpr("CAST(value AS STRING) AS json")
          .select(from_json(col("json"), schema).alias("r"))
          .select("r.*")
          .withColumn("event_time", to_timestamp(col("timestamp")))
          .withColumn("value_eth", col("value_wei")/expr("1e18"))
          .withColumn("gasPrice_gwei", col("gasPrice_wei")/expr("1e9"))
          .select("event_time","value_eth","gasPrice_gwei","input_len"))

q = (df.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", "data/tx_hist")             # sẽ tạo D:\eth-streaming\data\tx_hist
      .option("checkpointLocation", "ck/tx_hist") # checkpoint
      .start())

q.awaitTermination()
