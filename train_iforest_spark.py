from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark import SparkConf
from synapse.ml.isolationforest import IsolationForest

# conf = SparkConf()
# conf.set('spark.jars', '/full/path/to/spark-iforest-2.4.0.jar')

spark = (SparkSession.builder
         .appName("TrainIForestSpark")
         .getOrCreate())

# 1) Đọc dữ liệu lịch sử (đã được bạn ghi từ streaming)
hist = spark.read.parquet("data/tx_hist")

# 2) Tạo feature GIỐNG với lúc serve
data = (hist
        .withColumn("v_log", F.log1p(F.col("value_eth")))
        .withColumn("gas_log", F.log1p(F.col("gasPrice_gwei")))
        .na.drop(subset=["v_log", "gas_log", "input_len"]))

# 3) Pipeline: VectorAssembler -> StandardScaler -> IsolationForest
va = VectorAssembler(
    inputCols=["v_log", "gas_log", "input_len"],
    outputCol="feat"
)
scaler = StandardScaler(
    inputCol="feat", outputCol="feat_sc",
    withMean=True, withStd=True
)

# contamination = tỷ lệ outlier ước đoán (0.1%–1% tuỳ dữ liệu)
iforest = (IsolationForest()
           .setFeaturesCol("feat_sc")
           .setPredictionCol("prediction")
           .setAnomalyScoreCol("anomalyScore")
           .setContamination(0.005)     # ~0.5% outliers
           .setNumEstimators(300)        # <== thay cho numTrees
           .setMaxDepth(10)
           .setBootstrap(False)
           .setSeed(42))

pipe = Pipeline(stages=[va, scaler, iforest])

# 4) Fit model
model = pipe.fit(data)

# 5) (tuỳ chọn) Kiểm tra nhanh output schema
# out = model.transform(data.limit(1000))
# out.printSchema()
# out.select("v_log","gas_log","input_len","prediction").show(5, False)

# 6) Lưu model
model.write().overwrite().save("models/iforest_spark")

spark.stop()
