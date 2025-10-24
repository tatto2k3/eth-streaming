# ============================================================
# predict_udf.py — Gọi mô hình Autoencoder trong Spark Streaming
# ============================================================
from pyspark.ml.functions import predict_batch_udf
import tensorflow as tf
import numpy as np

# ==== Hàm predict_batch_udf ====
def predict_fn():
    # Load model và scaler
    model = tf.keras.models.load_model("./output/autoencoder_model.keras")
    mean = np.load("./output/scaler_mean.npy")
    scale = np.load("./output/scaler_scale.npy")

    def predict(X):
        # Chuẩn hóa giống khi train
        X = (X - mean) / scale
        recon = model.predict(X)
        err = np.mean(np.square(X - recon), axis=1)
        return err.tolist()

    return predict

# ==== UDF cho Spark ====
anomaly_udf = predict_batch_udf(
    predict_fn,
    input_tensor_shapes=[[5]],
    return_type="array<float>",
    batch_size=64
)
