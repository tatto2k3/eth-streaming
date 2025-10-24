# ============================================================
# model_train.py — Train Autoencoder để phát hiện bất thường ETH
# ============================================================
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import StandardScaler
import glob
import numpy as np
import os

# ==== Load dữ liệu CSV đã ghi bởi Spark Streaming ====
files = glob.glob("./output/eth-streaming-csv/*.csv")
if not files:
    raise FileNotFoundError("⚠️ Không tìm thấy file CSV trong ./output/eth-streaming-csv/. Hãy chạy streaming.py trước.")

df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

# ==== Chọn các cột đặc trưng quan trọng ====
features = ["value_eth", "gas", "gasPrice_gwei", "nonce", "input_len"]
df = df[features].fillna(0)

# ==== Chuẩn hóa dữ liệu ====
scaler = StandardScaler()
X = scaler.fit_transform(df)

os.makedirs("./output", exist_ok=True)
np.save("./output/scaler_mean.npy", scaler.mean_)
np.save("./output/scaler_scale.npy", scaler.scale_)

# ==== Xây mô hình Autoencoder ====
input_dim = X.shape[1]
model = tf.keras.Sequential([
    tf.keras.layers.Input(shape=(input_dim,)),
    tf.keras.layers.Dense(8, activation='relu'),
    tf.keras.layers.Dense(4, activation='relu'),
    tf.keras.layers.Dense(8, activation='relu'),
    tf.keras.layers.Dense(input_dim, activation='linear')
])

model.compile(optimizer='adam', loss='mse')
model.summary()

# ==== Huấn luyện ====
history = model.fit(X, X, epochs=20, batch_size=64, validation_split=0.1, verbose=1)

# ==== Lưu mô hình ====
model.save("./output/autoencoder_model.keras")
print("Đã lưu mô hình Autoencoder tại ./output/autoencoder_model.keras")