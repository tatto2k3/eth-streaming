# ============================================================
# consumer_alert.py — Nhận và hiển thị giao dịch bất thường
# ============================================================
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "eth.alerts",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest"
)

print("Listening for ETH anomalies...\n")
for msg in consumer:
    data = msg.value
    print(f"Block: {data.get('blockNumber')}, Tx: {data.get('hash')}, Value: {data.get('value_eth')} ETH, Error: {data.get('recon_error')}")
