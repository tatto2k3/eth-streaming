import os, time, json, requests, signal, sys
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
API_KEY = os.getenv("ETHERSCAN_KEY")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "eth.tx")
LAST_FILE = ".last_block"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=20, acks="all"
)

def get_latest_block():
    url = f"https://api.etherscan.io/v2/api?chainid=1&module=proxy&action=eth_blockNumber&apikey={API_KEY}"
    res = requests.get(url, timeout=10).json()
    return int(res["result"], 16)

def get_block(num: int):
    tag = hex(num)
    url = f"https://api.etherscan.io/v2/api?chainid=1&module=proxy&action=eth_getBlockByNumber&tag={tag}&boolean=true&apikey={API_KEY}"
    res = requests.get(url, timeout=15).json()["result"]
    tx_count = len(res.get("transactions", []))
    print(f"Block {num} có {tx_count} transactions.")
    return res

def read_last():
    try:
        return int(open(LAST_FILE).read().strip())
    except:
        return None

def write_last(b):
    with open(LAST_FILE, "w") as f:
        f.write(str(b))
    print("Wrote last block:", b)

exiting = False
def graceful_exit(sig, frame):
    global exiting
    if exiting:
        return
    exiting = True
    print("\nDừng chương trình theo yêu cầu người dùng...")
    write_last(last)
    producer.flush()
    producer.close(timeout=5)
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

latest = get_latest_block()
last = read_last() or (latest - 1)

if __name__ == "__main__":
    while True:
        try:
            cur = get_latest_block()
            if cur > last:
                for b in range(last + 1, cur + 1):
                    blk = get_block(b)
                    base = {
                        "blockNumber": int(blk["number"], 16),
                        "timestamp": int(blk["timestamp"], 16),
                        "miner": blk.get("miner"),
                        "tx_count": len(blk.get("transactions", []))
                    }
                    for tx in blk.get("transactions", []):
                        rec = base | {
                            "hash": tx["hash"],
                            "from": tx["from"],
                            "to": tx.get("to"),
                            "value_wei": int(tx["value"], 16),
                            "gas": int(tx["gas"], 16),
                            "gasPrice_wei": int(tx["gasPrice"], 16),
                            "nonce": int(tx["nonce"], 16),
                            "input_len": len(tx.get("input", "")) // 2
                        }
                        producer.send(TOPIC, rec)
                    last = b
                write_last(cur)
                producer.flush()
            time.sleep(3)
        except Exception as e:
            print("Lỗi:", e)
            time.sleep(5)
