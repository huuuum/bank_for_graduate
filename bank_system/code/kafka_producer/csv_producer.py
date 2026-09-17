# 模拟数据流
import json
import time
import csv
from pathlib import Path
from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_PATH = str(BASE_DIR / "data" / "train.csv")

# 配置kafka
KAFKA_SERVER = ["localhost:9092"]
TOPIC = "bank-customers"
SEND_INTERVAL = 0.1

# 创建生产者
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v : json.dumps(v,ensure_ascii=False).encode('utf-8'))  # 把字典转为JSON再转为字节，kafka只接受字节

# 读取文件
with open(DATA_PATH,"r",encoding="utf-8") as f:
    rows = list(csv.DictReader(f))
print(f"已读取:{len(rows)}条数据，开始生成模拟流")

# 模拟数据流
count = 0
try:
    while True:
        for row in rows:
            producer.send(TOPIC,value=row)
            count += 1
            if count % 500 == 0:
                print(f"已发送:{count}条数据")
            time.sleep(SEND_INTERVAL)
except KeyboardInterrupt:
    print(f"\n暂停发送，已发送{count}条数据")
finally:
    producer.flush()
    producer.close()