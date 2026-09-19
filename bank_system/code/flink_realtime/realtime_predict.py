# pyflink实时预测

import os
import json
from datetime import datetime
from pathlib import Path


# 配置环境变量，pyflink不兼容所以用新建的变量
BASE_DIR = Path(__file__).resolve().parents[2]
VENV = str(BASE_DIR / ".flink-venv")
JAVA_HOME = r"C:\Users\1\dev\jdk-17.0.20.1+1"
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = (
    VENV + r"\Scripts"
    + os.pathsep
    + JAVA_HOME + r"\bin"
    + os.pathsep
    + os.environ["PATH"]
)

import numpy as np
import joblib
import pymysql
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import ProcessFunction
from pyflink.common import Types

# 配置目录
MODEL_PATH = str(BASE_DIR / "model" / "rf_model.joblib")
CLEAN_CONFIG_PATH = str(BASE_DIR / "config" / "clean_config.json")
FEATURE_CONFIG_PATH = str(BASE_DIR / "models" / "config.json")
KAFKA_JAR = str(BASE_DIR / "code" / "flink_realtime" / "lib" / "flink-sql-connector-kafka-5.0.0-2.2.jar")

# 配置kafka
TOPIC = "bank-customer"
KAFKA_PROPS = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "bank-flink-group",
    "auto.offset.reset": "earliest",
}

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "okb13950958240",
    "database": "bank_system",
    "charset": "utf8mb4",
}

# 清洗函数
def clean_row(row,clean_cfg):
    for c in clean_cfg:
        if row[c] == "unknown":
            row[c] = clean_cfg["mode_map"][c]
    for c,(lo,hi) in clean_cfg["winsorize_bounds"].items():  # item()遍历取出key和value
        v = float(row[c])
        row[c] = max(lo,min(hi,v)) # min(hi,v) hi小于v的话取hi，max(lo,v) lo大于v时取lo
    return row
