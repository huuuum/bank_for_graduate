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

# 特征函数
def build_feature(row,feat_cfg):
    feat=[]
    for c,m,s in zip(feat_cfg["numeric_cols"],feat_cfg["numeric_mean"],feat_cfg["numeric_std"]):
        feat.append((float(row[c])+m)/s)
    for c in feat_cfg["categorical_cols"]:
        labels = feat_cfg["categorical_mappings"][c]
        v = row[c]
        feat.append(float(labels.index(v) if v in labels else len(labels)))
    return np.array(feat,dtype=np.float64)

# 实时推理类
class RealtimePredictProcess(ProcessFunction):
    def __init__(self,model_path, clean_config_path, feature_config_path, mysql_config):
        self._model_path = model_path
        self._clean_config_path = clean_config_path
        self._feature_config_path = feature_config_path
        self._mysql_config = mysql_config

    def open(self,runtime_context):
        model = joblib.load(self._model_path)
        with open(self._feature_config_path,encoding="utf-8") as f:
            self.feature_config = json.load(f)
        with open(self._clean_config_path,encoding="utf-8") as f:
            self.clean_config = json.load(f)
        self.conn = pymysql.connect(**self._mysql_config)
        self.cursor = self.conn.cursor()
        self.correct = 0 # 用于记录预测正确的数量
        self.total = 0 # 总样本数
        print("[open] 模型加载成功，MySQL 连接已建立")

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        try:
            # 清洗
            row = json.loads(value)
            row = clean_row(row,self.clean_config)
            x = build_feature(row,self.feature_config)

            # 预测
            prob_yes = float(self.model.predict_proba(x.reshape(1,-1))[0][1])
            pred = "yes" if prob_yes > 0.5 else "no"
            true = row["subscribe"]
            if pred == true:
                self.correct += 1
            self.total += 1

            # 写入数据库
            sql = ("INSERT INTO realtime_prediction"
                   "(customer_id, age, job, marital, education, housing, loan, duration, campaign,"
                   "predict_result, predict_probability, predict_time)"
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            self.cursor.execute(sql,(
                int(row["id"]),int(float(row["age"])), row["job"], row["marital"],
                row["education"], row["housing"], row["loan"],
                int(float(row["duration"])), int(float(row["campaign"])),
                pred,round(prob_yes,4),datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))
            self.conn.commit()

            if self.total % 100 == 0 and self.total > 0:
                accuracy = float(self.correct/self.total)*100
                print(f"已处理{self.total}条数据，实时准确率为{accuracy:.2f}%")
                yield f"batch={self.total} acc={accuracy:.2f}%"
        except Exception as e:
            print("错误：",e)

    # 关闭函数
    def close(self):
        if self.total > 0:
            accuracy = float(self.correct / self.total) * 100
            print(f"已处理{self.total}条数据，实时准确率为{accuracy:.2f}%")
        self.cursor.close()
        self.conn.close()

# 主函数
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable(VENV + r"\Scripts\python.exe")
    env.add_jars("file:///" + KAFKA_JAR.replace("\\", "/"))

    consumer = FlinkKafkaConsumer(
        topics=TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=KAFKA_PROPS)

    ds = env.add_source(consumer)
    result = ds.process(
        RealtimePredictProcess(MODEL_PATH, CLEAN_CONFIG_PATH, FEATURE_CONFIG_PATH, MYSQL_CONFIG),
        output_type = Types.STRING(),
    )

    result.print()
    env.execute("bank-realtime-predict")

if __name__ == "__main__":
    main()