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
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import ProcessFunction
from pyflink.common import Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy

# 配置目录
MODEL_PATH = str(BASE_DIR / "models" / "rf_model.joblib")
CLEAN_CONFIG_PATH = str(BASE_DIR / "models" / "clean_config.json")
FEATURE_CONFIG_PATH = str(BASE_DIR / "models" / "config.json")
KAFKA_JAR = str(BASE_DIR / "code" / "flink_realtime" / "lib" / "flink-sql-connector-kafka-5.0.0-2.2.jar")

# 配置kafka
TOPIC = "bank-customers"
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
    for c in clean_cfg["unknown_cols"]:
        if row[c] == "unknown":
            row[c] = clean_cfg["mode_map"][c]
    for c,(lo,hi) in clean_cfg["winsorize_bounds"].items():  # item()遍历取出key和value
        v = float(row[c])
        row[c] = max(lo,min(hi,v)) # min(hi,v) hi小于v的话取hi，max(lo,v) lo大于v时取lo
    return row

# 特征函数
def build_feature(row,feat_cfg,label_index):
    feat=[]
    for c,m,s in zip(feat_cfg["numeric_cols"],feat_cfg["numeric_mean"],feat_cfg["numeric_std"]):
        feat.append((float(row[c])-m)/s)
    for c in feat_cfg["categorical_cols"]:
        # labels = feat_cfg["categorical_mappings"][c]
        # v = row[c]
        # feat.append(float(labels.index(v) if v in labels else len(labels)))
        # 改为字典查找，性能更高
        feat.append((float(label_index[c].get(row[c],len(label_index[c])))))
    return np.array(feat,dtype=np.float64)

# 实时推理类
class RealtimePredictProcess(ProcessFunction):
    def __init__(self,model_path, clean_config_path, feature_config_path, mysql_config):
        self._model_path = model_path
        self._clean_config_path = clean_config_path
        self._feature_config_path = feature_config_path
        self._mysql_config = mysql_config

    def open(self,runtime_context):
        self.model = joblib.load(self._model_path)
        with open(self._feature_config_path,encoding="utf-8") as f:
            self.feature_config = json.load(f)
        with open(self._clean_config_path,encoding="utf-8") as f:
            self.clean_config = json.load(f)
        # 生成映射表，加快查找速度
        self.label_index = {
            # c是特征名，v是特征取值，i是下标
            c:{v:i for i,v in enumerate(labels)}
            for c,labels in self.feature_config["categorical_mappings"].items()
        }
        # self.conn = pymysql.connect(**self._mysql_config)
        # self.cursor = self.conn.cursor()
        self.correct = 0 # 用于记录预测正确的数量
        self.total = 0 # 总样本数
        print("[open] 模型加载成功，MySQL 连接已建立")

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        try:
            # 清洗
            row = json.loads(value)
            row = clean_row(row,self.clean_config)
            x = build_feature(row,self.feature_config,self.label_index)

            # 预测
            prob_yes = float(self.model.predict_proba(x.reshape(1,-1))[0][1])
            pred = "yes" if prob_yes > 0.5 else "no"
            true = row["subscribe"]
            if pred == true:
                self.correct += 1
            self.total += 1

            self._write_to_mysql(row,pred,prob_yes)

            if self.total % 100 == 0 and self.total > 0:
                accuracy = float(self.correct/self.total)*100
                print(f"已处理{self.total}条数据，实时准确率为{accuracy:.2f}%")
                yield f"batch={self.total} acc={accuracy:.2f}%"
        except Exception as e:
            print("错误：",e)

    # 下划线开头表示私有方法，只能在这个类里面调用
    def _write_to_mysql(self,row,pred,prob_yes):
        conn = pymysql.connect(**self._mysql_config)
        try:
            sql = ("INSERT IGNORE INTO realtime_prediction"  # ignore在触发唯一约束时不报错直接执行下一步
                   "(customer_id, age, job, marital, education, housing, loan, duration, campaign,"
                   "predict_result, predict_probability, predict_time)"
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            # 优先取事件，防止用处理时间导致时间的数据不是真实数据真实入库时间
            predict_time = row.get("event_time") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with conn.cursor() as cursor:
                cursor.execute(sql, (
                    int(row["id"]), int(float(row["age"])), row["job"], row["marital"],
                    row["education"], row["housing"], row["loan"],
                    int(float(row["duration"])), int(float(row["campaign"])),
                    pred, round(prob_yes, 4), predict_time,
                ))
            conn.commit()
        finally:
            conn.close()

    # 关闭函数
    def close(self):
        if self.total > 0:
            accuracy = float(self.correct / self.total) * 100
            print(f"已处理{self.total}条数据，实时准确率为{accuracy:.2f}%")

# 主函数
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable(VENV + r"\Scripts\python.exe")
    env.add_classpaths("file:///" + KAFKA_JAR.replace("\\", "/"))

    # 开启checkpoint，保证数据不丢失。checkpoint成功后再更新offset
    env.enable_checkpointing(5000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics(TOPIC)
        .set_group_id("bank-flink-group")
        .set_value_only_deserializer(SimpleStringSchema())  # 把value转成字符串
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .build()
    )
    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")

    result = ds.process(
        RealtimePredictProcess(MODEL_PATH, CLEAN_CONFIG_PATH, FEATURE_CONFIG_PATH, MYSQL_CONFIG),
        output_type = Types.STRING(),
    )

    result.print()
    env.execute("bank-realtime-predict")

if __name__ == "__main__":
    main()



# :: 窗口 A：Kafka（已在跑则跳过）
# cd C:\Users\1\dev\kafka_2.13-4.3.1
# bin\windows\kafka-server-start.bat config\server.properties
#
# :: 窗口 B：生产者（全局 py）
# cd E:\PycharmProject\bank_system
# py code\kafka_producer\csv_producer.py
#
# :: 窗口 C：Flink 实时推理（必须用 venv 的 python）
# cd E:\PycharmProject\bank_system
# .flink-venv\Scripts\python.exe code\flink_realtime\realtime_predict.py

# 清空topic数据
# cd C:\Users\1\dev\kafka_2.13-4.3.1
# bin\windows\kafka-topics.bat --delete --topic bank-customers --bootstrap-server localhost:9092
# bin\windows\kafka-topics.bat --create --topic bank-customers --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1