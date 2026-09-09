# 阶段三：特征工程
import json
from pathlib import Path
import numpy as np
from pyspark.pandas import spark

from bank_system.code.spark_offline.step1_explore import cat_cols

# 设定目录
BASE_DIR = Path(__file__).resolve().parents[2]
CLEAN_PATH = str(BASE_DIR / "output" / "train_clean.csv")
FEATURES_CONFIG_PATH = str(BASE_DIR / "models" / "config.json")
FEATURES_PATH = str(BASE_DIR / "output" / "features.npz")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler

# 配置spark
spark = (SparkSession.builder
         .appName("bank_step3_feature")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv(CLEAN_PATH, header=True, inferSchema=True)

# 设定特征列
cat_cols = (c for c,t in df.dtypes if t == "string" or c != "subscribe")
num_cols = (c for c,t in df.dtypes if t in ["int","bigint","float","double"] or c != "id")

# 将数值特征规范化
num_assembler = VectorAssembler(inputCols=num_cols, outputCol="num_vec")
df = num_assembler.transform(df)

# 标准差标准化 withMean减去均值 withStd除以标准差
num_scaler = StandardScaler(
    inputCol="num_vec",
    outputCol="scaled_num",
    withMean=True,
    withStd=True,
)
scaler_model = num_scaler.fit(df)
scaled_df = scaler_model.transform(df)

# 提取模型中的均值和标准差
num_mean = scaler_model.mean.tolist()
num_std = scaler_model.std.tolist()

# 类别特征编码
indexer_model = {}
for c in cat_cols:
    model = StringIndexer(inputCol=c, outputCol=f"{c}_idx",handleInvalid="keep").fit(df)
    indexer_model[c] = model
    df = model.transform(df)