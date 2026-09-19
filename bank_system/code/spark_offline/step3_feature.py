# 阶段三：特征工程
import json
from pathlib import Path
import numpy as np

# 设定目录
BASE_DIR = Path(__file__).resolve().parents[2]
TRAIN_CLEAN_PATH = str(BASE_DIR / "output" / "train_70_clean.csv")
TEST_CLEAN_PATH = str(BASE_DIR / "output" / "test_20_clean.csv")
FEATURES_CONFIG_PATH = str(BASE_DIR / "models" / "config.json")
FEATURES_TRAIN_PATH = str(BASE_DIR / "output" / "features.npz")
FEATURES_TEST_PATH = str(BASE_DIR / "output" / "features_test.npz")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler

# 配置spark
spark = (SparkSession.builder
         .appName("bank_step3_feature")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv(TRAIN_CLEAN_PATH, header=True, inferSchema=True)

# 设定特征列
cat_cols = [c for c,t in df.dtypes if t == "string" and c != "subscribe"]
num_cols = [c for c,t in df.dtypes if t in ["int","bigint","float","double"] and c != "id"]

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
df = scaler_model.transform(df)

# 展示标准化后的结果
# print("="*60)
# print("标准化后前两行数据展示")
# df.select("scaled_num").show(2,truncate=False)

# 提取模型中的均值和标准差
num_mean = scaler_model.mean.tolist()
num_std = scaler_model.std.tolist()

# 类别特征编码
indexer_models = {}
for c in cat_cols:
    model = StringIndexer(inputCol=c, outputCol=f"{c}_idx",handleInvalid="keep").fit(df)
    indexer_models[c] = model
    df = model.transform(df)
    # print("="*60)
    # print(f"{c}特征编码后后前两行数据展示")
    # df.select(f"{c}_idx").show(2,truncate=False)

# 最终特征 = 标准化特征+特征编码
feature_input_col = ["scaled_num"] + [c + "_idx" for c in cat_cols]
final_assembler = VectorAssembler(inputCols=feature_input_col,outputCol="features")
df = final_assembler.transform(df)
# print("="*60)
# print(f"前两行数据展示")
# df.select("features").show(2,truncate=False)

# 标签转换
df = df.withColumn("label",
                   F.when(F.col("subscribe") == "yes",1.0).otherwise(0.0))

# 导出JSON配置，用于流批一体
config = {
    "numeric_cols": num_cols,
    "categorical_cols": cat_cols,
    "numeric_mean": num_mean,
    "numeric_std": num_std,
    "categorical_mappings": {c : indexer_models[c].labels for c in cat_cols},
    "feature_order": num_cols + cat_cols,
    "label_mappings": {"yes" : 1.0, "no" : 0.0},
}

# 保存文件
with open(FEATURES_CONFIG_PATH, "w" ,encoding="utf-8") as f:
    json.dump(config, f, ensure_ascii=False, indent=2)
print("文件已保存于", FEATURES_CONFIG_PATH)

# 特征矩阵保存
def save_features(df, path):
    rows = df.select("features", "label").collect()
    X = np.array([r["features"].toArray() for r in rows])
    y = np.array([r["label"] for r in rows])
    np.savez(path, X=X, y=y)
    return X,y
X_train, y_train = save_features(df, FEATURES_TRAIN_PATH)

# 结果预览
print("="*60)
print(f"训练集特征维度{X_train.shape[1]}({len(num_cols)}数值列+{len(cat_cols)}类别列)")
print(f"训练集样本数量{X_train.shape[0]}")
print(f"训练集标签分布:yes={int(sum(y_train==1))}, no={int(sum(y_train==0))}")
df.select("features","label").show(5,truncate=False)

# 测试集处理
df_test = spark.read.csv(TEST_CLEAN_PATH, header=True, inferSchema=True)
df_test = num_assembler.transform(df_test)
df_test = scaler_model.transform(df_test)
for c in cat_cols:
    df_test = indexer_models[c].transform(df_test)
df_test = final_assembler.transform(df_test)
df_test = df_test.withColumn("label", F.when(F.col("subscribe") == "yes", 1.0).otherwise(0.0))

X_test, y_test = save_features(df_test, FEATURES_TEST_PATH)
print("=" * 60)
print(f"测试集特征维度 {X_test.shape[1]}，样本数 {X_test.shape[0]}")
print(f"测试集标签分布 yes={int(sum(y_test == 1))}, no={int(sum(y_test == 0))}")

df.select("features", "label").show(5, truncate=False)

spark.stop()
print("特征工程结束")