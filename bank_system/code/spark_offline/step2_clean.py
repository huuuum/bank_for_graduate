# 阶段二，数据清洗
from pathlib import Path

# 设定目录
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_PATH = str(BASE_DIR / "data" / "train.csv")
OUTPUT_PATH = str(BASE_DIR / "output" / "train_clean.csv")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 配置spark
spark = (SparkSession.builder
         .appName("bank_step2_clean")
         .master("local[2]")
         .getOrCreate())

# 读取数据
df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
print("清洗前的行数：", df.count())

# 缺失值unknown的处理
unknown_cols = ["job","marital","education","default","housing","loan"]

# 众数的计算（将数值计算整理排序后，取出第一个元素的第一行元素）
for c in unknown_cols:
    mode_val = (df.groupBy(c)
    .count()
    .orderBy(F.desc("count"))
    .first()[0]
    )

    # 替换众数
    df = df.withColumn(c, F.when(F.col(c) == "unknown" , mode_val).otherwise(F.col(c)))
    print(f"{c}列的unknown已经替换为{mode_val}")

# 处理异常值
df = df.filter(F.col("duration")>0)

# 验证清洗结果
print("="*60)
print("清洗后数据行数为：",df.count())
print("="*60)
for c in unknown_cols:
    n = df.filter(F.col(c) == "unknown").count()
    print(f"{c:20s}unknown数量为：{n}")
print("="*60)
print(f"duration的最小值为")