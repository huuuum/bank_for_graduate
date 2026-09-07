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

# 验证清洗结果
print("="*60)
print("清洗后数据行数为：",df.count())
print("="*60)
for c in unknown_cols:
    n = df.filter(F.col(c) == "unknown").count()
    print(f"{c:20s}unknown数量为：{n}")


# 异常值的处理
# 判断是否有负数
non_neg_cols = ["age", "duration", "campaign", "pdays", "previous"]
print("="*60)
print("异常值负数的检测")
df.select([
    F.sum((F.col(c) <0).cast("int")).alias(c)for c in non_neg_cols]
).show(vertical=True, truncate=False)

# 离群点检测：箱线图法
print("="*60)
print("箱线图法检测离群点")

outlier_cols = ["age", "duration", "campaign", "previous"]
for c in outlier_cols:
    q1, q3 = df.select(F.expr(f"percentile({c}, array(0.25, 0.75))")).first()[0]
    iqr = q3 - q1
    upper = q3 + iqr * 1.5
    lower = q1 - iqr * 1.5
    n = df.filter((F.col(c) < lower) | (F.col(c) > upper)).count()
    print(f"{c:15s}上界为：{upper:.1f},下界为{lower:.1f},离群点有：{n}个")

# 对离群点进行 Winsorize 缩尾
print("="*60)
winsorize_cols = ["age", "duration", "campaign"]  #previous范围小，只有0-6，不需要缩尾
for c in winsorize_cols:
    before = (df.agg(F.min(c)).first()[0], df.agg(F.max(c)).first()[0])
    lo_hi = df.select(F.expr(f"percentile({c}, array(0.01, 0.99))")).first()[0]
    low, high = lo_hi[0], lo_hi[1]
    df = df.withColumn(
        c,F.when(F.col(c)<low,low)
        .when(F.col(c)>high,high)
        .otherwise(F.col(c))
    )
    after = (df.agg(F.min(c)).first()[0], df.agg(F.max(c)).first()[0])
    print(f"  {c:10s}: 缩尾前 [{before[0]:.1f}, {before[1]:.1f}]  →  缩尾后 [{after[0]:.1f}, {after[1]:.1f}]")

# 保存文件
print("="*60)
import pandas as pd
pdf = df.toPandas()
pdf.to_csv(OUTPUT_PATH, index=False)
print("文件已保存至", OUTPUT_PATH)

print("="*60)
spark.stop()
print("数据清洗结束")