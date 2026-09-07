# 阶段一，数据探索
from pathlib import Path

# 设定根目录和数据目录
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_PATH = str(BASE_DIR / "data" / "train.csv")

#创建 Spark 入口
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# 配置spark
spark = (SparkSession.builder
         .appName("bank_step1_explore")  #应用名
         .master("local[2]")
         .getOrCreate())    #创建入口

# 仅打印错误和警告
spark.sparkContext.setLogLevel("WARN")

# 读数据
df = spark.read.csv(DATA_PATH,header=True,inferSchema=True)

# 探索数据
print("=" * 60)
print("数据基础信息")
print("行数:",df.count())
print("列数:",len(df.columns))

print("=" * 60)
print("schema表结构")
df.printSchema()

# 缺失值
print("=" * 60)
missing = df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])

missing.show(vertical=True, truncate=False)

# subscribe标签分布情况（是否订阅贷款）
print("=" * 60)
print("订阅数量")
df.groupBy("subscribe").count().show()
print("="*60)

# 每种特征的取值数量
print("=" * 60)
print("每种特征的取值数量")
cat_cols = [c for c,t in df.dtypes if t == "string" and c != "subscribe"]
for c in cat_cols:
    n = df.select(c).distinct().count()
    print(f"{c:20s}{n}种取值")

# 探索是否有非法占用符号
illegal = ["unknown", "?", "-", "NA", "null", "NaN", ""]
for c in df.columns:
    for l in illegal:
        n = df.filter(F.col(c).cast("string") == l).count()
        if n > 0:
            print(f"{c:20s}含有{l}{n}个")


# 关键特征的取值
print("=" * 60)
print("关键特征的取值")
for c in ["job", "marital", "education", "default", "poutcome", "month", "day_of_week"]:
    vals = [r[c] for r in df.select(c).distinct().collect()]
    print(f"{c}: {vals}")

# 数值列统计量
print("="*60)
print("数值列统计量")
num_cols = [c for c,t in df.dtypes if t in ("int", "double", "bigint", "float") and c != "id"]
df.select(num_cols).describe().show(truncate=False)

# 列分类结论
print("="*60)
print("列分类")
cat_feature_cols = [c for c,t in df.dtypes if t == "string" and c != "subscribe"]
num_feature_cols = [c for c,t in df.dtypes if t in ("int", "double", "bigint", "float") and c != "id"]
print("类别特征列：",cat_feature_cols)
print("数值特征列：",num_feature_cols)

# 结束
print("="*60)
spark.stop()
print("数据探索结束")

