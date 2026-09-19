# 阶段二，数据清洗
from pathlib import Path
import json

# 设定目录
BASE_DIR = Path(__file__).resolve().parents[2]
TRAIN_70_PATH = str(BASE_DIR / "output" / "train_70.csv")
TEST_20_PATH = str(BASE_DIR / "output" / "test_20.csv")
STREAM_10_PATH = str(BASE_DIR / "output" / "stream_10.csv")

TRAIN_70_CLEAN = str(BASE_DIR / "output" / "train_70_clean.csv")
TEST_20_CLEAN = str(BASE_DIR / "output" / "test_20_clean.csv")
STREAM_10_CLEAN = str(BASE_DIR / "output" / "stream_10_clean.csv")
CLEAN_CONFIG_PATH = str(BASE_DIR / "models" / "clean_config.json")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 配置spark
spark = (SparkSession.builder
         .appName("bank_step2_clean")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 读取数据
df_train = spark.read.csv(TRAIN_70_PATH, header=True, inferSchema=True)
df_test = spark.read.csv(TEST_20_PATH, header=True, inferSchema=True)
df_stream = spark.read.csv(STREAM_10_PATH, header=True, inferSchema=True)
print("清洗前的行数：", df_train.count())

# 缺失值unknown的处理
unknown_cols = ["job","marital","education","default","housing","loan"]

# 众数的计算（将数值计算整理排序后，取出第一个元素的第一行元素）
mode_map={}
for c in unknown_cols:
    mode_val = (df_train.groupBy(c)
    .count()
    .orderBy(F.desc("count"))
    .first()[0]
    )
    mode_map[c] = mode_val
    print(f"  {c:12s} 众数 = {mode_val}")




# 异常值的处理
# 判断是否有负数
non_neg_cols = ["age", "duration", "campaign", "pdays", "previous"]
print("="*60)
print("异常值负数的检测")
df_train.select([
    F.sum((F.col(c) <0).cast("int")).alias(c)for c in non_neg_cols]
).show(vertical=True, truncate=False)

# 离群点检测：箱线图法
print("="*60)
print("箱线图法检测离群点")

outlier_cols = ["age", "duration", "campaign", "previous"]
for c in outlier_cols:
    q1, q3 = df_train.select(F.expr(f"percentile({c}, array(0.25, 0.75))")).first()[0]
    iqr = q3 - q1
    upper = q3 + iqr * 1.5
    lower = q1 - iqr * 1.5
    n = df_train.filter((F.col(c) < lower) | (F.col(c) > upper)).count()
    print(f"{c:15s}上界为：{upper:.1f},下界为{lower:.1f},离群点有：{n}个")

# 对离群点进行 Winsorize 缩尾
print("="*60)
winsorize_cols = ["age", "duration", "campaign"]  #previous范围小，只有0-6，不需要缩尾
winsorize_bounds = {}
for c in winsorize_cols:
    lo_hi = df_train.select(F.expr(f"percentile({c}, array(0.01, 0.99))")).first()[0]
    low, high = lo_hi[0], lo_hi[1]
    winsorize_bounds[c] = [low, high]

# 保存清洗参数
clean_config={
    "unknown_cols":unknown_cols,
    "mode_map":mode_map,
    "winsorize_bounds":winsorize_bounds,
    "winsorize_cols":winsorize_cols,
}
with open(CLEAN_CONFIG_PATH, "w",encoding="utf-8") as f:
    json.dump(clean_config, f, ensure_ascii=False, indent=2)
print("清洗参数已保存至", CLEAN_CONFIG_PATH)

# 清洗函数
def clean(df):
    for c in unknown_cols:
        df = df.withColumn(c, F.when(F.col(c)=="unknown", mode_map[c]).otherwise(F.col(c)))
    for c in winsorize_cols:
        low , high = winsorize_bounds[c]
        df = df.withColumn(c, F.when(F.col(c)<low,low).when(F.col(c)>high,high).otherwise(F.col(c)))
    return df

# 调用清洗函数
df_train_clean = clean(df_train)
df_test_clean = clean(df_test)
df_stream_clean = clean(df_stream)

# 保存文件
def save_csv(df,path):
    pdf=df.toPandas()
    pdf.to_csv(path,index=False)
    print(f"  已保存 {path}（{pdf.shape[0]} 行）")

print("="*60)
save_csv(df_train_clean, TRAIN_70_CLEAN)
save_csv(df_test_clean, TEST_20_CLEAN)
save_csv(df_stream_clean, STREAM_10_CLEAN)

# 验证清洗结果
print("="*60)
print("各列unknown数量：")
for name,df in [("train_70", df_train_clean),
                 ("test_20", df_test_clean),
                 ("stream_10", df_stream_clean)]:
    leftover = {c : df.filter(F.col(c)=="unknown").count() for c in unknown_cols}
    print(f"  {name}: unknown 残留 {leftover}")

print("="*60)
print("各列缩尾边界：")
for c in winsorize_cols:
    lo , hi = winsorize_bounds[c]
    print(f"  {c:12s} 缩尾后范围应为 [{lo:.2f}, {hi:.2f}]")

print("="*60)
spark.stop()
print("数据清洗结束")