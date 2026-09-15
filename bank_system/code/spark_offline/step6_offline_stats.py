from pathlib import Path
from datetime import datetime
import pymysql

#配置根目录
BASE_DIR = Path(__file__).resolve().parents[2]
CLEAN_PATH = str(BASE_DIR / "output" / "train_clean.csv")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 配置MySQL连接
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "okb13950958240",
    "database": "bank_system",
    "charset": "utf8mb4",
}

# 配置spark
spark = (SparkSession.builder
         .appName("step6_offline_stats")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 读取数据
df = spark.read.csv(CLEAN_PATH,header=True,inferSchema=True)
print(f"读取{df.count()}条数据")

# 通用统计函数（传入数据集和分组字段，输出该字段每种取值的订购数量）
def group_stats(df,group_col):
    return (
        df.groupBy(group_col)
        .agg(
            F.count("*").alias("customer_count"),
            F.sum(F.when(F.col("subscribe")=="yes",1).otherwise(0)).alias("subscribe_count")
        ).orderBy(group_col)
        .collect()
    )

# 总体统计（overall_stats）
total = df.count()
subscribed = df.filter(F.col("subscribe")=="yes").count()  #filter对行数据操作，select对列数据进行操作；filter会删除不符合的列，withColumn用于新增列
unsubscribed = total - subscribed
subscribed_rate = round(subscribed/total ,4)
avg_age = round(df.agg(F.avg(F.col("age"))).first()[0],2)
avg_duration = round(df.agg(F.avg(F.col("duration"))).first()[0],2)
stat_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

overall_stats_row = (total,subscribed,unsubscribed,subscribed_rate,avg_age,avg_duration,stat_time)

print("="*60)
print(f"总客户数：{total}，"
      f"认购数：{subscribed},"
      f"认购率：{subscribed_rate}，"
      f"平均年龄：{avg_age}，"
      f"平均通话时长：{avg_duration}，"
      f"处理时间：{stat_time}")

# 年龄分布
df = df.withColumn("age_group",
                   F.when(F.col("age")<20,"0~19")
                   .when(F.col("age")<30,"20~29")
                   .when(F.col("age")<40,"30~39")
                   .when(F.col("age")<50,"40~49")
                   .when(F.col("age")<60,"50~59")
                   .otherwise("60+")
                   )
age_rows = [(r["age_group"],r["customer_count"],r["subscribe_count"])
            for r in group_stats(df,"age_group")]

job_rows = [(r["job"], r["customer_count"], r["subscribe_count"])
            for r in group_stats(df,"job")]

marital_rows = [(r["marital"], r["customer_count"], r["subscribe_count"])
                for r in group_stats(df,"marital")]

education_rows = [(r["education"], r["customer_count"], r["subscribe_count"])
                  for r in group_stats(df,"education")]

month_rows = [(r["month"], r["customer_count"], r["subscribe_count"])
              for r in group_stats(df,"month")]

rows = ["age_group","job","marital","education","month"]

# 注：10s给字符串占位用，10d给数值占位用
# 预览各分布统计结果（遍历已算好的结果，避免重复调用 group_stats）
for name, data in [("age_group", age_rows),
                   ("job", job_rows),
                   ("marital", marital_rows),
                   ("education", education_rows),
                   ("month", month_rows)]:
    print("=" * 60)
    print(f"【{name} 分布】")
    for r in data:
        print(f"  {r[0]:18s} 客户数 {r[1]:>6d}  认购数 {r[2]:>6d}")

# 写入MySQL数据库
# 插入函数
def insert_rows(table,columns,values): # table表名 columns字段名 values值
    conn = None
    cursor = None
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        placeholders = ",".join(["%s"] * len(columns))
        sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(sql, values)
        conn.commit()
        print(f"已插入{table}:{len(values)}行")
    except Exception as e:
        if conn is not None:
            conn.rollback()
            print("写入失败",e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

print("="*60)
insert_rows("overall_stats",
            ["total_customers", "subscribed_count", "not_subscribed_count",
             "subscribe_rate", "avg_age", "avg_duration", "stat_time"],
            [overall_stats_row])


insert_rows("age_stats", ["age_group", "customer_count", "subscribe_count"], age_rows)
insert_rows("job_stats", ["job", "customer_count", "subscribe_count"], job_rows)
insert_rows("marital_stats", ["marital", "customer_count", "subscribe_count"], marital_rows)
insert_rows("education_stats", ["education", "customer_count", "subscribe_count"], education_rows)
insert_rows("month_stats", ["month", "customer_count", "subscribe_count"], month_rows)

spark.stop()
print("="*60)
print("离线统计写库完成！")