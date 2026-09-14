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

# 总体统计（overall_stats）
total = df.count()
subscribed = df.filter(F.col("subscribe")=="yes").count()  #filter对行数据操作，select对列数据进行操作；filter会删除不符合的列，withColumn用于新增列
unsubcribed = total - subscribed
subscribed_rate = round(subscribed/total ,2)
avg_age = round(df.agg(F.avg(F.col("age"))).first()[0],2)
avg_duration = round(df.agg(F.avg(F.col("duration"))).first()[0],2)
stat_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

overall_stats_row = (total,subscribed,unsubcribed,subscribed_rate,avg_age,avg_duration,stat_time)

# 年龄分布
df = df.withColumn("age_group",
                   F.when(F.col("age")<20,"0-19")
                   .when(F.col("age")<30,"20-29")
                   .when(F.col("age")<40,"30-39")
                   .when(F.col("age")<50,"40-49")
                   .when(F.col("age")<60,"50-59")
                   .otherwise("60+")
                   )

# 通用统计函数（传入数据集和分组字段，输出该字段每种取值的订购数量）
def group_stats(df,group_col):







print("="*60)
print(f"总客户数：{total}，"
      f"认购数：{subscribed},"
      f"认购率：{subscribed_rate}，"
      f"平均年龄：{avg_age}，"
      f"平均通话时长：{avg_duration}，"
      f"处理时间：{stat_time}")

