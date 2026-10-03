from pathlib import Path
import csv
import pymysql

BASE_DIR = Path(__file__).resolve().parents[2]
CSV_PATH = BASE_DIR / "output" / "stream_10.csv"

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "okb13950958240",
    "database": "bank_system",
    "charset": "utf8mb4",
}

# 需要转成数值类型的列
INT_COLS = {"id", "age", "duration", "campaign", "pdays", "previous"}
FLOAT_COLS = {"emp_var_rate", "cons_price_index", "cons_conf_index",
              "lending_rate3m", "nr_employed"}

# customer 表列顺序
COLUMNS = ["id", "age", "job", "marital", "education", "default", "housing",
           "loan", "contact", "month", "day_of_week", "duration", "campaign",
           "pdays", "previous", "poutcome", "emp_var_rate", "cons_price_index",
           "cons_conf_index", "lending_rate3m", "nr_employed", "subscribe"]

# 显示转换类型
def cast_value(col, value):
    value = value.strip()  # 取消前后空格
    if col in INT_COLS:
        return int(value)
    if col in FLOAT_COLS:
        return float(value)
    return value

def main():
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        rows = [tuple(cast_value(c , row[c]) for c in COLUMNS) for row in csv.DictReader(f)]
    print(f"已读取{len(rows)}条数据")
    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute("TRUNCATE TABLE customer")
        placeholders = ", ".join(["%s"] * len(COLUMNS))
        sql_cols = ", ".join(f"`{c}`" if c =="default" else c for c in COLUMNS)
        sql = f"INSERT INTO customer({sql_cols}) VALUES ({placeholders});"
        cursor.executemany(sql, rows)
        conn.commit()
        print("="*60)
        print("导入成功")
    except Exception as e:
        conn.rollback()
        print("导入失败：", e)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()