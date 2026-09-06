import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False

user = 'root'
password = 'okb13950958240'
port = 3306
database = 'bank_data'
host = 'localhost'

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4')

# 整体违约率
sql_total_default = """
SELECT
    COUNT(loan_id) AS total_loan_count,
    SUM(is_default) AS total_is_default,
    ROUND(SUM(is_default) / COUNT(loan_id),4) AS default_rate
FROM loan_info;
"""
df_total_default = pd.read_sql(sql_total_default,engine)
df_total_default['default_rate'] = df_total_default['default_rate'].apply(lambda x : f'{x:.2%}')
print('\n---------整体违约率----------')
print(df_total_default)

# 按类型分类
sql_type_default = """
SELECT
    loan_type,
    COUNT(loan_id) AS total_loan_count,
    SUM(is_default) AS total_is_default,
    SUM(loan_amount) AS total_loan_amount,
    ROUND(SUM(is_default) / COUNT(loan_id),4) AS default_rate,
    ROUND(AVG(loan_amount),0) AS avg_loan_amount
FROM loan_info GROUP BY loan_type;
"""

df_type_default = pd.read_sql(sql_type_default,engine)
df_type_default['default_rate'] = df_type_default['default_rate'].apply(lambda x : f'{x:.2%}')
df_type_default['avg_loan_amount'] = df_type_default['avg_loan_amount'].apply(lambda x : f'{x / 10000:.1f}万元')
print('\n---------按类型分类----------')
print(df_type_default)

# 按等级分类
# sql_level_default = """
# SELECT
#     cust_level,
#     COUNT(loan_id) AS total_loan_count,
#     SUM(is_default) AS total_is_default,
#     SUM(loan_amount) AS total_loan_amount,
#     ROUND(SUM(is_default) / COUNT(loan_id),4) AS default_rate,
#     ROUND(AVG(loan_amount),0) AS avg_loan_amount
# FROM full_info GROUP BY cust_level;
# """

sql_level_default = """
SELECT 
    c.cust_level,
    COUNT(loan_id) AS total_loan_count,
    COUNT(DISTINCT l.cust_id) AS total_cust_id,
    ROUND(SUM(l.is_default) / COUNT(l.cust_id),4) AS default_rate,
    ROUND(AVG(l.loan_amount),0) AS avg_loan_amount
FROM loan_info AS l
LEFT JOIN cust_info AS c ON l.cust_id = c.cust_id
GROUP BY c.cust_level ORDER BY default_rate DESC ; 
"""

df_level_default = pd.read_sql(sql_level_default,engine)
df_level_default['default_rate'] = df_level_default['default_rate'].apply(lambda x : f'{x:.2%}')
df_level_default['avg_loan_amount'] = df_level_default['avg_loan_amount'].apply(lambda x : f'{x / 10000:.1f}万元')
print('\n---------按等级分类----------')
print(df_level_default)

# 按年龄分类
# sql_age_default = """
# SELECT
#     age_group,
#     COUNT(loan_id) AS total_loan_count,
#     SUM(is_default) AS total_is_default,
#     SUM(loan_amount) AS total_loan_amount,
#     ROUND(SUM(is_default) / COUNT(loan_id),4) AS default_rate,
#     ROUND(AVG(loan_amount),0) AS avg_loan_amount
# FROM full_info GROUP BY age_group;
# """

sql_age_default = """
SELECT 
    CASE
        WHEN c.age BETWEEN 18 AND 25 THEN '18-25岁'
        WHEN c.age BETWEEN 26 AND 35 THEN '26-35岁'
        WHEN c.age BETWEEN 36 AND 45 THEN '36-45岁'
        WHEN c.age BETWEEN 46 AND 55 THEN '46-55岁'
        WHEN c.age BETWEEN 56 AND 65 THEN '56-65岁'
        WHEN c.age BETWEEN 66 AND 75 THEN '66-75岁'
        ELSE '异常年龄'
    END AS age_group,
    COUNT(l.loan_id) AS total_loan_count,
    SUM(l.is_default) AS total_is_default,
    SUM(l.loan_amount) AS total_loan_amount,
    ROUND(SUM(l.is_default) / COUNT(l.loan_id),4) AS default_rate,
    ROUND(AVG(l.loan_amount),0) AS avg_loan_amount
FROM loan_info AS l 
LEFT JOIN cust_info AS c ON l.cust_id = c.cust_id
GROUP BY age_group ORDER BY age_group;
"""
df_age_default = pd.read_sql(sql_age_default,engine)
df_age_default['default_rate'] = df_age_default['default_rate'].apply(lambda x : f'{x:.2%}')
df_age_default['avg_loan_amount'] = df_age_default['avg_loan_amount'].apply(lambda x : f'{x / 10000:.1f}万元')
print('\n---------按年龄分类----------')
print(df_age_default)

# 月度放款趋势
sql_month_default = '''
SELECT
    DATE_FORMAT(issue_date,'%%Y-%%m') AS month,
    SUM(loan_amount) AS month_amount
FROM loan_info GROUP BY month ORDER BY month;
'''

df_month_default = pd.read_sql(sql_month_default,engine)
df_month_default['mouth_amount'] = df_month_default['month_amount'].apply(lambda x : f'{x / 10000:.2f}万元')
print('\n---------月度放款趋势----------')
print(df_month_default)

# 可视化
plt.figure(figsize=(16,10))

#各类型放款总额
plt.subplot(2,2,1)
x = df_type_default['loan_type']
y = df_type_default['total_loan_amount']
plt.bar(x,y)
plt.xlabel('贷款类型')
plt.ylabel('贷款金额')
plt.title('各类型放款总额')

# 客户等级违约率
plt.subplot(2,2,2)
x = df_level_default['cust_level']
y = df_level_default['default_rate'].str.strip('%').astype(float)
plt.bar(x,y)
plt.ylim(bottom=0)
plt.xlabel('客户等级')
plt.ylabel('违约率')
plt.title('各等级违约率')

# 年龄段违约率
plt.subplot(2,2,3)
x = df_age_default['age_group']
y = df_age_default['default_rate'].str.strip('%').astype(float)
plt.bar(x,y)
plt.xlabel('客户年龄段')
plt.ylabel('违约率')
plt.title('各年龄段违约率')

# 月度放款走势
plt.subplot(2,2,4)
x = df_month_default['month']
y = df_month_default['month_amount']
plt.plot(x,y,marker="o")
plt.xlabel('月份')
plt.ylabel('放款金额(万元)')
plt.title('月度放款走势')
plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig('data/mysql银行信贷分析图.png',dpi=300)
plt.show()
