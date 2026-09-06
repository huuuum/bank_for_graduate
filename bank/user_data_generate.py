from venv import create

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from numpy.lib import user_array
from sqlalchemy import create_engine


plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

np.random.seed(42)
# 用户数据生成
cust_count = 1000
cust_data = {
    "cust_id":[f"C{str(i).zfill(6)}" for i in range(1,cust_count+1)], #用户编号
    "age":np.random.randint(18,71,size=cust_count), #用户年龄
    "income":np.random.randint(100,10000,size=cust_count)*10, #用户收入
    "education":np.random.choice(['大专及以下','本科','研究生及以上'],size=cust_count,p=(0.6,0.3,0.1)), #用户学历
    "cust_level":np.random.choice(['普通','金卡','钻石'],size=cust_count,p=(0.7, 0.25, 0.05)), #用户等级
    "register_years":np.random.randint(2020,2026,size=cust_count), #用户开户年份
}
cust_df = pd.DataFrame(cust_data)
cust_df.to_csv('data/cust_info.csv',index=False,encoding='utf-8-sig')
print('用户信息表生成完毕')
print('用户表前十行数据：',cust_df.head(10))
print('用户表形状：',cust_df.shape)

# 贷款业务表生成
loan_count = 1200
loan_data = {
    "loan_id":[f"L{str(i).zfill(6)}"for i in range(1,loan_count+1)], #贷款编号
    "cust_id":np.random.choice(cust_df['cust_id'],size=loan_count), #关联用户编号
    "loan_amount":np.random.randint(1,500,size=loan_count)*10000, #贷款金额
    "loan_term":np.random.choice(['12','24','36','48'],size=loan_count), #贷款期限
    "loan_type":np.random.choice(['消费贷','经营贷','装修贷'],size=loan_count,p=(0.5,0.3,0.2)), #贷款类型
    "overdue_days":np.random.choice([0, 3, 10, 30, 60, 90],size=loan_count,p=(0.7, 0.1, 0.08, 0.06, 0.04, 0.02)), #逾期天数
    "issue_date":pd.date_range(start='2023-1-1',end='2024-12-31',periods=loan_count).strftime('%Y-%m-%d'), #放款日期
}
loan_df = pd.DataFrame(loan_data)

loan_df['is_default'] = loan_df['overdue_days'].apply(lambda x : 1 if x>=30 else 0) #判断是否违约
loan_df.to_csv('data/loan_info.csv',index=False,encoding='utf-8-sig')
print('贷款业务表生成完毕')
print('贷款表前十行数据：',cust_df.head(10))
print('贷款表形状：',cust_df.shape)

# 写入数据库
user = 'root'
password = 'okb13950958240'
host = 'localhost'
port = 3306
dbname = 'bank_data'

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}?charset=utf8mb4')

cust_df.to_sql('cust_info',engine,if_exists='replace',index=False)
loan_df.to_sql('loan_info',engine,if_exists='replace',index=False)

print('写入数据库完成')