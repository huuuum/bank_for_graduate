import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

from user_data_generate import engine

# 基础数据探索
cust_df = pd.read_csv('data/cust_info.csv')
loan_df = pd.read_csv('data/loan_info.csv')

print('用户表前五行：\n',cust_df.head())
print('用户表基础信息：\n',cust_df.info())

print('贷款表前五行：\n',loan_df.head())
print('贷款表基础信息：\n',loan_df.info())
print('贷款表数值字段统计：\n',loan_df[["loan_amount", "overdue_days", "is_default"]].describe())

default_rate = loan_df['is_default'].mean()
print(f'\n贷款违约率：{default_rate:.2%}')

# 重复值处理
print('用户表缺失值数量：',cust_df.duplicated().sum())
print('贷款表缺失值数量：',loan_df.duplicated().sum())
print('用户表id缺失值数量：',cust_df.duplicated(subset=['cust_id']).sum())
print('贷款表id缺失值数量：',loan_df.duplicated(subset=['loan_id']).sum())

cust_df = cust_df.drop_duplicates(keep='first')
loan_df = loan_df.drop_duplicates(keep='first')
cust_df = cust_df.drop_duplicates(subset=['cust_id'],keep='first')
loan_df = loan_df.drop_duplicates(subset=['loan_id'],keep='first')


# 缺失值处理
print('用户表缺失值：',cust_df.isna().sum())
cust_df = cust_df.dropna(subset=['cust_id'])
cust_df['education'] = cust_df['education'].fillna('未知')
cust_df['cust_level'] = cust_df['cust_level'].fillna('未知')
cust_df['age'] = cust_df['age'].fillna('未知')
cust_df['register_years'] = cust_df['register_years'].fillna('未知')

print('贷款表缺失值：',loan_df.isna().sum())
loan_df = loan_df.dropna(subset=['loan_id'])
loan_df['loan_amount'] = loan_df['loan_amount'].fillna('未知')
loan_df['overdue_days'] = loan_df['overdue_days'].fillna('未知')
loan_df['loan_term'] = loan_df['loan_term'].fillna('未知')
loan_df['loan_type'] = loan_df['loan_type'].fillna('未知')
loan_df['issue_date'] = loan_df['issue_date'].fillna('未知')

# 异常值处理
loan_df = loan_df[loan_df["loan_amount"] > 0 ]
loan_df = loan_df[loan_df["overdue_days"] >= 0 ]

# 合并两表
full_df = pd.merge(loan_df,cust_df,on='cust_id',how='left')

# 按照学历中位值,填补收入缺失值
full_df['income'] = full_df.groupby('education')['income'].transform(lambda x : x.fillna(x.median()))
# transform函数要求输出结果长度和原表长度一致，适合缺失值的补充。apply输出长度没有限制

# 年龄分箱
full_df['age_group'] = pd.cut(
    full_df['age'],
    bins=[18,25,35,45,55,65,75],
    labels=['18-25岁','26-35岁','36-45岁','46-55岁','56-65岁','66-75岁']
)
full_df = full_df.dropna(subset=['age_group'])

full_df.to_csv('data/full_info.csv',index=False,encoding='utf-8-sig')

user = 'root'
password = 'okb13950958240'
port = 3306
database = 'bank_data'
host = '127.0.0.1'

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4')
full_df.to_sql('full_info',engine,if_exists='replace',index=False)
