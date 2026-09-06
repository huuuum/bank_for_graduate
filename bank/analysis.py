from cProfile import label

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

full_df = pd.read_csv('data/full_info.csv')

# 根据贷款类型分类分析
loan_types_analysis = full_df.groupby('loan_type').agg(
    贷款笔数=('loan_id','count'),
    总放款金额=('loan_amount','sum'),
    平均贷款金额=('loan_amount','mean'),
    违约笔数=('is_default','sum'),
    违约率=('is_default','mean'),
).reset_index()

# 美化表格显示
loan_types_analysis['总放款金额'] = loan_types_analysis['总放款金额'].apply(lambda x : f"{x/10000:.1f}万")
loan_types_analysis['平均贷款金额'] = loan_types_analysis['平均贷款金额'].apply(lambda x : f"{x/10000:.1f}万")
loan_types_analysis['违约率'] = loan_types_analysis['违约率'].apply(lambda x : f"{x:.2%}")

print(loan_types_analysis)

# 年龄分箱
full_df['age_group'] = pd.cut(
    full_df['age'],
    bins=[18,25,35,45,55,65,75],
    labels=['18-25岁','26-35岁','36-45岁','46-55岁','56-65岁','66-75岁']
)

# 按照年龄分类分析
cust_age_analysis = full_df.groupby('age_group').agg(
    贷款笔数=('loan_id','count'),
    违约率=('is_default','mean'),
    平均收入=('income','mean')
).reset_index()

cust_age_analysis['违约率'] = cust_age_analysis['违约率'].apply(lambda x:f"{x:.2%}")
cust_age_analysis['平均收入'] = cust_age_analysis['平均收入'].apply(lambda x:f"{x/10000:.2f}万")

print(cust_age_analysis)

# 按照客户等级统计违约情况
cust_level_analysis = full_df.groupby('cust_level').agg(
    客户数=('cust_id','count'),
    贷款笔数=('cust_id','count'),
    违约率=('is_default','mean'),
    平均贷款金额=('loan_amount','mean'),
).reset_index().sort_values('违约率',ascending=False)

cust_level_analysis['违约率'] = cust_level_analysis['违约率'].apply(lambda x:f"{x:.2%}")
cust_level_analysis['平均贷款金额'] = cust_level_analysis['平均贷款金额'].apply(lambda x:f"{x/10000:.2f}万")

print(cust_level_analysis)