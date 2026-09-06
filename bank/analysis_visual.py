import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas import to_datetime

from user_data_generate import loan_df

full_df = pd.read_csv('data/full_info.csv')

plt.rcParams["font.family"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False

plt.figure(figsize=(15, 10))

# 图一：各贷款类型放款金额柱状图
plt.subplot(2,2,1)
amount_by_type = full_df.groupby('loan_type')['loan_amount'].sum() / 10000
x = amount_by_type.index
y = amount_by_type.values
plt.bar(x,y)
plt.title('各贷款类型放款金额（万元）')
plt.xlabel('放款类型')
plt.ylabel('放款金额（万元）')

# 图二：各客户等级违约率对比柱状图
plt.subplot(2,2,2)
default_by_type = full_df.groupby('cust_level')['is_default'].mean() * 100
x = default_by_type.index
y = default_by_type.values
plt.bar(x,y)
plt.title('各客户等级违约率对比')
plt.xlabel('客户等级类型')
plt.ylabel('违约率（%）')


# 图3：贷款金额分布直方图
plt.subplot(2,2,3)
x = full_df['loan_amount']/10000
plt.hist(x,bins=15)
plt.title('贷款金额分布')
plt.xlabel('贷款金额（万元）')
plt.ylabel('贷款笔数')

# 图4：月度放款金额趋势
plt.subplot(2,2,4)
# 按照月度聚合
full_df['issue_mouth'] = pd.to_datetime(loan_df['issue_date']).dt.to_period('M')
mouth_loan = full_df.groupby('issue_mouth')['loan_amount'].sum() / 10000
x = mouth_loan.index.astype(str)
y = mouth_loan.values
plt.plot(x,y, marker="o")
plt.title('月度放款金额趋势（万元）')
plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig('data/银行贷款分析图表.png',dpi=300,bbox_inches='tight')
plt.show()