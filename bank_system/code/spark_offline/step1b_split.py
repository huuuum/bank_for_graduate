# 划分数据集

import pandas as pd
from sklearn.model_selection import train_test_split
from pathlib import Path

# 定位项目根目录 + 数据路径
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_PATH = str(BASE_DIR / "data" / "train.csv")

# 输出路径
TRAIN_70 = str(BASE_DIR / "output" / "train_70.csv")
TEST_20 = str(BASE_DIR / "output" / "test_20.csv")
STREAM_10 = str(BASE_DIR / "output" / "stream_10.csv")

RANDOM_STATE = 42

df = pd.read_csv(DATA_PATH)
print(f"原始数据：{df.shape[0]}条")

# 一次划分
train_70,rest_30 = train_test_split(
    df,
    test_size=0.3,
    random_state=RANDOM_STATE,
    stratify=df["subscribe"]
)

# 二次划分
test_20, stream_10 = train_test_split(
    rest_30,
    test_size=1/3,                # 30% × 1/3 = 10%
    random_state=RANDOM_STATE,
    stratify=rest_30["subscribe"],
)

train_70.to_csv(TRAIN_70, index=False)
test_20.to_csv(TEST_20, index=False)
stream_10.to_csv(STREAM_10, index=False)

def show(name,df):
    n = df.shape[0]
    yes = (df["subscribe"] == "yes").sum()
    no = (df["subscribe"] == "no").sum()
    print(f"{name:10s}: {n:>5d} 条   yes={yes:>4d}({yes / n * 100:.2f}%)   no={no:>5d}({no / n * 100:.2f}%)")

print("="*60)
print("划分结果")
show("train_70", train_70)
show("test_20", test_20)
show("stream_10", stream_10)
print(f"\n合计：{train_70.shape[0] + test_20.shape[0] + stream_10.shape[0]} 条（应等于 {df.shape[0]}）")
print(f"三段 yes 占比应都约为 {(df['subscribe']=='yes').mean()*100:.2f}%")