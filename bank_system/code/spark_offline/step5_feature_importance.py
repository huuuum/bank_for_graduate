import json
import joblib
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
RF_MODEL_PATH = str(BASE_DIR /  "models" / "rf_model.joblib")
FEATURE_CONFIG_PATH = str(BASE_DIR / "models" / "config.json")
IMPORTANCE_PATH = str(BASE_DIR / "output" / "feature_importance.txt")

# 加载模型和配置特征
rf = joblib.load(RF_MODEL_PATH)
with open(FEATURE_CONFIG_PATH, encoding="utf-8") as f:
    config = json.load(f)

# 特征名称和重要性分数
feature_names = config["feature_order"]
importances = rf.feature_importances_

# 将重要性和特征绑定并返回一个新的列表
pairs = sorted(zip(feature_names, importances),key=lambda x: x[1], reverse=True)

# 打印贡献
print("="*60)
print("【随机森林特征重要性排名】")
cumsum = 0.0
for rank,(name,imp) in enumerate(pairs,1):
    cumsum += imp
    print(f"{rank:2d}. {name:18s}重要性{imp:.2f}累计贡献{cumsum:.2f}")

# 写入文件
with open(IMPORTANCE_PATH, "w", encoding="utf-8") as f:
    f.write("【随机森林特征重要性排名】\n")
    f.write("="*60+"\n")
    cumsum = 0.0
    for rank,(name,imp) in enumerate(pairs,1):
        cumsum += imp
        f.write(f"{rank:2d}. {name:18s}重要性{imp:.2f}累计贡献{cumsum:.2f}")
print("="*60)
print("文件已保存至:"+IMPORTANCE_PATH)