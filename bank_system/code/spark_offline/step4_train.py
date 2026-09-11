import numpy as np
import joblib
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (accuracy_score, precision_score, recall_score,
                             f1_score, confusion_matrix, classification_report)

# 定位目录
BASE_DIR = Path(__file__).resolve().parents[2]
FEATURES_PATH = str(BASE_DIR / "output" / "features.npz")
LR_MODEL_PATH = str(BASE_DIR / "models" / "lr_model.joblib")
RF_MODEL_PATH = str(BASE_DIR / "models" / "rf_model.joblib")
EVAL_PATH = str(BASE_DIR / "output" / "evaluation_report.txt")

# 加载数据
data = np.load(FEATURES_PATH)
X , y = data["X"], data["y"]
print("="*60)
print(f"特征矩阵:X.shape {X.shape}, y.shape {y.shape}")

# 划分数据集
# stratify用于平衡训练集中的yes和no比例
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42,stratify=y)
print("="*60)
print(f"训练集数量:{X_train.shape[0]}条,测试集数量:{X_test.shape[0]}")
print(f"训练集正样本数:{y_train.mean():.2f},测试集正样本数:{y_test.mean():.2f}")

# 逻辑回归
lr = LogisticRegression(class_weight="balanced",random_state=42,max_iter=1000)
lr.fit(X_train,y_train)

# 随机森林
rf = RandomForestClassifier(class_weight="balanced",random_state=42,n_estimators=100)
rf.fit(X_train,y_train)

# 评估
def evaluate(name , model):
    y_pred = model.predict(X_test)
    return {
        "name": name,
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred),
        "cm": confusion_matrix(y_test, y_pred),    #混淆矩阵
        "report": classification_report(y_test, y_pred,target_names=["no","yes"]),
    }

results = [evaluate("逻辑回归" , lr),evaluate("随机森林",rf)]

# 评估结果展示
for r in results:
    print("="*60)
    print(f"【{r["name"]}】")
    print(f"准确率{r["accuracy"]*100:.2f}%")
    print(f"精确率{r["precision"]*100:.2f}%")
    print(f"召回率{r["recall"]*100:.2f}%")
    print(f"f1分数{r["f1"]*100:.2f}%")
    print("混淆矩阵:")
    print(r["cm"])
    print(r["report"])

# 保存模型
joblib.dump(lr, LR_MODEL_PATH)
joblib.dump(rf, RF_MODEL_PATH)
print("="*60)
print(f"模型已保存至{LR_MODEL_PATH}与{RF_MODEL_PATH}")

print("="*60)
# 根据f1分数来决定更好的模型
best_model = max(results, key=lambda r:r["f1"])
# 保存最好的模型
with open(EVAL_PATH, "w" ,encoding="utf-8") as f:
    f.write("模型评估报告:\n")
    for r in results:
        f.write("=" * 60+"\n")
        f.write(f"【{r["name"]}】\n")
        f.write(f"准确率{r["accuracy"] * 100:.2f}%\n")
        f.write(f"精确率{r["precision"] * 100:.2f}%\n")
        f.write(f"召回率{r["recall"] * 100:.2f}%\n")
        f.write(f"f1分数{r["f1"] * 100:.2f}%\n")
        f.write("混淆矩阵:\n" + str(r["cm"]) + "\n")
        f.write(r["report"])
    f.write(f"\n最佳模型（按 F1）: {best_model['name']}\n")
print("评估报告已保存:", EVAL_PATH)