# Task 8: Medium sklearn classification on generated data (~18s)
import numpy as np, time, joblib, os
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score
start = time.time()
os.makedirs("output", exist_ok=True)
rng = np.random.default_rng(42)
X = rng.random((5000, 20))
y = (X[:, 0] + X[:, 1] + X[:, 2] > 1.5).astype(int)
model = RandomForestClassifier(n_estimators=50, max_depth=10, random_state=42)
scores = cross_val_score(model, X, y, cv=5)
model.fit(X, y)
joblib.dump(model, "output/t8_model.joblib")
print(f"task8: RF 50-trees, cv accuracy={scores.mean():.4f}+/-{scores.std():.4f}, done in {time.time()-start:.2f}s")
