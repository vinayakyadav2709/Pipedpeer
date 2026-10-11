# Hyperparameter search for a random forest, 5-fold cross validation.
# n_jobs=-1 so sklearn uses every core.
import time

from sklearn.datasets import make_classification
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV

t0 = time.time()

X, y = make_classification(n_samples=5000, n_features=30, n_informative=12,
                           random_state=42)

params = {
    "n_estimators": [100, 200],
    "max_depth": [None, 10, 20],
    "min_samples_leaf": [1, 2, 4],
}

search = GridSearchCV(RandomForestClassifier(random_state=0), params, cv=5, n_jobs=-1)
print("fitting", 2 * 3 * 3 * 5, "models ...")
search.fit(X, y)

print("best params:", search.best_params_)
print("best score: %.4f" % search.best_score_)
print("TOTAL %.1fs" % (time.time() - t0))
