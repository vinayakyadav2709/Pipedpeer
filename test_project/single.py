import os
import pickle
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

def main():
    # Create sample data
    df = pd.DataFrame({"feature": [1, 2, 3, 4, 5, 6], "target": [0, 0, 0, 1, 1, 1]})
    X = df[["feature"]]
    y = df["target"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    model = DecisionTreeClassifier(random_state=42)
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    acc = accuracy_score(y_test, predictions)

    print(f"Accuracy: {acc:.4f}")

    model_dir = os.path.join(os.path.dirname(__file__), "models")
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, "model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    print(f"Model saved to: {model_path}")

    loaded = pickle.load(open(model_path, "rb"))
    print(f"Verification: loaded model type = {type(loaded).__name__}")

if __name__ == "__main__":
    main()
