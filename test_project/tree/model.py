import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score


def load_data(csv_path):
    """Load a 2-column CSV (feature, target) and return X, y."""
    df = pd.read_csv(csv_path)
    cols = df.columns.tolist()
    X = df[[cols[0]]]
    y = df[cols[1]]
    return X, y


def train_tree(X, y):
    """Train a DecisionTreeClassifier and return model + accuracy."""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )
    model = DecisionTreeClassifier(random_state=42)
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    acc = accuracy_score(y_test, predictions)
    return model, acc
