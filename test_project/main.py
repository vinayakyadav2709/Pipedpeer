import os
import pickle
from tree.model import load_data, train_tree


def main():
    # Load data
    csv_path = os.path.join(os.path.dirname(__file__), "data", "data.csv")
    print(f"Loading data from: {csv_path}")
    X, y = load_data(csv_path)
    print(f"Loaded {len(X)} samples, {X.shape[1]} feature(s)")

    # Train
    model, accuracy = train_tree(X, y)
    print(f"Model: {model}")
    print(f"Accuracy: {accuracy:.4f}")

    # Save model
    model_dir = os.path.join(os.path.dirname(__file__), "models")
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, "model_file")
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    print(f"Model saved to: {model_path}")

    # Verify saved model exists
    if os.path.isfile(model_path):
        print("VERIFY: model file exists at expected location")
        loaded = pickle.load(open(model_path, "rb"))
        print(f"VERIFY: loaded model type = {type(loaded).__name__}")
    else:
        print("ERROR: model file NOT found!")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
