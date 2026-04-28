import pickle, os

data = {"msg": "hello from task2", "count": 42}
os.makedirs("output", exist_ok=True)
with open("output/task2.pkl", "wb") as f:
    pickle.dump(data, f)
print("task2: saved output/task2.pkl")
