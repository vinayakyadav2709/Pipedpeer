import json, os

result = {"task": "task3", "values": [1, 2, 3, 4], "sum": sum([1, 2, 3, 4])}
os.makedirs("output", exist_ok=True)
with open("output/task3.json", "w") as f:
    json.dump(result, f)
print(f"task3: sum={result['sum']}")
print("task3: saved output/task3.json")
