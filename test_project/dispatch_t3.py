# Task 3: Medium CPU - numpy matrix operations (~12s)
import numpy as np, time, json, os
start = time.time()
os.makedirs("output", exist_ok=True)
rng = np.random.default_rng(42)
a = rng.random((3000, 3000))
b = rng.random((3000, 3000))
c = a @ b
diag = np.diag(c)
result = {"task": 3, "diag_sum": float(np.sum(diag)), "shape": c.shape, "time": f"{time.time()-start:.2f}s"}
with open("output/t3_result.json", "w") as f:
    json.dump(result, f)
print(f"task3: 3000x3000 matmul done in {time.time()-start:.2f}s, diag_sum={result['diag_sum']:.1f}")
