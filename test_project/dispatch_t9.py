# Task 9: CPU heavy - numpy iterative computation (~25s)
import numpy as np, time, json, os
start = time.time()
os.makedirs("output", exist_ok=True)
rng = np.random.default_rng(123)
data = rng.random((2000, 2000))
result = data.copy()
for i in range(15):
    result = result @ data[:2000, :2000]
    result = np.clip(result, -100, 100)
trace = float(np.trace(result))
means = [float(np.mean(result, axis=j)[0]) for j in range(2)]
with open("output/t9_compute.json", "w") as f:
    json.dump({"task": 9, "trace": trace, "means": means, "iterations": 15, "time": f"{time.time()-start:.2f}s"}, f)
print(f"task9: 15 iterations of 2000x2000 matmul, trace={trace:.2f}, done in {time.time()-start:.2f}s")
