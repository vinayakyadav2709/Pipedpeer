# Task 4: Memory heavy (~4GB) numpy array + processing (~18s)
import numpy as np, time, json, os
start = time.time()
os.makedirs("output", exist_ok=True)
size = 9000  # 9000*9000*8 bytes * 2 arrays ≈ 1.3GB actual + overhead ~4GB for 3 arrays
print("task4: allocating arrays...")
a = np.ones((size, size//4), dtype=np.float64)
b = np.ones((size//4, size), dtype=np.float64)
print("task4: computing matmul...")
c = a @ b
mean_val = float(np.mean(c))
peak_mem = c.nbytes + a.nbytes + b.nbytes
with open("output/t4_mem.json", "w") as f:
    json.dump({"task": 4, "peak_alloc_bytes": peak_mem, "mean": mean_val, "time": f"{time.time()-start:.2f}s"}, f)
print(f"task4: peak alloc {peak_mem/1e9:.1f}GB, mean={mean_val:.2f}, done in {time.time()-start:.2f}s")
