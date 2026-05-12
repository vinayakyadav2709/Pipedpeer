# Task 6: Quick pandas data processing + output (~5s)
import pandas as pd, time, os
start = time.time()
os.makedirs("output", exist_ok=True)
df = pd.DataFrame({"x": range(100000), "y": [i**0.5 for i in range(100000)], "cat": ["A","B","C","D"]*25000})
summary = df.groupby("cat").agg({"x": ["count","mean"], "y": ["sum","max"]})
summary.to_csv("output/t6_summary.csv")
print(f"task6: processed 100k rows, saved summary.csv, done in {time.time()-start:.2f}s")
