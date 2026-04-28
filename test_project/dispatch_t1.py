# Task 1: Instant - requests + pyyaml output (~2s)
import requests, yaml, os, time
start = time.time()
os.makedirs("output", exist_ok=True)
try:
    r = requests.get("https://httpbin.org/get", timeout=10)
    status = r.status_code
except Exception:
    status = "offline (expected)"
with open("output/t1_status.yaml", "w") as f:
    yaml.dump({"task": 1, "status": str(status), "time": f"{time.time()-start:.2f}s"}, f)
print(f"task1: requests status={status}")
