# Task 5: CPU moderate - scipy optimize + integrate (~20s)
import numpy as np, time, json, os
from scipy.optimize import minimize
from scipy.integrate import quad
start = time.time()
os.makedirs("output", exist_ok=True)
def rosen(x):
    return sum(100.0*(x[1:]-x[:-1]**2)**2 + (1-x[:-1])**2)
res = minimize(rosen, [1.3, 0.7, 0.8, 1.9, 1.2], method="Nelder-Mead")
ival, _ = quad(lambda x: np.sin(x**2), 0, 5)
with open("output/t5_scipy.json", "w") as f:
    json.dump({"task": 5, "rosenbrock_min": list(res.x), "quad_sin_x2": ival, "time": f"{time.time()-start:.2f}s"}, f)
print(f"task5: rosenbrock min at {res.x}, quad integral={ival:.4f}, done in {time.time()-start:.2f}s")
