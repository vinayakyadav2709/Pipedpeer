# Task 10: Quick multi-lib - pydantic + click + yaml output (~3s)
from pydantic import BaseModel, Field
import yaml, click, time, os
start = time.time()
os.makedirs("output", exist_ok=True)
class TaskResult(BaseModel):
    task_id: int = Field(ge=1, le=100)
    worker: str
    status: str = "completed"
    metrics: dict = {}
r = TaskResult(task_id=10, worker="lab", metrics={"files": 3, "libs_used": ["pydantic","click","pyyaml","pillow"]})
with open("output/t10_manifest.yaml", "w") as f:
    yaml.dump(r.model_dump(), f)
print(f"task10: pydantic model validated, yaml saved, done in {time.time()-start:.2f}s")
