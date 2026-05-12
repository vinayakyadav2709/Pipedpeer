# Task 2: Quick Pillow image manipulation (~3s)
from PIL import Image, ImageDraw
import os, time
start = time.time()
os.makedirs("output", exist_ok=True)
img = Image.new("RGB", (1920, 1080), "blue")
draw = ImageDraw.Draw(img)
for x in range(0, 1920, 40):
    draw.line([(x, 0), (x, 1080)], fill="white", width=2)
img.save("output/t2_grid.png")
size = os.path.getsize("output/t2_grid.png")
print(f"task2: generated grid image ({size} bytes) in {time.time()-start:.2f}s")
