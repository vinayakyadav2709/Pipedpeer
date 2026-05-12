# Task 7: Memory + matplotlib plot output (~3GB, ~12s)
import numpy as np, time, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
start = time.time()
os.makedirs("output", exist_ok=True)
x = np.linspace(0, 100, 10000)
y = np.sin(x) * np.exp(-x/20) + np.random.normal(0, 0.02, 10000)
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
axes[0,0].plot(x, y, linewidth=0.5); axes[0,0].set_title("Damped Sine")
axes[0,1].hist(y, bins=100); axes[0,1].set_title("Distribution")
axes[1,0].scatter(x[:1000], y[:1000], s=1, alpha=0.5); axes[1,0].set_title("Scatter")
axes[1,1].specgram(y, NFFT=256, Fs=100); axes[1,1].set_title("Spectrogram")
plt.tight_layout()
plt.savefig("output/t7_plots.png", dpi=150)
plt.close()
size = os.path.getsize("output/t7_plots.png")
print(f"task7: 4-panel plot saved ({size} bytes), done in {time.time()-start:.2f}s")
