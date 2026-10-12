# Blur a big image by cutting it into tiles and blurring each tile in parallel.
# Each tile carries a border (HALO) so the seams match the whole-image blur.
# Saves a small preview as blurred_preview.pgm.
import time
from multiprocessing import Pool

import numpy as np

SIZE = 2048     # image is SIZE x SIZE
TILE = 256      # 64 tiles
R = 10          # box blur radius (21x21 box)
PASSES = 10     # repeated box blur ~ gaussian
HALO = R * PASSES


def blur_tile(tile):
    out = tile
    k = (2 * R + 1) ** 2
    for _ in range(PASSES):
        h, w = out.shape
        acc = np.zeros((h - 2 * R, w - 2 * R), dtype=np.float32)
        for dy in range(2 * R + 1):
            for dx in range(2 * R + 1):
                acc += out[dy:dy + h - 2 * R, dx:dx + w - 2 * R]
        out = acc / k
    return out


if __name__ == "__main__":
    t0 = time.time()
    rng = np.random.default_rng(1)
    img = rng.random((SIZE, SIZE), dtype=np.float32)
    yy, xx = np.mgrid[0:SIZE, 0:SIZE]
    img += ((xx // 128 + yy // 128) % 2).astype(np.float32)  # checkerboard + noise
    padded = np.pad(img, HALO, mode="edge")

    tiles = []
    for y in range(0, SIZE, TILE):
        for x in range(0, SIZE, TILE):
            tiles.append(padded[y:y + TILE + 2 * HALO, x:x + TILE + 2 * HALO].copy())
    print("blurring", len(tiles), "tiles of", TILE, "x", TILE)

    with Pool() as pool:
        blurred = pool.map(blur_tile, tiles)

    per_row = SIZE // TILE
    rows = [np.hstack(blurred[i * per_row:(i + 1) * per_row]) for i in range(per_row)]
    result = np.vstack(rows)
    print("result", result.shape, "mean %.4f std %.4f" % (result.mean(), result.std()))

    small = result[::4, ::4]
    small = (255 * (small - small.min()) / (small.max() - small.min())).astype(np.uint8)
    with open("blurred_preview.pgm", "wb") as f:
        f.write(b"P5 %d %d 255\n" % (small.shape[1], small.shape[0]))
        f.write(small.tobytes())
    print("wrote blurred_preview.pgm")
    print("TOTAL %.1fs" % (time.time() - t0))
