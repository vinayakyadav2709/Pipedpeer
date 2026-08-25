"""The compute kernel, in its own module so it is importable by name on a
worker. A function defined in the submitting script's __main__ is pickled as
__main__.work, which does not resolve on the worker."""


def work(i):
    acc = 0.0
    for k in range(1, 3000000):
        acc += (i + k) ** 0.5
    return acc
