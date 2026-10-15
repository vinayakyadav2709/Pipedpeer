# Pool kernels ship as source first, by value with cloudpickle second

Stock `multiprocessing` sends functions by name, which a remote worker cannot resolve. The shim sends the function's source (`361fb7c`); lambdas, closures, bound methods and partials that have no shippable source go by value with cloudpickle, which every generated environment now includes (`13b906b`, `837f9c2`). The header carries exactly one form, because the runner prefers `func_src` and would silently ignore the other (`func_src` in `_ClusterPool`, `src/internal/nixgen/shim/pool.py`).
