import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

import np_d as np

r = np.matmul([[1,0],[0,1]],[[1,0],[0,1]])
print(r)