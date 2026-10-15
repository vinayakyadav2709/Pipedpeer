import os
import sys

# The shim dir is what a job gets on PYTHONPATH; put it first here too.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
