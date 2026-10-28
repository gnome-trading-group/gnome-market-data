import os
import sys

# The handlers import the common layer's modules by name, as the Lambda runtime puts the layer on the path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'layers', 'common', 'python'))
sys.dont_write_bytecode = True
