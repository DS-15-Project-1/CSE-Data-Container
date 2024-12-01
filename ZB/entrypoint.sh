#!/bin/bash
set -e

# Print environment information
echo "Current directory: $(pwd)"
echo "Contents of current directory:"
ls -la
echo "Python version:"
python --version
echo "PyArrow version:"
python -c "import pyarrow; print(pyarrow.__version__)"

# Activate the Conda environment
