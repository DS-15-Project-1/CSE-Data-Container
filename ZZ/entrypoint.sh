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
echo "Pandas version:"
python -c "import pandas; print(pandas.__version__)"
echo "NumPy version:"
python -c "import numpy; print(numpy.__version__)"
echo "SciPy version:"
python -c "import scipy; print(scipy.__version__)"
echo "Matplotlib version:"
python -c "import matplotlib; print(matplotlib.__version__)"

# Run the Python script
exec python /app/mseed_parquet_ZZ.py
