#!/bin/bash
set -e

# Activate the Conda environment
source /opt/conda/etc/profile.d/conda.sh
conda activate myenv

# Run the Python script
exec python /app/mseed_parquet_ZBS.py
