FROM python:3.8-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir obspy pyarrow pandas numpy scipy matplotlib wheel setuptools psutil

RUN pip install --upgrade obspy

# Copy entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Copy the conversion script
COPY mseed_parquet_ZZ.py /app/mseed_parquet_ZZ.py

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
