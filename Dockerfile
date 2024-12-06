FROM python:3.12

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    paramiko \
    obspy \
    pandas \
    pyarrow \
    fastparquet \
    tqdm \
    numpy

COPY . .

CMD ["python", "mseed_parquet_local.py"]