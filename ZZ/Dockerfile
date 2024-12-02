FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

RUN pip install --no-cache-dir \
    obspy \
    pyarrow \
    pandas \
    numpy \
    scipy \
    matplotlib \
    psutil

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

COPY mseed_parquet_ZZ.py /app/mseed_parquet_ZZ.py

CMD ["/app/entrypoint.sh"]