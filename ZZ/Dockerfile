FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN python -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

ENV CFLAGS="-mno-avx2"
ENV CXXFLAGS="-mno-avx2"

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
RUN chmod 644 /app/mseed_parquet_ZZ.py

CMD ["strace", "-f", "/app/entrypoint.sh"]

