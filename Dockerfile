FROM python:3.12-bullseye

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libarchive13 \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip && \
    python -m pip install wheel

RUN python -m pip install \
    obspy \
    pandas \
    pyarrow \
    numpy \
    paramiko \
    tqdm 

COPY mseed_parquet.py /app/mseed_parquet.py

CMD ["python", "mseed_parquet.py"]