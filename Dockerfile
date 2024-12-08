FROM python:3.12

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gcc \
    libc-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    paramiko \
    obspy \
    pandas \
    pyarrow \
    fastparquet \
    tqdm \
    numpy \
    ctypes

# Install Zig compiler
RUN wget https://ziglang.org/download/0.10.1/zig-linux-x86_64-0.10.1.tar.xz && \
    tar xf zig-linux-x86_64-0.10.1.tar.xz && \
    mv zig-linux-x86_64-0.10.1 /usr/local/zig && \
    rm zig-linux-x86_64-0.10.1.tar.xz

ENV PATH="/usr/local/zig:$PATH"

COPY . .

# Compile Zig code
RUN zig build-exe -O ReleaseFast seismic_processing.zig -lstdc++

CMD ["python", "mseed_parquet_local.py"]