FROM python:3.12

# Install System dependencies
RUN apt-get update && apt-get install -y \
    wget \
    && rm -rf /var/lib/apt/lists/*

# RUN wget https://ziglang.org/builds/zig-linux-x86_64-0.9.1-dev.3389+fddbd6bfb.tar.xz | tar xJ -C /usr/local --strip-components 1

# # Copy Zig source files
# COPY seismic_processing.zig /
# COPY setup.py /

# # Build Zig library
# RUN zig build-lib -dynamic -fPIC -O ReleaseSafe seismic_processing.zig

# Install Python dependencies
RUN pip install --no-cache-dir paramiko obspy pandas pyarrow tqdm

WORKDIR /app

# Copy chosen script file  
COPY mseed_parquet_local.py /app/mseed_parquet_local.py

CMD ["python", "mseed_parquet_local.py"]

