# Start with a Python base image
FROM python:3.7-slim

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    git \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install wheel
RUN python -m pip install --upgrade pip wheel setuptools

# Install required Python packages
RUN pip install --no-cache-dir \
    obspy \
    pandas \
    pyarrow

#Install tensorflow version that works with host computer specs
RUN pip install tensorflow==1.15

# Copy the conversion script
COPY mseed_parquet.py /app/mseed_parquet.py

# Set the default command to run when starting the container
CMD ["python", "/app/mseed_parquet.py"]
