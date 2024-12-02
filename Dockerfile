FROM python:3.12-slim

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory
WORKDIR /app

# Upgrade pip and install wheel
RUN python -m pip install --upgrade pip wheel setuptools

# Install required Python packages
RUN pip install --no-cache-dir \
    obspy \
    pandas \
    tensorflow \
    pyarrow

# Copy the conversion script
COPY mseed_parquet.py /app/mseed_parquet.py

# Set the default command to run when starting the container
CMD ["python", "/app/mseed_parquet.py"]
