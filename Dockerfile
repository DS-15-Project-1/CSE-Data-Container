FROM continuumio/miniconda3

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory
WORKDIR /app

# Update conda and install necessary packages
RUN conda update -n base -c defaults conda && \
    conda install -c conda-forge obspy pandas pyarrow numpy matplotlib scipy -y && \
    conda clean --all -y

# Install additional packages via pip
RUN pip install --no-binary=:all: wheel setuptools

# Install gcc and other build dependencies
RUN apt-get update && apt-get install -y gcc g++ && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy the conversion script
COPY mseed_parquet_ZZ.py /app/mseed_parquet_ZZ.py
COPY mseed_parquet_ZB.py /app/mseed_parquet_ZB.py


# Set the default command to run when starting the container
CMD ["python", "/app/mseed_parquet_ZB.py"]
