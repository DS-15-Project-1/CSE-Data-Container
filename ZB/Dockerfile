FROM continuumio/miniconda3

WORKDIR /app

# Copy environment file
COPY environment.yml .

# Create the environment
RUN conda env create -f environment.yml

# Activate the environment
SHELL ["conda", "run", "-n", "myenv", "/bin/bash", "-c"]

# Install packages via pip
RUN pip install --no-binary=:all: wheel setuptools && \
    pip install --no-binary=:all: pyarrow pandas numpy scipy matplotlib

# Copy entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Copy the conversion script
COPY mseed_parquet_ZZ.py /app/mseed_parquet_ZZ.py

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
