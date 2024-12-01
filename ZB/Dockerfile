FROM continuumio/miniconda3

WORKDIR /app

# Copy environment file
COPY environment.yml .

# Create the environment
RUN conda env create -f environment.yml

# Activate the environment
SHELL ["conda", "run", "-n", "myenv", "/bin/bash", "-c"]

# Install additional packages via conda
RUN conda install -c conda-forge pyarrow pandas numpy scipy matplotlib -y

# Copy entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Copy the conversion script
COPY mseed_parquet_ZB.py /app/mseed_parquet_ZB.py

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
