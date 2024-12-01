FROM continuumio/miniconda3

WORKDIR /app

# Copy environment file
COPY environment.yml .

# Create the environment
RUN conda env create -f environment.yml

RUN pip install --upgrade pyarrow

# Copy entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Copy the conversion script
COPY mseed_parquet_ZZ.py /app/mseed_parquet_ZB.py

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
