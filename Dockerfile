FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

RUN python -m pip install --upgrade pip wheel setuptools

RUN pip install --no-cache-dir \
    obspy \
    pandas \
    pyarrow

COPY mseed_parquet.py /app/mseed_parquet.py

# Add a non-root user
RUN useradd -ms /bin/bash appuser
USER appuser

CMD ["python", "/app/mseed_parquet.py"]