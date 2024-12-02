FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir \
    obspy \
    pyarrow \
    pandas \
    numpy \
    scipy \
    matplotlib \
    psutil

CMD ["python", "main.py"]
