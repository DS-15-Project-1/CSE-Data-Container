FROM python:3.12-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir \
    obspy \
    pandas \
    numpy \
    scipy \
    matplotlib \
    psutil

CMD ["python", "main.py"]
