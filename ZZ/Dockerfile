FROM continuumio/miniconda3

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

RUN conda update -n base -c defaults conda && \
    conda install -c conda-forge obspy pandas pyarrow numpy matplotlib scipy -y && \
    conda clean --all -y

RUN pip install --no-binary=:all: wheel setuptools

RUN apt-get update && apt-get install -y gcc g++ && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY main.py .

CMD ["python", "main.py"]
