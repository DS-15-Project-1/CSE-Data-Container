FROM python:3.12-slim

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory
WORKDIR /app

# Install additional system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install additional Python packages
RUN pip install \
    obspy \
    pandas \
    numpy \
    pyarrow

# Set the default command to run when starting the container
CMD ["sh", "-c", "nginx"]
