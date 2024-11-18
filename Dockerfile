FROM python:3.9-slim

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ARG DEBIAN_FRONTEND=noninteractive

# Set the working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    git \
    build-essential \
    libssl3 \
    libgl1-mesa-glx \
    libqt5gui5 \
    libnotify4 \
    libnss3 \
    xdg-utils \
    x11-apps \
    xvfb \
    nginx \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install wheel
RUN python -m pip install --upgrade pip wheel setuptools

# Install necessary Python packages
RUN pip install \
    obspy \
    dask \
    pandas \
    numpy

# Copy Nginx configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Set up Nginx to run as a non-root user
RUN useradd -m nginx
RUN chown -R nginx:nginx /var/cache/nginx
RUN chown -R nginx:nginx /var/log/nginx

# Set the default command to run when starting the container
CMD ["sh", "-c", "nginx && python -m dask.distributed --nprocs 4 --nthreads 2 --memory-limit 4GB --interface 0.0.0.0"]
