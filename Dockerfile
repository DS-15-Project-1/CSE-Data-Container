FROM ghcr.io/dask/dask:latest

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory
WORKDIR /app

# Copy the src directory
COPY src/ /app/src/

# Install additional system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    git \
    nginx \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install additional Python packages
RUN pip install \
    obspy \
    pandas \
    numpy

# Copy Nginx configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Set up Nginx to run as a non-root user
RUN useradd -m nginx
RUN mkdir -p /var/cache/nginx && chown -R nginx:nginx /var/cache/nginx
RUN chown -R nginx:nginx /var/log/nginx

# Set the default command to run when starting the container
CMD ["sh", "-c", "nginx && dask-scheduler --host 0.0.0.0 --port 8786"]
