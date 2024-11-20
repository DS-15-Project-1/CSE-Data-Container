FROM ghcr.io/dask/dask

# Set the working directory
WORKDIR /app

# Upgrade pip and install wheel
RUN python -m pip install --upgrade pip wheel setuptools

RUN pip install obspy dask[complete] pandas numpy

# Copy Nginx configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Set up Nginx to run as a non-root user
RUN useradd -m nginx

RUN mkdir -p /var/log/nginx \
    && chown -R nginx:nginx /var/log/nginx
COPY app/data /app/data

ENV PATH="/usr/local/bin:${PATH}"
ENV PATH=/opt/conda/bin:$PATH

RUN echo 'export PATH=/opt/conda/bin:$PATH' >> ~/.bashrc

# Set the default command to run when starting the container
CMD ["dask-scheduler", "--host", "0.0.0.0", "--port", "8786"]
