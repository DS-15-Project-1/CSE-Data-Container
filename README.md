# Co2 Sequestration Seismic Data_Extraction

This project aims to facilitate the extraction, processing, and analysis of CO2 sequestration data. It provides tools to manage large datasets and offers a streamlined workflow for the task.

## Setup

1. Clone the repository
2. Run `docker-compose up --build`
3. Clone the repository to your local machine using the command:
   ```bash
   git clone https://github.com/DS-15-Project-1/Co2-Sec-Extraction-Container.git
   ```

## Using the Container

1. Dockerfile: Contains the instructions for building the Docker image.

2. docker-compose.yml: Defines the services and configuration for running the container.

3. src/main.py: The main Python script that reads miniSEED files and processes them.

4. nginx/nginx.conf: Configuration file for the Nginx server.

5. data:(TODO) needs specification.

6. .gitignore: Specifies files and directories that Git should ignore.

7. README.md: Project documentation.

This structure provides a streamlined environment for reading miniSEED files with ObsPy, processing them with Dask, and serving the results through Nginx. The container will:

1. Read miniSEED files from the `/app/data/` directory
2. Process the data using ObsPy and Dask
3. Save the processed data as a CSV file in the `/app/data/` directory
4. Serve the CSV file through Nginx

To use this setup:

1. Set mount point on server at `data/` directory.
2. Run `docker-compose up --build` to start the container.
3. The container will read the miniSEED file, convert it to a CSV, and save it in the `/app/data/` directory.
4. You can access the CSV file through the Nginx server at `http://localhost/processed_data.csv`. (TODO needs specification)
