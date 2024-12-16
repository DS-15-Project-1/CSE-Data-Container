# Co2 Sequestration Seismic Data_Extraction

This project aims to facilitate the extraction, processing, and analysis of CO2 sequestration data. It provides tools to manage large datasets and offers a streamlined workflow for the task.

## Setup

1. Clone the repository
2. Run `docker-compose up --build`
3. Clone the repository to your local machine using the command:
4. 
   ```bash
   git clone https://github.com/DS-15-Project-1/Co2-Sec-Extraction-Container.git
   ```

## Using the Container

1. Dockerfile: Contains the instructions for building the Docker image.
2. docker-compose.yml: Defines the services and configuration for running the container.
3. data: /mnt/*
4. .gitignore: Specifies files and directories that Git should ignore.

1. Read miniSEED files from the `/mnt/data/SWP_Database_Container` directory
2. Process the data using ObsPy and PyArrow
3. Save the processed data as a CSV file in the `/mnt/code/output` directory

To use this setup:

1. Set mount point on server at `/mnt/` directory.
2. Run `docker-compose up --build` to start the container.
3. The container will read the miniSEED file, convert it to a parquet file, and save it in the desired output directory.
