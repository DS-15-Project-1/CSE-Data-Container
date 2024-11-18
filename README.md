# Co2 Sequestration Seismic Data_Extraction

This project will
This project aims to facilitate the extraction, processing, and analysis of CO2 sequestration data. It provides tools to manage large datasets and offers a streamlined workflow for data scientists and engineers working in carbon capture and storage.

## Setup

1. Clone the repository
2. Run `docker-compose up --build`
3. Clone the repository to your local machine using the command:
   ```bash
   git clone <repository-url>
   ```
4. Navigate to the project directory.
5. Run `docker-compose up --build` to set up the necessary environment and services.

## Usage

1. Access the processed data at `http://localhost:8080/processed_data.h5`
2. Use Dask on your client machine to read the data:

3. Once the setup is complete, you can access the processed data at:
   ```plaintext
   http://localhost:8080/processed_data.h5
   ```
4. Use Dask on your client machine to read and interact with the processed data. Dask enables parallel computing and offers a scalable solution for handling large datasets, improving data processing efficiency.
