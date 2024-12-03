import os
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import traceback
import gc
from tqdm import tqdm
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def convert_file_to_parquet(input_file, output_file):
    logger.info(f"Attempting to convert: {input_file}")
    
    try:
        # Get file size
        file_size = os.path.getsize(input_file)
        logger.info(f"File size: {file_size} bytes")
        
        if file_size > 1024 * 1024 * 1024:  # 1 GB limit
            logger.warning(f"File too large ({file_size} bytes), skipping: {input_file}")
            return
        
        # Read the file
        st = read(input_file, headonly=True)
        logger.info(f"Successfully read header of: {input_file}")
        
        # Extract metadata
        network = st[0].stats.network
        station = st[0].stats.station
        location = st[0].stats.location
        channel = st[0].stats.channel
        start_time = st[0].stats.starttime.isoformat()
        end_time = st[0].stats.endtime.isoformat()
        sampling_rate = st[0].stats.sampling_rate
        
        # Calculate chunk size
        chunk_size = int(3600 * sampling_rate)  # 1 hour of data, rounded to nearest integer
        
        # Read data in chunks
        for i in tqdm(range(0, int(len(st[0])), chunk_size), desc=f"Processing {input_file}"):
            chunk = st.slice(starttime=st[0].stats.starttime + i / sampling_rate,
                             endtime=st[0].stats.starttime + (i + chunk_size) / sampling_rate)
            
            # Create DataFrame
            df = pd.DataFrame({
                'network': [network],
                'station': [station],
                'location': [location],
                'channel': [channel],
                'starttime': [chunk.stats.starttime.isoformat()],
                'endtime': [chunk.stats.endtime.isoformat()],
                'sampling_rate': [sampling_rate],
                'data': [chunk.data]
            })
            
            # Convert DataFrame to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Check if output file exists
            if os.path.exists(output_file):
                # Read existing Parquet file
                existing_table = pq.read_table(output_file)
                
                # Append new data to existing table
                combined_table = pa.concat_tables([existing_table, table])
                
                # Write combined table to Parquet
                pq.write_table(combined_table, output_file)
            else:
                # Write new table to Parquet
                pq.write_table(table, output_file)
            
            del df, table, chunk
            gc.collect()
        
        logger.info(f"Successfully processed: {input_file} -> {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.warning(f"Skipping {input_file} and continuing with next file...")
        return False

def process_directory(directory_path, input_dir, output_dir):
    batch_start_time = time.time()
    
    # Get all files in the directory
    dir_files = []
    for root, _, files in os.walk(os.path.join(input_dir, directory_path)):
        for file in files:
            rel_path = os.path.relpath(root, input_dir)
            dir_files.append(os.path.join(rel_path, file))
    
    logger.info(f"Processing directory: {directory_path} with {len(dir_files)} files")
    
    for file in dir_files:
        input_file = os.path.join(input_dir, file)
        output_file = os.path.join(output_dir, file).replace(os.path.splitext(file)[1], ".parquet")
        
        # Create the output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Convert the file
        success = convert_file_to_parquet(input_file, output_file)
        
        if not success:
            logger.warning(f"Failed to process {input_file}")
    
    batch_end_time = time.time()
    batch_duration = batch_end_time - batch_start_time
    logger.info(f"Directory {directory_path} processed in {batch_duration:.2f} seconds")

# Set the input and output directories
input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019"
output_dir = "/mnt/code/output"

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Get all subdirectories in the input directory
subdirectories = [name for name in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, name))]

# Process each subdirectory
for subdir in subdirectories:
    process_directory(subdir, input_dir, output_dir)

logger.info("Conversion complete!")