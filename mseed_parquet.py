
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
import sys
import signal

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
            return False
        
        # Read the file
        logger.info(f"Starting to read file: {input_file}")
        st = read(input_file, headonly=False)
        logger.info(f"Successfully read: {input_file}")
        
        # Log additional information about the stream
        logger.info(f"Stream info: {st}")
        
        # Extract metadata
        network = st[0].stats.network
        station = st[0].stats.station
        location = st[0].stats.location
        channel = st[0].stats.channel
        sampling_rate = st[0].stats.sampling_rate
        
        logger.info(f"Creating DataFrame")
        df = pd.DataFrame({
            'network': [network],
            'station': [station],
            'location': [location],
            'channel': [channel],
            'starttime': [st[0].stats.starttime.isoformat()],
            'endtime': [st[0].stats.endtime.isoformat()],
            'sampling_rate': [sampling_rate],
            'data': [st[0].data]
        })
        
        logger.info(f"Converting DataFrame to PyArrow Table")
        table = pa.Table.from_pandas(df)
        
        logger.info(f"Writing table to Parquet")
        pq.write_table(table, output_file)
        
        logger.info(f"Successfully processed: {input_file} -> {output_file}")
        return True
    
    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.warning(f"Skipping {input_file} and continuing with next file...")
        return False

def process_directory(directory_path, input_dir, output_dir):
    batch_start_time = time.time()
    
    dir_files = []
    for root, _, files in os.walk(os.path.join(input_dir, directory_path)):
        for file in files:
            rel_path = os.path.relpath(root, input_dir)
            dir_files.append((rel_path, file))
    
    logger.info(f"Processing directory: {directory_path} with {len(dir_files)} files")
    
    successful_conversions = 0
    failed_conversions = 0
    
    for rel_path, file in tqdm(dir_files, desc=f"Processing directory {directory_path}"):
        input_file = os.path.join(input_dir, rel_path, file)
        # Create a unique output file name for each input file
        output_file = os.path.join(output_dir, rel_path, f"{os.path.splitext(file)[0]}_{julian_day}.parquet")
        
        julian_day = int(os.path.splitext(file)[0].split('.')[-1])
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        success = convert_file_to_parquet(input_file, output_file)
        
        if success:
            successful_conversions += 1
        else:
            failed_conversions += 1
        
        if failed_conversions > len(dir_files) // 2:
            logger.critical(f"More than half of the files in {directory_path} failed conversion. Stopping.")
            return
    
    batch_end_time = time.time()
    batch_duration = batch_end_time - batch_start_time
    logger.info(f"Directory {directory_path} processed in {batch_duration:.2f} seconds")
    logger.info(f"Successful conversions: {successful_conversions}, Failed conversions: {failed_conversions}")
if __name__ == "__main__":
    
    try:

        logger.info(f"Contents of /mnt: {os.listdir('/mnt')}")
        logger.info(f"Contents of /mnt/data: {os.listdir('/mnt/data')}")
        logger.info(f"Contents of /mnt/data/SWP_Seismic_Database_Current: {os.listdir('/mnt/data/SWP_Seismic_Database_Current')}")

        input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019/ZZ/FWU1/"
        output_dir = "/mnt/code/output"

        logger.info(f"Input directory: {input_dir}")
        logger.info(f"Output directory: {output_dir}")

        if not os.path.exists(input_dir):
            logger.error(f"Input directory does not exist: {input_dir}")
            logger.info(f"Contents of /mnt: {os.listdir('/mnt')}")
            logger.info(f"Contents of /mnt/data: {os.listdir('/mnt/data')}")
            sys.exit(1)

        # Find the correct subdirectory

        subdirectories = [name for name in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, name))]
        logger.info(f"Subdirectories found: {subdirectories}")

        if not subdirectories:
            logger.warning("No subdirectories found in the input directory")
            logger.info("Attempting to process files directly in the input directory")
            process_directory("", input_dir, output_dir)
        else:
            # Process each subdirectory
            for subdir in tqdm(subdirectories, desc="Processing subdirectories"):
                process_directory(subdir, input_dir, output_dir)

        logger.info(f"Contents of input directory: {os.listdir(input_dir)}")
        logger.info("Conversion complete!")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
