import os
import gc
from typing import Tuple
import uuid
from obspy import read
import pandas as pd
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_unique_filename(base_filename: str) -> str:
    base_name, extension = os.path.splitext(base_filename)
    return f"{base_name}_{uuid.uuid4().hex[:8]}{extension}"

def convert_mseed_to_parquet(input_file: str, output_file: str) -> bool:
    logger.info(f"Attempting to convert {input_file} to {output_file}")
    try:
        # Read the miniSEED file
        st = read(input_file, headonly=False)
        
        # Extract metadata
        network = st[0].stats.network
        station = st[0].stats.station
        location = st[0].stats.location
        channel = st[0].stats.channel
        sampling_rate = st[0].stats.sampling_rate
        
        # Create DataFrame
        df = pd.DataFrame({
            'network': [network],
            'station': [station],
            'location': [location],
            'channel': [channel],
            'starttime': [st[0].stats.starttime.isoformat()],
            'endtime': [st[0].stats.endtime.isoformat()],
            'sampling_rate': [sampling_rate],
            'data': [st[0].data.tolist()]  # Convert numpy array to list for compatibility
        })
        
        # Write DataFrame to Parquet
        df.to_parquet(output_file, index=False)
        
           # Clear memory
        del df
        gc.collect()
        
        logger.info(f"Successfully converted {input_file} to {output_file}")
        return True
    
    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")
        return False

def process_directory(input_dir: str, output_dir: str, batch_size: int = 10) -> None:
    total_files = 0
    successful_conversions = 0
    batch = []

    for root, _, files in os.walk(input_dir):
        for file in tqdm(files, desc=f"Processing files in {root}", leave=False):
            input_file = os.path.join(root, file)
            relative_path = os.path.relpath(input_file, input_dir)
            output_file = generate_unique_filename(os.path.join(output_dir, relative_path))
            
            # Add the file to the current batch
            batch.append((input_file, output_file))
            
            # If the batch is full, process it
            if len(batch) == batch_size:
                for input_file, output_file in batch:
                    # Create the output directory if it doesn't exist
                    output_dir_path = os.path.dirname(output_file)
                    os.makedirs(output_dir_path, exist_ok=True)
                    
                    total_files += 1
                    
                    if convert_mseed_to_parquet(input_file, output_file):
                        successful_conversions += 1
                
                # Clear the batch
                batch.clear()
        
        # Process any remaining files in the last batch
        if batch:
            for input_file, output_file in batch:
                output_dir_path = os.path.dirname(output_file)
                os.makedirs(output_dir_path, exist_ok=True)
                
                total_files += 1
                
                if convert_mseed_to_parquet(input_file, output_file):
                    successful_conversions += 1
            
            batch.clear()

    logger.info(f"Total files processed: {total_files}")
    logger.info(f"Successful conversions: {successful_conversions}")

if __name__ == "__main__":
    input_dir = "/mnt/f/ZZ_clean/FWU9/"
    output_dir = "/mnt/f/parquet/ZZ/FWU9/"
    
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Output directory: {output_dir}")
    
    process_directory(input_dir, output_dir, batch_size=10)
    logger.info("Conversion complete!")