import os
import io
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def convert_mseed_to_parquet(input_file, output_file):
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
            'data': [st[0].data]
        })
        
        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet
        pq.write_file(table, output_file)
        
        logger.info(f"Successfully converted {input_file} to {output_file}")
        return True
    
    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")
        return False

def process_directory(input_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    for root, dirs, files in os.walk(input_dir):
        for file in tqdm(files, desc=f"Processing files in {root}", leave=False):
            if file.endswith('.mseed'):
                input_file = os.path.join(root, file)
                relative_path = os.path.relpath(input_file, input_dir)
                output_file = os.path.join(output_dir, relative_path)
                
                # Create output directory if it doesn't exist
                output_file_dir = os.path.dirname(output_file)
                if not os.path.exists(output_file_dir):
                    os.makedirs(output_file_dir)
                
                # Change the file extension from .mseed to .parquet
                output_file = os.path.splitext(output_file)[0] + '.parquet'
                
                convert_mseed_to_parquet(input_file, output_file)

if __name__ == "__main__":
    input_dir = "/mnt/data/"
    output_dir = "/home/rob/projects/output"
    
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Output directory: {output_dir}")
    
    process_directory(input_dir, output_dir)
    logger.info("Conversion complete!")