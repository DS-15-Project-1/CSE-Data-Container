import os
from pathlib import Path
import obspy
import pyarrow
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_directory(input_dir, output_dir):
    logger.info(f"Processing directory: {input_dir}")
    try:
        for root, dirs, files in os.walk(input_dir):
            for file in files:
                input_file = os.path.join(root, file)
                try:
                    logger.info(f"Processing file: {input_file}")
                    
                    # Read the mseed file
                    st = obspy.read(input_file)
                    
                    # Extract channel name from the filename
                    channel = file.split('.')[0].split('.')[-1]
                    logger.info(f"Channel detected: {channel}")
                    
                    # Convert to pandas DataFrame
                    df = pd.DataFrame({
                        'time': st[0].times('matplotlib'),
                        'data': st[0].data
                    })
                    
                    # Create output file path
                    rel_path = os.path.relpath(input_file, input_dir)
                    output_file = os.path.join(output_dir, channel, rel_path).replace(os.path.splitext(file)[1], ".parquet")
                    
                    # Create necessary subdirectories
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)
                    
                    # Write to Parquet
                    df.to_parquet(output_file)
                    
                    logger.info(f"File processed: {input_file}")
                    logger.info(f"Output file: {output_file}")
                
                except Exception as e:
                    logger.error(f"Error processing {input_file}: {str(e)}")
        
        logger.info("Directory processing complete.")
    
    except Exception as e:
        logger.error(f"Error processing directory: {str(e)}")

def main():
    input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019/ZZ"
    output_dir = "/mnt/code/output"
    
    print(f"INPUT_DIR: {input_dir}")
    print(f"OUTPUT_DIR: {output_dir}")
    
    inspect_directory(input_dir)
    process_directory(input_dir, output_dir)

    print("\nConversion complete!")
    print(f"Total files processed: {len(os.listdir(input_dir))}")
    print(f"Files successfully converted: {len(os.listdir(output_dir))}")
    print(f"Output directory: {output_dir}")

if __name__ == "__main__":
    main()
