import os
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import traceback

def convert_file_to_parquet(input_file, output_file):
    print(f"Attempting to convert: {input_file}")
    try:
        # Read the file
        st = read(input_file)
        print(f"Successfully read: {input_file}")
        
        # Extract metadata
        network = st[0].stats.network
        station = st[0].stats.station
        location = st[0].stats.location
        channel = st[0].stats.channel
        start_time = st[0].stats.starttime.isoformat()
        end_time = st[0].stats.endtime.isoformat()
        sampling_rate = st[0].stats.sampling_rate
        
        # Create DataFrame
        df = pd.DataFrame({
            'network': [network],
            'station': [station],
            'location': [location],
            'channel': [channel],
            'starttime': [start_time],
            'endtime': [end_time],
            'sampling_rate': [sampling_rate],
            'data': [st[0].data]
        })

        # Check if the Parquet file already exists
        if os.path.exists(output_file):
            print(f"Parquet file already exists: {output_file}")
            try:
                # Read existing data
                existing_table = pq.read_table(output_file)
                existing_df = existing_table.to_pandas()
                # Append new data
                df = pd.concat([existing_df, df], ignore_index=True)
                print(f"Successfully appended data to existing Parquet file: {output_file}")
            except Exception as e:
                print(f"Error reading existing Parquet file: {str(e)}")
                print(f"Traceback: {traceback.format_exc()}")
                print("Creating a new Parquet file instead of appending.")
        
        # Write to Parquet
        try:
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_file)
            print(f"Successfully converted: {input_file} -> {output_file}")
        except Exception as e:
            print(f"Error writing to Parquet file: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")

    except Exception as e:
        print(f"Error converting {input_file}: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")

# Set the input and output directories
input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019"
output_dir = "/mnt/code/output"

# Create the output directory if it doesn't exist
try:
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory created or already exists: {output_dir}")
except Exception as e:
    print(f"Error creating output directory: {str(e)}")
    print(f"Traceback: {traceback.format_exc()}")
    sys.exit(1)

# Iterate over the directory structure
for root, dirs, files in os.walk(input_dir):
    print(f"Searching for files in: {root}")
    for file in files:
        input_file = os.path.join(root, file)
        rel_path = os.path.relpath(input_file, input_dir)
        output_file = os.path.join(output_dir, rel_path).replace(os.path.splitext(file)[1], ".parquet")
        
        # Create the output directory if it doesn't exist
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            print(f"Output subdirectory created or already exists: {os.path.dirname(output_file)}")
        except Exception as e:
            print(f"Error creating output subdirectory for file: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            continue
        
        # Convert the file
        convert_file_to_parquet(input_file, output_file)

print("Conversion complete!")