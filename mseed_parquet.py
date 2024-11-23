import os
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def convert_miniseed_to_parquet(input_file, output_file):
    print(f"Attempting to convert: {input_file}")
    try:
        # Read the miniseed file using ObsPy
        st = read(input_file)
        
        # Extract metadata
        network = st[0].stats.network
        station = st[0].stats.station
        location = st[0].stats.location
        channel = st[0].stats.channel
        start_time = st[0].stats.starttime
        end_time = st[0].stats.endtime
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
        
        # Write to Parquet
        pq.write_file(pa.Table.from_pandas(df), output_file)
        print(f"Successfully converted: {input_file} -> {output_file}")
    except Exception as e:
        print(f"Error converting {input_file}: {str(e)}")

# Set the input and output directories
input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019"
output_dir = "/mnt/code/output"

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Iterate over the directory structure
for root, dirs, files in os.walk(input_dir):
    print(f"Searching for mseed files in: {root}")
    for file in files:
        if file.endswith((".D", ".mseed")):
            input_file = os.path.join(root, file)
            rel_path = os.path.relpath(input_file, input_dir)
            output_file = os.path.join(output_dir, rel_path).replace(".D", ".parquet")
            
            # Create the output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Convert the file
            convert_miniseed_to_parquet(input_file, output_file)

print("Conversion complete!")
