import os
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

def convert_miniseed_to_parquet(miniseed_file, output_dir):
    try:
        # Read the miniseed file
        st = read(miniseed_file)
        
        # Extract metadata
        network = st[0].stats.network
        station = st[0].stats.station
        location = st[0].stats.location
        channel = st[0].stats.channel
        starttime = st[0].stats.starttime.datetime
        endtime = st[0].stats.endtime.datetime
        sampling_rate = st[0].stats.sampling_rate
        data = st[0].data
        
        # Generate timestamps array
        npts = len(data)
        time_array = np.arange(0, npts) / sampling_rate
        timestamps = st[0].stats.starttime + time_array
        
        # Create DataFrame
        df = pd.DataFrame({
            'network': [network],
            'station': [station],
            'location': [location],
            'channel': [channel],
            'starttime': [starttime],
            'endtime': [endtime],
            'sampling_rate': [sampling_rate],
            'data': [data],
            'timestamps': [timestamps]
        })
        
        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Create output filename
        base_name = os.path.basename(miniseed_file)
        output_file = os.path.join(output_dir, f"{os.path.splitext(base_name)[0]}.parquet")
        
        # Write the table to Parquet file
        pq.write_table(table, output_file)
        
        print(f"Converted {miniseed_file} to {output_file}")
    except Exception as e:
        print(f"Error processing {miniseed_file}: {str(e)}")

# Define the input and output directories
input_dir = "/mnt/data/SWP_Database_Current/2019/ZZ"
output_dir = "/mnt/code/output"

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Process each miniseed file in the input directory and its subdirectories
for root, dirs, files in os.walk(input_dir):
    # Skip the "#recycle" directory
    if "#recycle" in root:
        continue
    
    for file in files:
        if file.endswith('.mseed'):
            miniseed_file = os.path.join(root, file)
            
            # Create the same directory structure in the output
            relative_path = os.path.relpath(root, input_dir)
            output_subdir = os.path.join(output_dir, relative_path)
            os.makedirs(output_subdir, exist_ok=True)
            
            convert_miniseed_to_parquet(miniseed_file, output_subdir)

print("Conversion complete!")
