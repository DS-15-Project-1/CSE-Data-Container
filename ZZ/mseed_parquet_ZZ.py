import os
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

def convert_miniseed_to_parquet(miniseed_file, output_dir):
    try:
        # Read the miniseed file
        st = read(miniseed_file, format='MSEED')
        
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
        output_file = os.path.join(output_dir, f"{base_name}.parquet")
        
        # Write the table to Parquet file
        pq.write_table(table, output_file)
        
        print(f"Converted {miniseed_file} to {output_file}")
        return True
    except Exception as e:
        print(f"Error processing {miniseed_file}: {str(e)}")
        return False


# Define the input and output directories
input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019/ZZ"
output_dir = "/mnt/code/output"

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Process each file in the input directory and its subdirectories
total_files = 0
converted_files = 0
for root, dirs, files in os.walk(input_dir):
    print(f"Processing directory: {root}")
    for file in files:
        if file.startswith("ZZ."):
            miniseed_file = os.path.join(root, file)
            
            # Create the same directory structure in the output
            relative_path = os.path.relpath(root, input_dir)
            output_subdir = os.path.join(output_dir, relative_path)
            os.makedirs(output_subdir, exist_ok=True)
            
            total_files += 1
            if convert_miniseed_to_parquet(miniseed_file, output_subdir):
                converted_files += 1

print(f"Conversion complete!")
print(f"Total files processed: {total_files}")
print(f"Files successfully converted: {converted_files}")
print(f"Output directory: {output_dir}")

# Print directory contents
print("Directory contents:")
for root, dirs, files in os.walk(input_dir):
    print(f"Directory: {root}")
    for file in files:
        print(f"  {file}")
