import os
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def convert_miniseed_to_parquet(miniseed_file):
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
        
        # Create DataFrame
        df = pd.DataFrame({
            'network': [network],
            'station': [station],
            'location': [location],
            'channel': [channel],
            'starttime': [starttime],
            'endtime': [endtime],
            'sampling_rate': [sampling_rate],
            'data': [data]
        })
        
        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Create output filename
        base_name = os.path.splitext(miniseed_file)[0]
        output_file = f"{base_name}.parquet"
        
        # Write the table to Parquet file
        pq.write_table(table, output_file)
        
        print(f"Converted {miniseed_file} to {output_file}")
    except Exception as e:
        print(f"Error processing {miniseed_file}: {str(e)}")

# Get the current directory
current_dir = os.getcwd()

# Process each miniseed file in the current directory
for file in os.listdir(current_dir):
    if file.endswith('.mseed'):
        convert_miniseed_to_parquet(file)

print("Conversion complete!")
