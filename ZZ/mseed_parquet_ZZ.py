import os
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import traceback
from datetime import datetime

def convert_channel_to_parquet(input_dir, output_dir):
    print(f"Processing channel data in {input_dir}")
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Iterate over all channels (HHZ, HHE, HHN)
    channels = ['HHZ', 'HHE', 'HHN']
    for channel in channels:
        # Create the output file name
        output_file = os.path.join(output_dir, f"{channel}.parquet")
        
        # Initialize the table with metadata
        metadata = {
            'network': '',
            'station': '',
            'location': '',
            'channel': channel,
            'starttime': None,
            'endtime': None,
            'sampling_rate': None,
            'data': [],
            'timestamps': []
        }
        
        # Initialize the PyArrow Table
        table = pa.Table.from_dict(metadata)
        
        # Process each file in the input directory
        for file in os.listdir(input_dir):
            if file.endswith('.D'):
                input_file = os.path.join(input_dir, file)
                
                try:
                    # Read the file
                    st = read(input_file)
                    
                    # Extract metadata
                    network = st[0].stats.network
                    station = st[0].stats.station
                    location = st[0].stats.location
                    start_time = st[0].stats.starttime
                    end_time = st[0].stats.endtime
                    sampling_rate = st[0].stats.sampling_rate
                    
                    # Convert UTCDateTime to datetime
                    start_time = datetime.fromtimestamp(start_time.timestamp)
                    end_time = datetime.fromtimestamp(end_time.timestamp)
                    
                    # Get timestamps
                    timestamps = st[0].times()
                    
                    # Add data to the table
                    table = pa.concat_tables([table, pa.Table.from_arrays(
                        [network, station, location, [channel] * len(timestamps),
                         [start_time] * len(timestamps), [end_time] * len(timestamps),
                          [sampling_rate] * len(timestamps), timestamps, st[0].data],
                        names=['network', 'station', 'location', 'channel', 'starttime', 'endtime',
                              'sampling_rate', 'timestamps', 'data']
                    )])
                    
                except Exception as e:
                    print(f"Error processing {input_file}: {str(e)}")
                    traceback.print_exc()
        
        # Write the table to Parquet file
        pq.write_table(table, output_file)
        
        print(f"Successfully converted {channel} data: {output_file}")

# Usage
input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019/ZZ"
output_dir = "/mnt/code/output/ZZ"

# Process each station directory
for station in os.listdir(input_dir):
    station_input_dir = os.path.join(input_dir, station)
    station_output_dir = os.path.join(output_dir, station)
    convert_channel_to_parquet(station_input_dir, station_output_dir)

print("Conversion complete!")
